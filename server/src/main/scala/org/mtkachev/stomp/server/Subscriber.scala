package org.mtkachev.stomp.server

import codec._
import org.apache.mina.core.session.IoSession
import actors.Actor
import java.util.UUID
import org.mtkachev.stomp.server.Subscriber._
import scala.collection.immutable.Queue
import scala.None

/**
 * User: mick
 * Date: 19.10.2010
 * Time: 20:08:57
 */

class Subscriber(val qm: DestinationManager, val session: IoSession,
                 val login: String, val password: String) extends Actor {

  private var subscriptions = List.empty[Subscription]
  private var messages = Queue.empty[Message]
  private var transactions = Map.empty[String, Transaction]
  private var acks = List.empty[String]

  val sessionId = UUID.randomUUID.toString

  def subscriptionList = subscriptions
  def messageList = messages
  def transactionMap = transactions
  def ackList = acks

  start

  def act = {
    loop {
      react {
        case msg: FrameMsg => {
          msg.frame match {
            case frame: Disconnect => {
              if(!session.isClosing) session.close(false)
              exit()
            }

            case frame: Subscribe => {
              val subscription = new Subscription(frame.expression, this, frame.ackMode, frame.id)
              subscriptions = subscription :: subscriptions

              qm ! DestinationManager.Subscribe(subscription)
            }
            case frame: UnSubscribe => {
              val applyPartitions = (p: (List[Subscription], List[Subscription])) => {
                subscriptions = p._1
                p._2.foreach(s => qm ! DestinationManager.UnSubscribe(s))
              }
              frame match {
                case UnSubscribe(None, Some(expression), _) => {
                  applyPartitions(subscriptions.partition(s => s.expression != expression))
                }
                case UnSubscribe(id: Some[String], None, _) => {
                  applyPartitions(subscriptions.partition(s => s.id != id))
                }
                case any =>
              }
            }
            case frame: Send => {
              if(!doWithTx(frame.transactionId, tx => tx.send(frame))) {
                send(frame)
              }
            }
            case frame: Ack => {
              doWithTx(frame.transactionId, tx => tx.ack(frame))
              ack(frame.messageId)
            }
          }
        }
        case msg: Recieve => {
          if(msg.subscription.acknowledge) {
            if(ackNeeded) {
              messages = messages.enqueue(message(msg.subscription, msg.contentLength, msg.body))
            } else {
              unack(receive(msg.subscription, msg.contentLength, msg.body))
            }
          } else {
            receive(msg.subscription, msg.contentLength, msg.body)
          }
        }

        case msg: OnConnect => {
          session.write(new Connected(this.sessionId, Map.empty))
        }

        case msg: Stop => {
          exit()
          session.close(false)
        }
      }
    }
  }

  def ackNeeded = !acks.isEmpty
  def ackAllowed(messageId: String) = acks.contains(messageId)

  def unack(m: Message) {acks = m.messageId :: acks}

  def ack(messageId: String) {
    if(ackAllowed(messageId)) {
      acks = acks.filterNot(_ == messageId)
      val (msg, mq) = messages.dequeue
      receive(msg)
      messages = mq
    }
  }

  def receive(subscription: Subscription, contentLength: Int, body: Array[Byte]): Message = {
    receive(message(subscription, contentLength, body))
  }

  def receive(msg: Message): Message = {
    session.write(msg)
    msg
  }

  def send(frame: Send) {
    qm ! DestinationManager.Message(frame.destination, frame.contentLength, frame.body)
  }

  def message(subscription: Subscription, contentLength: Int, body: Array[Byte]) =
    new Message(subscription.destination, UUID.randomUUID.toString, contentLength, body, Map.empty)

  def commitTx(txKey: String) = {
    doWithTx(Some(txKey), tx => tx.commt)
  }

  def abortTx(txKey: String) = {
    doWithTx(Some(txKey), tx => tx.rollback)
  }

  def doWithTx(txKeyOpt: Option[String], f: Transaction => Unit): Boolean = txKeyOpt match {
    case Some(txKey) => transactions.get(txKey) match {
      case Some(tx) => f(tx); true
      case _ => false
    }
    case _ => false
  }

  class Transaction {
    var s = Queue.empty[Send]
    var a = List.empty[String]

    def commt() {
      a = List.empty[String]
      s.foreach(frame => send(frame))
    }

    def rollback() {
      acks = acks ::: a
      s = Queue.empty[Send]
    }

    def send(f: Send) {
      s = s.enqueue(f)
    }

    def ack(ack: Ack) {
      a = ack.messageId :: a
    }
  }
}

object Subscriber {
  def IO_SESS_ATTRIBUTE = "IO.Subscriber"

  def apply(qm: DestinationManager, session: IoSession, login: String, password: String) =
    new Subscriber(qm, session, login, password)


  case class FrameMsg(frame: ConnectedStateFrame)
  case class Recieve(subscription: Subscription, contentLength: Int, body: Array[Byte])
  case class Stop()

  case class OnConnect()
}
