package org.mtkachev.stomp.server

import codec._
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

class Subscriber(val qm: DestinationManager, val transport: TransportCtx,
                 val login: String, val password: String) extends Actor {

  private var subscriptions = Map.empty[Subscription, Queue[Message]]
  private var ackIndex = Map.empty[String, Subscription]
  private var acks = Map.empty[Subscription, List[String]]
  private var transactions = Map.empty[String, Transaction]

  val sessionId = UUID.randomUUID.toString

  def subscriptionMap = subscriptions
  def transactionMap = transactions
  def ackMap = acks
  def ackIndexMap = ackIndex

  start()

  def act() {
    loop {
      react {

        case msg: FrameMsg => {
          msg.frame.receipt match {
            case Some(receiptId) => {
              receipt(receiptId)
            }
            case _ => {}
          }

          msg.frame match {
            case frame: Disconnect => {
              if(!transport.isClosing) transport.close()
              exit()
            }

            case frame: Subscribe => {
              val subscription = new Subscription(frame.expression, this, frame.ackMode, frame.id)
              subscriptions += (subscription -> Queue.empty)

              qm ! DestinationManager.Subscribe(subscription)
            }

            case frame: UnSubscribe => {
              val s = frame match {
                case UnSubscribe(None, Some(expression), _) => {
                  subscriptions.keys.find(s => s.expression != expression)
                }
                case UnSubscribe(id: Some[String], None, _) => {
                  subscriptions.keys.find(s => s.id != id)
                }
                case any => None
              }
              if(!s.isEmpty) {
                qm ! DestinationManager.UnSubscribe(s.get)
                subscriptions = subscriptions.filterKeys(_ != s.get)
              }
            }

            case frame: Send => {
              if(!doWithTx(frame.transactionId, tx => tx.storeSend(frame))) {
                send(frame)
              }
            }

            case frame: Ack => {
              doWithTx(frame.transactionId, tx => tx.storeAck(frame))
              ack(frame.messageId)
            }

            case frame: Begin => {
              transactions.get(frame.transactionId) match {
                case None => transactions += (frame.transactionId -> new Transaction)
                case _ =>
              }
              ()
            }

            case frame: Commit => {
              commitTx(frame.transactionId)
            }

            case frame: Abort => {
              abortTx(frame.transactionId)
            }
          }
        }

        case msg: Receive => {
          if(msg.subscription.acknowledge) {
            if(ackNeeded(msg.subscription)) {
              val messages4s = subscriptions.get(msg.subscription)
              if(!messages4s.isEmpty) {
                subscriptions += (msg.subscription -> messages4s.get.enqueue(message(msg.subscription, msg.contentLength, msg.body)))
              } else {
                subscriptions += (msg.subscription -> Queue(message(msg.subscription, msg.contentLength, msg.body)))
              }
            } else {
              unack(msg.subscription, receive(msg.subscription, msg.contentLength, msg.body))
            }
          } else {
            receive(msg.subscription, msg.contentLength, msg.body)
          }
          ()
        }

        case msg: OnConnect => {
          transport.write(new Connected(this.sessionId))
          ()
        }

        case msg: Stop => {
          exit()
          transport.close()
          ()
        }
      }
    }
  }

  def ackNeeded(s: Subscription) = {
    val acks4s = acks.get(s)
    !acks4s.isEmpty && !acks4s.get.isEmpty
  }

  def ackAllowed(s: Subscription, messageId: String) = {
    val acks4s = acks.get(s)
    acks4s.isEmpty || acks4s.get.contains(messageId)
  }

  def unack(s: Subscription, message: Message) {
    unack(s, message.messageId)
  }

  def unack(s: Subscription, msgId: String) {
    val acks4s = acks.get(s)
    if(acks4s.isEmpty) {
      acks += (s -> List(msgId))
      ackIndex += (msgId -> s)
    } else {
      acks += (s -> (msgId :: acks4s.get))
      ackIndex += (msgId -> s)
    }
  }

  def ack(messageId: String) {
    val sOpt = ackIndex.get(messageId)
    if(!sOpt.isEmpty && ackAllowed(sOpt.get, messageId)) {
      val s = sOpt.get
      acks += (s -> acks(s).filterNot(_ == messageId))
      ackIndex = ackIndex.filterNot(_._1 == messageId)
      val messages4s = subscriptions.get(s)
      if(!messages4s.isEmpty && !messages4s.get.isEmpty && !ackNeeded(s)) {
        val (msg, mq) = messages4s.get.dequeue
        unack(s, receive(msg))
        subscriptions += (s -> mq)
      }
    }
  }

  def receive(subscription: Subscription, contentLength: Int, body: Array[Byte]): Message = {
    receive(message(subscription, contentLength, body))
  }

  def receive(msg: Message): Message = {
    transport.write(msg)
    msg
  }

  def receipt(receiptId: String) {
    transport.write(Receipt(receiptId))
  }

  def send(frame: Send) {
    qm ! DestinationManager.Message(frame.destination, frame.contentLength, frame.body)
  }

  def message(subscription: Subscription, contentLength: Int, body: Array[Byte]) =
    new Message(subscription.destination, UUID.randomUUID.toString, contentLength, body)

  def commitTx(txKey: String) {
    doWithTx(Some(txKey), tx => tx.commt())
    transactions = transactions.filterNot(_._1 == txKey)
  }

  def abortTx(txKey: String) {
    doWithTx(Some(txKey), tx => tx.rollback())
    transactions = transactions.filterNot(_._1 == txKey)
  }

  def doWithTx(txKeyOpt: Option[String], f: Transaction => Unit): Boolean = txKeyOpt match {
    case Some(txKey) => transactions.get(txKey) match {
      case Some(tx) => f(tx); true
      case _ => false
    }
    case _ => false
  }

  class Transaction {
    var s: Queue[Send] = null
    var a: List[(Subscription, String)] = null

    clear()

    def clear() {
      a = List.empty[(Subscription, String)]
      s = Queue.empty[Send]
    }

    def commt() {
      for(frame <-s) send(frame)
      clear()
    }

    def rollback() {
      for((subscription, msgId) <- a) unack(subscription, msgId)
      clear()
    }

    def storeSend(f: Send) {
      s = s.enqueue(f)
    }

    def storeAck(ack: Ack) {
      val sOpt = ackIndex.get(ack.messageId)
      if(!sOpt.isEmpty) {
        a = (sOpt.get, ack.messageId) :: a
      }
    }
  }
}

object Subscriber {
  def apply(qm: DestinationManager, transport: TransportCtx, login: String, password: String) =
    new Subscriber(qm, transport, login, password)


  case class FrameMsg(frame: ConnectedStateFrame)
  case class Receive(destination: Destination, subscription: Subscription, contentLength: Int, body: Array[Byte])
  case class Stop()

  case class OnConnect()
}
