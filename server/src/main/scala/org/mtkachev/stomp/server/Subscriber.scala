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

  var subscriptionList = List.empty[Subscription]
  var messageQueue = Queue.empty[Message]
  val sessionId = UUID.randomUUID.toString
  var unacked: Option[String] = None

  start

  def act = {
    loop {
      react {
        case msg: FrameMsg => {
          //TODO: пришло сообщение от самого субскрайбера
          msg.frame match {
            case frame: Disconnect => {
              if(!session.isClosing) session.close(false)
              exit()
            }

            case frame: Subscribe => {
              val subscription = new Subscription(frame.expression, this, frame.ackMode, frame.id)
              subscriptionList = subscription :: subscriptionList

              qm ! DestinationManager.Subscribe(subscription)
            }
            case frame: UnSubscribe => {
              val applyPartitions = (p: (List[Subscription], List[Subscription])) => {
                subscriptionList = p._1
                p._2.foreach(s => qm ! DestinationManager.UnSubscribe(s))
              }
              frame match {
                case UnSubscribe(Some(id), None, _) => {
                  applyPartitions(subscriptionList.partition(s => s.id != id))
                }
                case UnSubscribe(None, Some(expression), _) => {
                  applyPartitions(subscriptionList.partition(s => s.expression != expression))
                }
                case any =>
              }
            }
            case frame: Send => {
              qm ! DestinationManager.Message(frame.destination, frame.contentLength, frame.body)
            }
            case frame: Ack => {
              ack(frame.messageId)
            }
          }
        }
        case msg: Recieve => {
          if(msg.subscription.acknowledge) {
            if(ackNeeded) {
              messageQueue = messageQueue.enqueue(message(msg.subscription, msg.contentLength, msg.body))
            } else {
              unack(sendMessage(msg.subscription, msg.contentLength, msg.body))
            }
          } else {
            sendMessage(msg.subscription, msg.contentLength, msg.body)
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

  def ackNeeded = unacked match {
    case Some(_) => true
    case None => false
  }

  def ack(messageId: String) {
    unacked match {
      case Some(unackedMessaeId) => {
        if(unackedMessaeId == messageId) {
          unacked = None
          if(!messageQueue.isEmpty) {
            val (msg, mq) = messageQueue.dequeue
            unack(sendMessage(msg))
            messageQueue = mq
          }
        }
      }
      case None =>
    }
  }

  def unack(message: Message) {
    unacked = Some(message.messageId)
  }

  def sendMessage(subscription: Subscription, contentLength: Int, body: Array[Byte]): Message = {
    sendMessage(message(subscription, contentLength, body))
  }

  def sendMessage(msg: Message): Message = {
    session.write(msg)
    msg
  }

  def message(subscription: Subscription, contentLength: Int, body: Array[Byte]) =
    new Message(subscription.destination, UUID.randomUUID.toString, contentLength, body, Map.empty)
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
