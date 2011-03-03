package org.mtkachev.stomp.server.codec

import net.lag.naggati.{IoHandlerActorAdapter, MinaMessage, ProtocolError}
import org.apache.mina.core.buffer.IoBuffer
import org.apache.mina.core.session.{IdleStatus, IoSession}
import java.io.IOException
import com.twitter.actors.Actor
import com.twitter.actors.Actor._
import scala.collection.{immutable, mutable}
import org.mtkachev.stomp.server.{Subscriber, DestinationManager, SubscriberManager}

/**
 * User: mick
 * Date: 11.08.2010
 * Time: 17:37:07
 */

class MessageHandler(val session: IoSession,
                     val queueManager: DestinationManager,
                     val subscriberManager: SubscriberManager
        ) extends Actor {

  start

  def act = {
    loop {
      react {
        case MinaMessage.SessionOpened
          => println("session opened : " + session)
        case MinaMessage.SessionClosed
          => handle(session, new Disconnect(Map())) 
        case MinaMessage.MessageReceived(msg)
          => handle(session, msg)
      }
    }
  }

  private def handle(session: IoSession, msg : AnyRef) {
    def client = session.getAttribute(Subscriber.IO_SESS_ATTRIBUTE)
    client match {
      case subscriber: Subscriber => msg match {
        case msg: ConnectedStateFrame => {
          subscriber ! Subscriber.FrameMsg(msg)
        }
        case any => {
          handleErrorMessage(msg)
        }
      }
      case any => msg match {
        case msg: Connect => {
          subscriberManager ! SubscriberManager.Connect(queueManager, session, msg.login, msg.password)
        }
        case any => {
          handleErrorMessage(msg)
        }
      }
    }
  }

  private def handleErrorMessage(msg: AnyRef) = msg match {
    case msg: ErrorIn => {
      println("error parse :" + msg)
    }
    case any => {
      println("error not supported : " + msg)
    }
  }
}