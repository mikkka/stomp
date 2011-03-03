package org.mtkachev.stomp.server

import actors.Actor

import org.apache.mina.core.session.IoSession
import org.mtkachev.stomp.server.SubscriberManager._

/**
 * User: mick
 * Date: 15.02.11
 * Time: 19:24
 */

class SubscriberManager extends Actor {
  private var subscribers = List.empty[Subscriber]

  def subscriberList = subscribers

  start

  def act = {
    loop {
      react {
        case msg: Connect => {
          subscribe(msg.queueManager, msg.session, msg.login, msg.password)
        }
        case msg: Disconnect => {
          unSubscribe(msg.subscriber)
        }
        case msg: Stop => {
          exit()
          subscribers.foreach(s => s ! Subscriber.Stop())
          subscribers = List.empty[Subscriber]
        }
      }
    }
  }

  private def subscribe(queueManager: DestinationManager, session: IoSession, login: String, password: String) = {
    val subscriber = Subscriber(queueManager, session, login, password)
    session.setAttribute(Subscriber.IO_SESS_ATTRIBUTE, subscriber)

    subscribers = subscriber :: subscribers
    subscriber ! Subscriber.OnConnect()
  }

  private def unSubscribe(s: Subscriber) = {
    s ! Subscriber.Stop()
    subscribers = subscribers.filterNot(_ == s)
  }
}

object SubscriberManager {
  case class Connect(queueManager: DestinationManager, session: IoSession, login: String, password: String)
  case class Disconnect(subscriber: Subscriber)
  case class Stop()
}