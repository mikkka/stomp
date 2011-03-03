package org.mtkachev.stomp.server

import actors.Actor
import org.mtkachev.stomp.server.Destination._

/**
 * User: mick
 * Date: 19.10.2010
 * Time: 20:16:04
 */

class Destination(val name: String, private var subscriptions: List[Subscription]) extends Actor {
  def this(name: String, subscription: Subscription) =
    this(name, List(subscription))

  def this(name: String) =
    this(name, Nil)

  def subscriptionList = subscriptions

  start

  def act = {
    loop {
      react {
        case msg: AddSubscriber => {
          addSubscription(msg.subscription)
        }
        case msg: RemoveSubscriber => {
          //TODO: возможно проверить на то, что очередь пуста и убить её
          removeSubscription(msg.subscription)
        }
        case msg: Message => {
          subscriptions.foreach(s => s.message(msg.contentLength, msg.body))
        }
        case msg: Stop => {
          exit()
        }
      }
    }
  }

  private def addSubscription(subscription: Subscription) {
    subscriptions = subscription :: subscriptions
  }

  private def removeSubscription(subscription: Subscription) {
    subscriptions = subscriptions.filterNot(_ == subscription)
  }

  def isEmpty = subscriptions.isEmpty
}

object Destination {
  case class AddSubscriber(subscription: Subscription)
  case class RemoveSubscriber(subscription: Subscription)
  case class Message(contentLength: Int, body: Array[Byte])
  case class Stop()
}