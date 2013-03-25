package org.mtkachev.stomp.server

import actors.Actor
import org.mtkachev.stomp.server.Destination._
import collection.immutable.Queue

/**
 * User: mick
 * Date: 19.10.2010
 * Time: 20:16:04
 */

class Destination(val name: String) extends Actor {
  private var subscriptions: List[Subscription] = List.empty
  private var readySubscriptions: Queue[Subscription] = Queue.empty
  private var messages: Queue[Message] = Queue.empty

  def subscriptionList = subscriptions
  def readySubscriptionQueue = readySubscriptions
  def messageQueue = messages

  start()

  override def act() {
    loop {
      react {
        case msg: AddSubscriber => {
          addSubscription(msg.subscription)
        }
        case msg: RemoveSubscriber => {
          removeSubscription(msg.subscription)
        }
        case msg: Message => {
          if (!readySubscriptionQueue.isEmpty) {
            val (s, q) = readySubscriptions.dequeue
            val nextMsg = if (messageQueue.isEmpty) msg
            else {
              messages = messages.enqueue(msg)
              dequeueMsg
            }

            s.message(nextMsg.contentLength, nextMsg.body)
            readySubscriptions = q
          } else {
            messages = messages.enqueue(msg)
          }
        }
        case msg: Ack => {
          readySubscriptions = readySubscriptionQueue.enqueue(msg.subscription)
        }
        case msg: Fail => {
          readySubscriptions = readySubscriptionQueue.enqueue(msg.subscription)
          messages = messages.enqueue(msg.messages)
        }
        case msg: Ready => {
          readySubscriptions = readySubscriptionQueue.enqueue(msg.subscription)
        }
        case msg: Stop => {
          exit()
        }
      }
    }
  }

  private def dequeueMsg = {
    val (newMsg, q) = messages.dequeue
    messages = q
    newMsg
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

  case class Ack(subscription: Subscription, messagesId: List[String])
  case class Fail(subscription: Subscription, messages: List[Message])
  case class Ready(subscription: Subscription)

  case class Stop()
}