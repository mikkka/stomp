package org.mtkachev.stomp.server

import actors.Actor
import org.mtkachev.stomp.server.Destination._
import scala.collection.immutable.{HashSet, Queue}

/**
 * User: mick
 * Date: 19.10.2010
 * Time: 20:16:04
 */

class Destination(val name: String) extends Actor {
  private var subscriptions = HashSet.empty[Subscription]
  private var readySubscriptions = Queue.empty[Subscription]
  private var messages = Queue.empty[Envelope]

  def subscriptionSet = subscriptions
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
        case Dispatch(msg) => {
          if (!readySubscriptions.isEmpty) {
            val (s, q) = readySubscriptions.dequeue
            val nextMsg = if (messageQueue.isEmpty) msg
            else {
              messages = messages.enqueue(msg)
              dequeueMsg
            }

            s.message(this, nextMsg)
            readySubscriptions = q
          } else {
            messages = messages.enqueue(msg)
          }
        }
        case msg: Ack => {
          subscriptionReady(msg.subscription)
        }
        case msg: Fail => {
          messages = messages.enqueue(msg.messages.map(_.envelope))
          subscriptionReady(msg.subscription)
        }
        case msg: Ready => {
          subscriptionReady(msg.subscription)
        }
        case msg: Stop => {
          exit()
        }
      }
    }
  }

  private def subscriptionReady(subscription: Subscription) {
    if(subscriptions.contains(subscription)) {
      readySubscriptions = readySubscriptions.enqueue(subscription)
    }
    if (!readySubscriptionQueue.isEmpty && !messageQueue.isEmpty) {
      val (s, q) = readySubscriptions.dequeue
      readySubscriptions = q
      val msg = dequeueMsg
      s.message(this, msg)
    }
  }

  private def dequeueMsg = {
    val (newMsg, q) = messages.dequeue
    messages = q
    newMsg
  }

  private def addSubscription(subscription: Subscription) {
    subscriptions = subscriptions + subscription
    subscription.subscribed(this)
  }

  private def removeSubscription(subscription: Subscription) {
    subscriptions = subscriptions.filterNot(_ == subscription)
    readySubscriptions = readySubscriptions.filterNot(_ == subscription)
  }

  def isEmpty = subscriptions.isEmpty
}

object Destination {
  case class AddSubscriber(subscription: Subscription)
  case class RemoveSubscriber(subscription: Subscription)

  case class Dispatch(envelope: Envelope)

  case class Ack(subscription: Subscription, messagesId: List[String])
  case class Fail(subscription: Subscription, messages: List[Dispatch])
  case class Ready(subscription: Subscription)

  case class Loaded(envelopes: Vector[Envelope])

  case class Stop()
}