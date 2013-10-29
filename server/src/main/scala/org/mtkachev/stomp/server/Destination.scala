package org.mtkachev.stomp.server

import actors.Actor
import org.mtkachev.stomp.server.Destination._
import scala.collection.immutable.{HashSet, Queue, Iterable}
import org.mtkachev.stomp.server.persistence.ImMemoryPersister
import org.mtkachev.stomp.server.persistence.Persister.{Remove, Load, StoreOne, StoreList}

/**
 * User: mick
 * Date: 19.10.2010
 * Time: 20:16:04
 */

class Destination(val name: String, val maxQueueSize: Int) extends Actor {
  private var subscriptions = HashSet.empty[Subscription]
  private var readySubscriptions = Queue.empty[Subscription]
  private var messages = Queue.empty[Envelope]
  private val persister = new ImMemoryPersister
  private var workMode: WorkMode = Instant
  private var lastReceivedMessageId: String = ""

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
          dispatch(msg)
        }
        case msg: Ack => {
          persister ! Remove(msg.messagesId)
          subscriptionReady(msg.subscription)
        }
        case msg: Fail => {
          fail(msg)
        }
        case msg: Ready => {
          subscriptionReady(msg.subscription)
        }
        case msg: Loaded => {
          if(!msg.envelopes.isEmpty) {
            enqueueMsg(msg.envelopes)
            if(msg.envelopes.last.id ==lastReceivedMessageId) {
              workMode = Instant
            }
          }
        }
        case msg: Stop => {
          exit()
        }
      }
    }
  }

  def tryToSend() {
    if (!readySubscriptions.isEmpty && !messages.isEmpty) {
      val (s, q) = readySubscriptions.dequeue
      readySubscriptions = q
      val msg = dequeueMsg
      s.message(this, msg)
    }
  }

  def dispatch(msg: Envelope) {
    enqueueMsg(msg)
    tryToSend()
  }


  def fail(msg: Destination.Fail) {
    enqueueMsg(msg.messages.map(_.envelope))
    subscriptionReady(msg.subscription)
  }

  private def subscriptionReady(subscription: Subscription) {
    if (subscriptions.contains(subscription)) {
      readySubscriptions = readySubscriptions.enqueue(subscription)
    }
    tryToSend()
  }

  private def dequeueMsg = {
    val (newMsg, q) = messages.dequeue
    messages = q
    if (messages.size == 0) {
      persister ! Load(maxQueueSize * 3 / 4)
    }
    newMsg
  }

  def enqueueMsg(msg: Envelope) {
    lastReceivedMessageId = msg.id
    messages = workMode match {
      case Instant =>
        persister ! StoreOne(msg, true)
        messages.enqueue(msg)
      case Paging =>
        persister ! StoreOne(msg, false)
        messages
    }
    if (messages.size > maxQueueSize) {
      workMode = Paging
    }
  }

  def enqueueMsg(msg: Iterable[Envelope]) {
    messages = workMode match {
      case Instant =>
        persister ! StoreList(msg, true)
        messages.enqueue(msg)
      case Paging =>
        persister ! StoreList(msg, false)
        messages
    }
    if (messages.size > maxQueueSize) {
      workMode = Paging
    }
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

  abstract sealed class WorkMode
  object Instant extends WorkMode
  object Paging extends WorkMode
}