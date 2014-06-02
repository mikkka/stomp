package org.mtkachev.stomp.server

import actors.Actor
import org.mtkachev.stomp.server.Destination._
import scala.collection.immutable.{HashSet, Queue, Iterable}
import org.mtkachev.stomp.server.persistence.InMemoryPersister
import org.mtkachev.stomp.server.persistence.Persister.{Remove, Load, StoreOne, StoreList}
import com.typesafe.scalalogging.slf4j.StrictLogging

/**
 * User: mick
 * Date: 19.10.2010
 * Time: 20:16:04
 */

class Destination(val name: String, val maxQueueSize: Int) extends Actor with StrictLogging {
  //current destination subscriptions
  private var subscriptions = HashSet.empty[Subscription]
  //subscriptions that are ready for message recv
  private var readySubscriptions = Queue.empty[Subscription]
  //messages buffer
  private var messages = Queue.empty[Envelope]
  //messages persister
  private val persister = new InMemoryPersister
  //work mode
  private var workMode: WorkMode = Instant
  // id of last message that was added to message queue
  private var lastReceivedMessageId: String = ""

  private val loadSize = if(maxQueueSize >= 3) maxQueueSize * 3 / 4 else 1

  def subscriptionSet = subscriptions
  def readySubscriptionQueue = readySubscriptions
  def messageQueue = messages

  def time = System.currentTimeMillis()

  start()

  logger.info(s"destination $name was started")

  override def act() {
    loop {
      react {
        case msg: AddSubscriber =>
          addSubscription(msg.subscription)
        case msg: RemoveSubscriber =>
          removeSubscription(msg.subscription)
        case Dispatch(msg) =>
          dispatch(msg)
        case msg: Ack =>
          persister ! Remove(msg.messagesId)
          subscriptionReady(msg.subscription)
        case msg: Fail =>
          fail(msg)
        case msg: Ready =>
          subscriptionReady(msg.subscription)
        case msg: Loaded =>
          logger.debug(s"loaded ${msg.envelopes.size}")
          if(!msg.envelopes.isEmpty) {
            if(msg.envelopes.last.id == lastReceivedMessageId) {
              workMode = Instant
              logger.debug(s"go INSTANT mode")
            }
            messages = messages.enqueue(msg.envelopes)
            tryToSend()
          }
        case msg: Stop =>
          exit()
      }
    }
  }

  def tryToSend() {
    if (!readySubscriptions.isEmpty && !messages.isEmpty) {
      val (s, q) = readySubscriptions.dequeue
      readySubscriptions = q
      val msg = dequeueMsg
      s.message(this, msg)
      logger.debug(s"message ${msg.id} sent")
    }
  }

  def dispatch(msg: Envelope) {
    logger.debug(s"dispatch ${msg.id}")
    enqueueMsg(msg, fail = false)
    tryToSend()
  }


  def fail(msg: Destination.Fail) {
    logger.debug(s"failed ${msg.messages.size} messages")
    enqueueMsg(msg.messages.map(_.envelope), fail = true)
    if(msg.ready)
      subscriptionReady(msg.subscription)
  }

  private def subscriptionReady(subscription: Subscription) {
    logger.debug(s"subscription ready")
    if (subscriptions.contains(subscription)) {
      readySubscriptions = readySubscriptions.enqueue(subscription)
    }
    tryToSend()
  }

  private def dequeueMsg = {
    val (newMsg, q) = messages.dequeue
    messages = q
    if (messages.size == 0) {
      logger.debug(s"need to load messages from store")
      persister ! Load(loadSize)
    }
    newMsg
  }

  def enqueueMsg(msg: Envelope, fail: Boolean) {
    lastReceivedMessageId = msg.id

    messages = workMode match {
      case Instant =>
        persister ! StoreOne(msg, fail = fail, move = true)
        logger.debug(s"queue after message add ${messages.size + 1}")
        messages.enqueue(msg)
      case Paging =>
        persister ! StoreOne(msg, fail = fail, move = false)
        logger.debug(s"queue after message add ${messages.size}")
        messages
    }
    if (messages.size > maxQueueSize) {
      workMode = Paging
      logger.debug(s"go PAGING mode")
    }
  }

  def enqueueMsg(msg: Iterable[Envelope], fail: Boolean) {
    lastReceivedMessageId = msg.last.id

    messages = workMode match {
      case Instant =>
        persister ! StoreList(msg, fail = fail, move = true)
        logger.debug(s"queue after message add ${messages.size + msg.size}")
        messages.enqueue(msg)
      case Paging =>
        persister ! StoreList(msg, fail = fail, move = false)
        logger.debug(s"queue after message add ${messages.size}")
        messages
    }
    if (messages.size > maxQueueSize) {
      workMode = Paging
      logger.debug(s"go PAGING mode")
    }
  }

  private def addSubscription(subscription: Subscription) {
    logger.info(s"subscription ${subscription.expression} was added to $name")
    subscriptions = subscriptions + subscription
    subscription.subscribed(this)
  }

  private def removeSubscription(subscription: Subscription) {
    logger.info(s"subscription ${subscription.expression} was removed from $name")
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
  case class Fail(subscription: Subscription, messages: List[Dispatch], ready: Boolean = true)
  case class Ready(subscription: Subscription)

  case class Loaded(envelopes: Vector[Envelope])

  case class Stop()

  abstract sealed class WorkMode
  object Instant extends WorkMode
  object Paging extends WorkMode
}