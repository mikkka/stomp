package org.mtkachev.stomp.server

import actors.Actor
import collection.immutable.HashMap
import org.mtkachev.stomp.server.DestinationManager._
/**
 * User: mick
 * Date: 19.10.2010
 * Time: 20:16:23
 */

class DestinationManager(destinationFactory: String => Destination) extends Actor {
  // queue name -> destination actor
  private var queues = new HashMap[String, Destination]
  /**
   * keep it for checking "subscription" requests after new destination creation
   */
  private var subscriptions = List.empty[Subscription]

  def queueMap = queues
  def subscriptionList = subscriptions

  def act() {
    loop {
      react {
        case msg: Subscribe =>
          /**
           * add subscription to subscriptions list
           * send message AddSubscriber to suitable Destination
           */
          subscribe(msg.subscription)

        case msg: UnSubscribe =>
          /**
           * remove subscription from subscriptions list
           * send message RemoveSubscriber to suitable Destination
           */
          unSubscribe(msg.subscription)

        case msg: Dispatch =>
          /**
           * find or create destination suitable for message destination
           */
          dispatchMessage(msg)

        case msg: Stop =>
          exit()
          queues.foreach(el => el._2 ! Destination.Stop())
      }
    }
  }

  private def subscribe(subscription: Subscription) {
    subscriptions = subscription :: subscriptions
    queues.
      filter(el => subscription.matches(el._2)).
      foreach{case(_, queue) => queue ! Destination.AddSubscriber(subscription)}
  }

  private def unSubscribe(subscription: Subscription) {
    subscriptions = subscriptions.filterNot(_ == subscription)
    queues.
      filter(el => subscription.matches(el._2)).
      foreach{case(_, queue) => queue ! Destination.RemoveSubscriber(subscription)}
  }

  private def dispatchMessage(msg: Dispatch) {
    queues.get(msg.destination) match {
      case Some(queue) => queue ! Destination.Dispatch(msg.envelope)
      case None =>
        val queue = destinationFactory(msg.destination)
        queues = queues + (msg.destination -> queue)
        subscriptions.filter(s => s.matches(queue)).foreach(s => queue ! Destination.AddSubscriber(s))
        queue ! Destination.Dispatch(msg.envelope)
    }
  }
}

object DestinationManager {
  sealed class Msg()

  case class Subscribe(subscription: Subscription) extends Msg
  case class UnSubscribe(subscription: Subscription) extends Msg
  case class Dispatch(destination: String, envelope: Envelope) extends Msg

  case class Stop()
}