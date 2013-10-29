package org.mtkachev.stomp.server

import actors.Actor
import collection.immutable.HashMap
import org.mtkachev.stomp.server.DestinationManager._
/**
 * User: mick
 * Date: 19.10.2010
 * Time: 20:16:23
 */

class DestinationManager extends Actor {
  private var queues = new HashMap[String, Destination]
  private var subscriptions = List.empty[Subscription]

  def queueMap = queues
  def subscriptionList = subscriptions

  def act() {
    loop {
      react {
        case msg: Subscribe => {
          subscribe(msg.subscription)
        }
        case msg: UnSubscribe => {
          unSubscribe(msg.subscription)
        }
        case msg: Dispatch => {
          dispatchMessage(msg)
        }
        case msg: Stop => {
          exit()
          queues.foreach(el => el._2 ! Destination.Stop())
        }
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
      case Some(queue) => {
        queue ! Destination.Dispatch(msg.envelope)
      }
      case None => {
        val queue = new Destination(msg.destination, 1024)
        queues = queues + (msg.destination -> queue)
        subscriptions.filter(s => s.matches(queue)).foreach(s => queue ! Destination.AddSubscriber(s))
        queue ! Destination.Dispatch(msg.envelope)
      }
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