package org.mtkachev.stomp.server

import scala.actors.Actor

import org.specs2.mutable._
import org.specs2.mock._
import org.specs2.specification.Scope
import org.specs2.execute.AsResult
import org.specs2.execute.Result._

import org.mtkachev.stomp.server.codec._
import org.mtkachev.stomp.server.Matchers._
import org.mtkachev.stomp.server.Subscriber.FrameMsg

/**
 * User: mick
 * Date: 19.08.13
 * Time: 12:36
 */
class DestinationSpecification extends Specification {
  "destination" should {
    "handle add subscrber and remove subscriber" in new DestinationSpecScope {
      destination.subscriptionList.size must_==(0)
      val subscription = Subscription("/foo/bar", subscriber, true, Some("foo"))

      destination ! Destination.AddSubscriber(subscription)
      destination.subscriptionList.size must eventually(3, 1 second)(be_==(1))
      destination.subscriptionList(0) must_== subscription
      subscriber.messages.size must eventually(3, 1 second)(be_==(1))
      subscriber.messages(0) must_== Subscriber.Subscribed(destination, subscription)

      destination ! Destination.RemoveSubscriber(subscription)
      destination.subscriptionList.size must eventually(3, 1 second)(be_==(0))
    }
    "dispatch message when there are ready subscription" in new DestinationSpecScope {
      val subscription = Subscription("/foo/bar", subscriber, true, Some("foo"))
      destination ! Destination.AddSubscriber(subscription)
      destination ! Destination.Ready(subscription)

      destination.subscriptionList.size must eventually(3, 1 second)(be_==(1))
      destination.readySubscriptionQueue.size must eventually(3, 1 second)(be_==(1))

      val env = Envelope(10, "0123456789".getBytes)
      destination ! Destination.Dispatch(env)

      subscriber.messages.size must eventually(3, 1 second)(be_==(2))
      subscriber.messages(1) must_== Subscriber.Receive(destination, subscription, env)
    }
    "dispatch message with there are no ready subscription (store it)" in new DestinationSpecScope {
    }
    "dispatch message from queue" in new DestinationSpecScope {
    }
    "handle ack and dispatch message from queue" in new DestinationSpecScope {
    }
    "handle ready and dispatch message from queue" in new DestinationSpecScope {
    }
    "handle fail and dispatch message from queue" in new DestinationSpecScope {
    }
  }

  trait DestinationSpecScope extends Around with Scope with Mockito {
    val dm = new MockDestinationManager
    val transportCtx: TransportCtx = mock[TransportCtx]
    val subscriber: MockSubscriber = new MockSubscriber(dm, transportCtx)

    val destination: Destination = new Destination("foo")

    def around[T : AsResult](t: =>T) = {
      issues(
        List(
          implicitly[AsResult[T]].asResult(t),
          cleanUp
        ),
        ";"
      )
    }

    def cleanUp = {
      success
    }

    def waitForWorkout {
      destination.getState must eventually(10, 100 millis)(be(Actor.State.Suspended))
    }
  }

  class MockDestinationManager extends DestinationManager {
    val messages = new scala.collection.mutable.ListBuffer[AnyRef]

    start()

    override def act() {
      loop {
        react {
          case "poison" =>  exit()
          case msg: AnyRef => messages += msg
        }
      }
    }
  }

  class MockSubscriber(qm: DestinationManager, transport: TransportCtx) extends Subscriber(qm, transport, "foo", "bar") {
    val messages = new scala.collection.mutable.ListBuffer[AnyRef]

    start()

    override def act() {
      loop {
        react {
          case "poison" =>  exit()
          case msg: AnyRef => messages += msg
        }
      }
    }
  }
}
