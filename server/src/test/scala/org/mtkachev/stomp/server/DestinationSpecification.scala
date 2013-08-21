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
      destination.subscriptionSet.size must_== 0

      destination ! Destination.AddSubscriber(subscription)
      destination.subscriptionSet.size must eventually(3, 1 second)(be_==(1))
      destination.subscriptionSet.head must_== subscription
      subscriber.messages.size must eventually(3, 1 second)(be_==(1))
      subscriber.messages(0) must_== Subscriber.Subscribed(destination, subscription)

      destination ! Destination.Ready(subscription)
      destination.readySubscriptionQueue.size must eventually(3, 1 second)(be_==(1))

      destination ! Destination.RemoveSubscriber(subscription)
      destination.subscriptionSet.size must eventually(3, 1 second)(be_==(0))
      destination.readySubscriptionQueue.size must_== 0
    }
    "dispatch message when there are ready subscription" in new DestinationSpecScope {
      destination ! Destination.AddSubscriber(subscription)
      destination ! Destination.Ready(subscription)

      destination.subscriptionSet.size must eventually(3, 1 second)(be_==(1))
      destination.readySubscriptionQueue.size must eventually(3, 1 second)(be_==(1))

      val env = Envelope(10, "0123456789".getBytes)
      destination ! Destination.Dispatch(env)

      subscriber.messages.size must eventually(3, 1 second)(be_==(2))
      subscriber.messages(1) must_== Subscriber.Receive(destination, subscription, env)

      destination.messageQueue.size must_== 0
    }
    "dispatch message with there are no ready subscription (store it)" in new DestinationSpecScope {
      val (env1, env2) = subscribeAndTwoDispatch

      //all envs stored in queue!
      destination.messageQueue.size must_== 2
      destination.messageQueue(0) must_== env1
      destination.messageQueue(1) must_== env2
    }
    "handle ready and dispatch message from queue" in new DestinationSpecScope {
      val (env1, env2) = subscribeAndTwoDispatch
      destination ! Destination.Ready(subscription)

      subscriber.messages.size must eventually(3, 1 second)(be_==(2))
      subscriber.messages(1) must_== Subscriber.Receive(destination, subscription, env1)
      destination.messageQueue.size must_== 1
    }
    "handle ack and dispatch message from queue" in new DestinationSpecScope {
      val (env1, env2) = subscribeAndTwoDispatch
      destination ! Destination.Ready(subscription)

      subscriber.messages.size must eventually(3, 1 second)(be_==(2))
      subscriber.messages(1) must_== Subscriber.Receive(destination, subscription, env1)

      destination ! Destination.Ack(subscription, List(env1.id))

      subscriber.messages.size must eventually(3, 1 second)(be_==(3))
      subscriber.messages(2) must_== Subscriber.Receive(destination, subscription, env2)
      destination.messageQueue.size must_== 0
    }
    "handle fail and dispatch message from queue" in new DestinationSpecScope {
      val (env1, env2) = subscribeAndTwoDispatch
      destination ! Destination.Ready(subscription)

      subscriber.messages.size must eventually(3, 1 second)(be_==(2))
      subscriber.messages(1) must_== Subscriber.Receive(destination, subscription, env1)

      destination ! Destination.Fail(subscription, List(Destination.Dispatch(env1)))

      subscriber.messages.size must eventually(3, 1 second)(be_==(3))
      subscriber.messages(2) must_== Subscriber.Receive(destination, subscription, env2)

      destination.messageQueue.size must_== 1
      destination.messageQueue(0) must_== env1
    }
    "handle ack after subscriber removal" in new DestinationSpecScope {
      destination.subscriptionSet.size must_== 0

      destination ! Destination.AddSubscriber(subscription)
      destination.subscriptionSet.size must eventually(3, 1 second)(be_==(1))
      destination.subscriptionSet.head must_== subscription
      subscriber.messages.size must eventually(3, 1 second)(be_==(1))
      subscriber.messages(0) must_== Subscriber.Subscribed(destination, subscription)

      destination ! Destination.RemoveSubscriber(subscription)
      destination ! Destination.Ack(subscription, List("foobar"))
      destination.subscriptionSet.size must eventually(3, 1 second)(be_==(0))
      destination.readySubscriptionQueue.size must_== 0
    }
  }

  trait DestinationSpecScope extends Around with Scope with Mockito {
    val dm = new MockDestinationManager
    val transportCtx: TransportCtx = mock[TransportCtx]
    val subscriber: MockSubscriber = new MockSubscriber(dm, transportCtx)
    val subscription = Subscription("/foo/bar", subscriber, true, Some("foo"))

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

    def subscribeAndTwoDispatch = {
      val env1 = Envelope(10, "1123456789".getBytes)
      val env2 = Envelope(10, "2123456789".getBytes)

      destination ! Destination.Dispatch(env1)
      destination ! Destination.AddSubscriber(subscription)
      destination ! Destination.Dispatch(env2)

      waitForWorkout

      //no rescv got because was not ready
      subscriber.messages.size must eventually(3, 1 second)(be_==(1))
      subscriber.messages(0) must_== Subscriber.Subscribed(destination, subscription)

      (env1, env2)
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
