package org.mtkachev.stomp.server

import org.specs._
import org.specs.mock.Mockito

import scala.actors.Actor

import org.specs.util.TimeConversions._

object DestinationManagerCodeSpecification extends Specification with Mockito {
/*  private var destinationManager: DestinationManager = null
  private var subscriber: Subscriber = null

  "queue manager" should {
    doBefore {
      destinationManager = new DestinationManager
      subscriber = new Subscriber(destinationManager, null, "foo", "bar")
    }
    doAfter {
      destinationManager ! DestinationManager.Stop()
      destinationManager.getState must eventually(10, 1 second)(be(Actor.State.Terminated))
      if(subscriber != null) {
        subscriber ! Subscriber.Stop()
      }
    }

    "handle subscribe message but no queue" in {
      val msg = Subscription("foo/bar", subscriber, true, Option("123"))
      destinationManager ! DestinationManager.Subscribe(msg)

      destinationManager.subscriptionList.size must eventually(10, 1 second) (be(1))
      destinationManager.queueMap.size must eventually(10, 1 second) (be(0))
    }

    "handle message dispatch for new queue" in {
      destinationManager ! DestinationManager.Message("foo/bar", 0, Array.empty[Byte])
      destinationManager.queueMap.size must eventually(10, 1 second) (be(1))
    }

    "handle subscribe message for existing queue" in {
      val subscription = Subscription("foo/bar", subscriber, true, Option("123"))

      destinationManager ! DestinationManager.Message("foo/bar", 0, Array.empty[Byte])
      destinationManager ! DestinationManager.Subscribe(subscription)

      destinationManager.subscriptionList.size must eventually(10, 1 second) (be(1))
      destinationManager.queueMap.size must eventually(10, 1 second) (be(1))
      destinationManager.queueMap("foo/bar").subscriptionList.size must eventually(10, 1 second) (be(1))
    }

    "handle unsubscribe message for existing queue" in {
      val subscription = Subscription("foo/bar", subscriber, true, Option("123"))

      destinationManager ! DestinationManager.Message("foo/bar", 0, Array.empty[Byte])
      destinationManager ! DestinationManager.Subscribe(subscription)
      destinationManager ! DestinationManager.UnSubscribe(subscription)

      destinationManager.subscriptionList.size must eventually(10, 1 second) (be(0))
      destinationManager.queueMap.size must eventually(10, 1 second) (be(1))
      destinationManager.queueMap("foo/bar").subscriptionList.size must eventually(10, 1 second) (be(0))
    }

    "dispatch data message" in {
      val subscription = spy(Subscription("foo/bar", subscriber, true, Option("123")))

      destinationManager ! DestinationManager.Message("foo/bar", 3, Array[Byte](01, 02, 03))
      destinationManager ! DestinationManager.Subscribe(subscription)

      destinationManager.queueMap("foo/bar").subscriptionList.size must eventually(10, 1 second) (be(1))

      destinationManager ! DestinationManager.Message("foo/bar", 4, Array[Byte](01, 02, 03, 04))

      there was one(subscription).message(4, Array[Byte](01, 02, 03, 04))
   }
  }*/
}