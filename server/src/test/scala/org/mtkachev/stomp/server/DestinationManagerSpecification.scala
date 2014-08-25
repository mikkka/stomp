package org.mtkachev.stomp.server

import org.specs2.mutable._
import org.specs2.mock.Mockito

import scala.actors.Actor
import org.specs2.execute.{AsResult, StandardResults}
import org.specs2.execute.Result._
import org.specs2.specification.Scope
import org.mtkachev.stomp.server.Matchers._
import org.mtkachev.stomp.codec.Message

class DestinationManagerSpecification extends Specification with Mockito {
  "queue manager" should {

    "handle subscribe message but no queue" in new DestinationManagerSpecScope {
      val msg = Subscription("foo/bar", subscriber, acknowledge = true, Option("123"))
      destinationManager ! DestinationManager.Subscribe(msg)

      destinationManager.subscriptionList.size must eventually(10, 1 second) (be_==(1))
      destinationManager.queueMap.size must eventually(10, 1 second) (be_==(0))

      success
    }

    "handle message dispatch for new queue" in new DestinationManagerSpecScope {
      destinationManager ! DestinationManager.Dispatch("foo/bar", Envelope(0, Array.empty[Byte]))
      destinationManager.queueMap.size must eventually(10, 1 second) (be_==(1))

      success
    }

    "handle subscribe message for existing queue" in new DestinationManagerSpecScope {
      val subscription = Subscription("foo/bar", subscriber, acknowledge = true, Option("123"))

      destinationManager ! DestinationManager.Dispatch("foo/bar", Envelope(0, Array.empty[Byte]))
      destinationManager ! DestinationManager.Subscribe(subscription)

      destinationManager.subscriptionList.size must eventually(10, 1 second) (be_==(1))
      destinationManager.queueMap.size must eventually(10, 1 second) (be_==(1))
      destinationManager.queueMap("foo/bar").subscriptionSet.size must eventually(10, 1 second) (be_==(1))

      success
    }

    "handle unsubscribe message for existing queue" in new DestinationManagerSpecScope {
      val subscription = Subscription("foo/bar", subscriber, acknowledge = true, Option("123"))

      destinationManager ! DestinationManager.Dispatch("foo/bar", Envelope(0, Array.empty[Byte]))
      destinationManager ! DestinationManager.Subscribe(subscription)
      destinationManager ! DestinationManager.UnSubscribe(subscription)

      destinationManager.subscriptionList.size must eventually(10, 1 second) (be_==(0))
      destinationManager.queueMap.size must eventually(10, 1 second) (be_==(1))
      destinationManager.queueMap("foo/bar").subscriptionSet.size must eventually(10, 1 second) (be_==(0))

      success
    }

    "dispatch data message" in new DestinationManagerSpecScope {
      val subscription = Subscription("foo/bar", subscriber, acknowledge = false, Option("123"))

      destinationManager ! DestinationManager.Dispatch("foo/bar", Envelope(3, Array[Byte](1, 2, 3)))
      destinationManager ! DestinationManager.Subscribe(subscription)

      destinationManager.queueMap("foo/bar").subscriptionSet.size must eventually(10, 1 second) (be_==(1))

      destinationManager ! DestinationManager.Dispatch("foo/bar", Envelope(4, Array[Byte](1, 2, 3, 4)))

      //there was one(subscription).message(mockEq(3), any[Array[Byte]])
      there was two(transportCtx).write(any[Message])
      there was one(transportCtx).write(argThat(matchMessage(
        new Message("123", "", 3, Array[Byte](1, 2, 3)))))
      there was one(transportCtx).write(argThat(matchMessage(
        new Message("123", "", 4, Array[Byte](1, 2, 3, 4)))))

      success
   }
   //TODO: write test for ack (i.e. subscriber can get next message only after ack!)
  }

  trait DestinationManagerSpecScope extends Around with Scope {
    //val transportCtx = new MockTransportCtx
    val transportCtx: TransportCtx = mock[TransportCtx]
    val destinationManager = new DestinationManager(new SimpleDestinationFactory(1024))
    val subscriber = new Subscriber(destinationManager, transportCtx, "foo", "bar")

    destinationManager.start()

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
      destinationManager ! DestinationManager.Stop()
      destinationManager.getState must eventually(10, 1 second)(be(Actor.State.Terminated))
      if(subscriber != null) {
        subscriber ! Subscriber.Stop()
      }
      subscriber.getState must eventually(10, 1 second)(be(Actor.State.Terminated))
      scala.actors.Scheduler.impl.shutdown()
      success
    }
  }
}