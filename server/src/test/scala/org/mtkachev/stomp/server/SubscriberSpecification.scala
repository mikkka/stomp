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
 * Date: 17.03.11
 * Time: 20:24
 */

class SubscriberSpecification extends Specification {
  "subscriber" should {
    "subscrbe and unsubscribe" in new SubscriberSpecScope {
      subscriber ! FrameMsg(Subscribe(Some("foo"), "/foo/bar", true, None))
      subscriber ! FrameMsg(Subscribe(None, "/baz/ger", false, None))

      subscriber.subscriptionMap.size must eventually(10, 100 millis)(be_==(2))

      subscriber.subscriptionMap.keys must contain(Subscription("/foo/bar", subscriber, true, Some("foo")))
      subscriber.subscriptionMap.keys must contain(Subscription("/baz/ger", subscriber, false, None))

      dm.messages.size must_== 2
      dm.messages must contain(DestinationManager.Subscribe(Subscription("/foo/bar", subscriber, true, Some("foo"))))
      dm.messages must contain(DestinationManager.Subscribe(Subscription("/baz/ger", subscriber, false, None)))

      subscriber ! FrameMsg(UnSubscribe(Some("foo"), None, None))
      subscriber ! FrameMsg(UnSubscribe(None, Some("/baz/ger"), None))

      subscriber.subscriptionMap.size must eventually(10, 100 millis)(be_==(0))

      dm.messages.size must_== 4
      dm.messages must contain(DestinationManager.UnSubscribe(Subscription("/foo/bar", subscriber, true, Some("foo"))))
      dm.messages must contain(DestinationManager.UnSubscribe(Subscription("/baz/ger", subscriber, false, None)))

      subscriber.getState must(be(Actor.State.Suspended))

      success
    }
    "send" in new SubscriberSpecScope {
      val content = "0123456789".getBytes
      subscriber ! FrameMsg(Send("foo/bar", 10, None, Some("foobar"), content))

      dm.messages.size must eventually(10, 100 millis)(be_==(1))
      dm.messages must contain(DestinationManager.Message("foo/bar", 10, content))

      subscriber.getState must(be(Actor.State.Suspended))
      there was one(transportCtx).write(new Receipt("foobar"))

      success
    }
    "receive" in new SubscriberSpecScope {
      subscriber ! FrameMsg(Subscribe(Some("foo"), "/foo/bar", false, None))
      subscriber ! FrameMsg(Subscribe(None, "/baz/ger", false, None))
      subscriber.subscriptionMap.size must eventually(10, 100 millis)(be_==(2))

      val content = "0123456789".getBytes

      subscriber.subscriptionMap.keys.foreach(s => subscriber ! Subscriber.Recieve(s, 10, content))

      there was one(transportCtx).write(argThat(matchMessage(new Message("foo", "", 10, content))))
      there was one(transportCtx).write(argThat(matchMessage(new Message("/baz/ger", "", 10, content))))

      subscriber.getState must(be(Actor.State.Suspended))

      success
    }
    "ack" in new SubscriberSpecScope {
      subscriber ! FrameMsg(Subscribe(Some("foo"), "/foo/bar", true, None))
      subscriber ! FrameMsg(Subscribe(Some("baz"), "/baz/bar", true, None))
      subscriber.subscriptionMap.size must eventually(10, 100 millis)(be_==(2))

      val content11 = "1234567890_1".getBytes
      val content21 = "2345678901_1".getBytes
      val content31 = "3456789012_1".getBytes

      val content12 = "1234567890_2".getBytes
      val content22 = "2345678901_2".getBytes
      val content32 = "3456789012_2".getBytes

      val sIter = subscriber.subscriptionMap.keysIterator
      val subscription1 = sIter.next()
      val subscription2 = sIter.next()

      subscriber ! Subscriber.Recieve(subscription1, 10, content11)
      subscriber ! Subscriber.Recieve(subscription2, 10, content12)

      subscriber.ackIndexMap.size must eventually(10, 100 millis)(be_==(2))
      subscriber.ackIndexMap.values must contain(Subscription("/foo/bar", subscriber, true, Some("foo")))
      subscriber.ackIndexMap.values must contain(Subscription("/baz/bar", subscriber, true, Some("baz")))
      subscriber.ackMap(subscription1).size must_== 1
      subscriber.ackMap(subscription2).size must_== 1

      subscriber ! Subscriber.Recieve(subscription1, 10, content21)
      subscriber ! Subscriber.Recieve(subscription2, 10, content22)

      subscriber.ackIndexMap.size must eventually(10, 100 millis)(be_==(2))
      subscriber.ackMap(subscription1).size must_== 1
      subscriber.ackMap(subscription2).size must_== 1
      subscriber.subscriptionMap(subscription1).size must eventually(10, 100 millis)(be_==(1))
      subscriber.subscriptionMap(subscription2).size must eventually(10, 100 millis)(be_==(1))

      subscriber ! Subscriber.Recieve(subscription1, 10, content31)
      subscriber ! Subscriber.Recieve(subscription2, 10, content32)

      subscriber.ackIndexMap.size must eventually(10, 100 millis)(be_==(2))
      subscriber.ackMap(subscription1).size must_== 1
      subscriber.ackMap(subscription2).size must_== 1
      subscriber.subscriptionMap(subscription1).size must eventually(10, 100 millis)(be_==(2))
      subscriber.subscriptionMap(subscription2).size must eventually(10, 100 millis)(be_==(2))

      for(i <- 1 to 6) {
        val msgId = subscriber.ackIndexMap.keysIterator.next()
        subscriber ! FrameMsg(Ack(msgId, None, None))
        if(i < 6) {
          subscriber.ackIndexMap.keysIterator.next must eventually(10, 100 millis)(be_!=(msgId))
        } else {
          subscriber.ackIndexMap must eventually(10, 100 millis)(beEmpty)
        }
      }

      subscriber.subscriptionMap(subscription1).size must eventually(10, 100 millis)(be_==(0))
      subscriber.subscriptionMap(subscription2).size must eventually(10, 100 millis)(be_==(0))
      subscriber.ackIndexMap.size must eventually(10, 100 millis)(be_==(0))
      subscriber.ackMap(subscription1).size must_== 0
      subscriber.ackMap(subscription2).size must_== 0

      there was one(transportCtx).write(argThat(matchMessage(new Message("foo", "", 10, content11))))
      there was one(transportCtx).write(argThat(matchMessage(new Message("foo", "", 10, content21))))
      there was one(transportCtx).write(argThat(matchMessage(new Message("foo", "", 10, content31))))

      there was one(transportCtx).write(argThat(matchMessage(new Message("baz", "", 10, content12))))
      there was one(transportCtx).write(argThat(matchMessage(new Message("baz", "", 10, content22))))
      there was one(transportCtx).write(argThat(matchMessage(new Message("baz", "", 10, content32))))

      subscriber.getState must(be(Actor.State.Suspended))

      success
    }
    "tx commit" in new SubscriberSpecScope {
      val subscription = Subscription("/foo/bar", subscriber, true, Some("foo"))
      val content1 = "1234567890_1".getBytes
      val content2 = "2345678901_1".getBytes
      val content3 = "2345678901_1".getBytes

      subscriber ! FrameMsg(Subscribe(subscription.id, subscription.expression, subscription.acknowledge, None))
      subscriber ! FrameMsg(Begin("tx1", None))

      subscriber.subscriptionMap.size must eventually(3, 1 second)(be_==(1))
      subscriber.transactionMap.size must eventually(3, 1 second)(be_==(1))

      subscriber ! Subscriber.Recieve(subscription, 10, content1)
      subscriber ! Subscriber.Recieve(subscription, 10, content2)

      waitForWorkout

      val firstMsgId = subscriber.ackIndexMap.keysIterator.next()
      subscriber ! FrameMsg(Ack(firstMsgId, Some("tx1"), None))

      waitForWorkout

      val secondMsgId = subscriber.ackIndexMap.keysIterator.next()
      subscriber ! FrameMsg(Ack(secondMsgId, Some("tx1"), None))

      subscriber.ackIndexMap.size must eventually(10, 100 millis)(be_==(0))

      subscriber ! FrameMsg(Send("foo/baz", 10, Some("tx1"), None, content3))

      waitForWorkout

      subscriber ! FrameMsg(Commit("tx1", None))

      subscriber.transactionMap.size must eventually(3, 1 second)(be_==(0))

      println(dm.messages)
      dm.messages.size must eventually(10, 100 millis)(be_==(2))
      dm.messages(0) must_== DestinationManager.Subscribe(subscription)
      dm.messages(1) must_== DestinationManager.Message("foo/baz", 10, content3)

      subscriber.ackIndexMap.size must_== 0

      success
    }
    "tx rollback" in new SubscriberSpecScope {
      val subscription = Subscription("/foo/bar", subscriber, true, Some("foo"))
      val content1 = "1234567890_1".getBytes
      val content2 = "2345678901_1".getBytes
      val content3 = "2345678901_1".getBytes

      subscriber ! FrameMsg(Subscribe(subscription.id, subscription.expression, subscription.acknowledge, None))
      subscriber ! FrameMsg(Begin("tx1", None))

      subscriber.subscriptionMap.size must eventually(3, 1 second)(be_==(1))
      subscriber.transactionMap.size must eventually(3, 1 second)(be_==(1))

      subscriber ! Subscriber.Recieve(subscription, 10, content1)
      subscriber ! Subscriber.Recieve(subscription, 10, content2)

      waitForWorkout

      dm.messages.size must eventually(10, 100 millis)(be_==(1))

      val firstMsgId = subscriber.ackIndexMap.keysIterator.next()
      subscriber ! FrameMsg(Ack(firstMsgId, Some("tx1"), None))

      waitForWorkout

      val secondMsgId = subscriber.ackIndexMap.keysIterator.next()
      subscriber ! FrameMsg(Ack(secondMsgId, Some("tx1"), None))

      dm.messages.size must eventually(10, 100 millis)(be_==(1))

      subscriber ! FrameMsg(Abort("tx1", None))

      waitForWorkout

      subscriber.transactionMap.size must eventually(3, 1 second)(be_==(0))
      dm.messages.size must eventually(10, 100 millis)(be_==(1))

      subscriber.ackIndexMap.size must_== 2
      subscriber.ackMap(subscription).size must_== 2

      success
    }
  }

  trait SubscriberSpecScope extends Around with Scope with Mockito {
    val dm: MockDestinationManager = new MockDestinationManager
    val transportCtx: TransportCtx = mock[TransportCtx]
    val subscriber: Subscriber = new Subscriber(dm, transportCtx, "foo", "bar")

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
      dm ! "poison"
      subscriber ! Subscriber.Stop()
      subscriber.getState must eventually(10, 100 millis)(be(Actor.State.Terminated))
      scala.actors.Scheduler.impl.shutdown()

      success
    }

    def waitForWorkout {
      subscriber.getState must eventually(10, 100 millis)(be(Actor.State.Suspended))
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
}