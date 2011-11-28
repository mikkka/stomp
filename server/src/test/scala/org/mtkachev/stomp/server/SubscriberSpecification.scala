package org.mtkachev.stomp.server

import org.specs.Specification
import org.specs.mock.Mockito
import scala.actors.Actor

import org.specs.util.TimeConversions._

import org.mtkachev.stomp.server.Subscriber._
import org.mtkachev.stomp.server.codec._
import org.mtkachev.stomp.server.Matchers._
/**
 * User: mick
 * Date: 17.03.11
 * Time: 20:24
 */

object SubscriberSpecification extends Specification with Mockito {
  private var subscriber: Subscriber = null
  private var dm: MockDestinationManager = null
  private var transportCtx: TransportCtx = null

  "subscriber" should {
    doBefore {
      transportCtx = mock[TransportCtx]
      dm = new MockDestinationManager
      subscriber = new Subscriber(dm, transportCtx, "foo", "bar")
    }
    doAfter {
      dm ! "poison"
      subscriber ! Subscriber.Stop()
      subscriber.getState must eventually(10, 100 millis)(be(Actor.State.Terminated))
      scala.actors.Scheduler.impl.shutdown()
    }
    "subscrbe and unsubscribe" in {
      subscriber ! FrameMsg(Subscribe(Some("foo"), "/foo/bar", true, Map.empty))
      subscriber ! FrameMsg(Subscribe(None, "/baz/ger", false, Map.empty))

      subscriber.subscriptionMap.size must eventually(10, 100 millis)(be(2))

      subscriber.subscriptionMap.keys mustContain Subscription("/foo/bar", subscriber, true, Some("foo"))
      subscriber.subscriptionMap.keys mustContain Subscription("/baz/ger", subscriber, false, None)

      dm.messages.size must_== 2
      dm.messages mustContain DestinationManager.Subscribe(Subscription("/foo/bar", subscriber, true, Some("foo")))
      dm.messages mustContain DestinationManager.Subscribe(Subscription("/baz/ger", subscriber, false, None))

      subscriber ! FrameMsg(UnSubscribe(Some("foo"), None, Map.empty))
      subscriber ! FrameMsg(UnSubscribe(None, Some("/baz/ger"), Map.empty))

      subscriber.subscriptionMap.size must eventually(10, 100 millis)(be(0))

      dm.messages.size must_== 4
      dm.messages mustContain DestinationManager.UnSubscribe(Subscription("/foo/bar", subscriber, true, Some("foo")))
      dm.messages mustContain DestinationManager.UnSubscribe(Subscription("/baz/ger", subscriber, false, None))

      subscriber.getState must(be(Actor.State.Suspended))
    }
    "send" in {
      val content = "0123456789".getBytes
      subscriber ! FrameMsg(Send("foo/bar", 10, None, content, Map.empty))

      dm.messages.size must eventually(10, 100 millis)(be(1))
      dm.messages mustContain DestinationManager.Message("foo/bar", 10, content)

      subscriber.getState must(be(Actor.State.Suspended))
    }
    "receive" in {
      subscriber ! FrameMsg(Subscribe(Some("foo"), "/foo/bar", false, Map.empty))
      subscriber ! FrameMsg(Subscribe(None, "/baz/ger", false, Map.empty))
      subscriber.subscriptionMap.size must eventually(10, 100 millis)(be(2))

      val content = "0123456789".getBytes

      subscriber.subscriptionMap.keys.foreach(s => subscriber ! Subscriber.Recieve(s, 10, content))

      there was one(transportCtx).write(argThat(matchMessage(new Message("foo", "", 10, content, Map.empty))))
      there was one(transportCtx).write(argThat(matchMessage(new Message("/baz/ger", "", 10, content, Map.empty))))

      subscriber.getState must(be(Actor.State.Suspended))
    }
    "ack" in {
      subscriber ! FrameMsg(Subscribe(Some("foo"), "/foo/bar", true, Map.empty))
      subscriber ! FrameMsg(Subscribe(Some("baz"), "/baz/bar", true, Map.empty))
      subscriber.subscriptionMap.size must eventually(10, 100 millis)(be(2))

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

      subscriber.ackIndexMap.size must eventually(10, 100 millis)(be(2))
      subscriber.ackIndexMap.values mustContain Subscription("/foo/bar", subscriber, true, Some("foo"))
      subscriber.ackIndexMap.values mustContain Subscription("/baz/bar", subscriber, true, Some("baz"))
      subscriber.ackMap(subscription1).size mustBe 1
      subscriber.ackMap(subscription2).size mustBe 1

      subscriber ! Subscriber.Recieve(subscription1, 10, content21)
      subscriber ! Subscriber.Recieve(subscription2, 10, content22)

      subscriber.ackIndexMap.size must eventually(10, 100 millis)(be(2))
      subscriber.ackMap(subscription1).size mustBe 1
      subscriber.ackMap(subscription2).size mustBe 1
      subscriber.subscriptionMap(subscription1).size must eventually(10, 100 millis)(be(1))
      subscriber.subscriptionMap(subscription2).size must eventually(10, 100 millis)(be(1))

      subscriber ! Subscriber.Recieve(subscription1, 10, content31)
      subscriber ! Subscriber.Recieve(subscription2, 10, content32)

      subscriber.ackIndexMap.size must eventually(10, 100 millis)(be(2))
      subscriber.ackMap(subscription1).size mustBe 1
      subscriber.ackMap(subscription2).size mustBe 1
      subscriber.subscriptionMap(subscription1).size must eventually(10, 100 millis)(be(2))
      subscriber.subscriptionMap(subscription2).size must eventually(10, 100 millis)(be(2))

      for(i <- 1 to 6) {
        val msgId = subscriber.ackIndexMap.keysIterator.next()
        subscriber ! FrameMsg(Ack(msgId, None, Map.empty))
        if(i < 6) {
          subscriber.ackIndexMap.keysIterator.next must eventually(10, 100 millis)(notBe(msgId))
        } else {
          subscriber.ackIndexMap must eventually(10, 100 millis)(beEmpty)
        }
      }

      subscriber.subscriptionMap(subscription1).size must eventually(10, 100 millis)(be(0))
      subscriber.subscriptionMap(subscription2).size must eventually(10, 100 millis)(be(0))
      subscriber.ackIndexMap.size must eventually(10, 100 millis)(be(0))
      subscriber.ackMap(subscription1).size mustBe 0
      subscriber.ackMap(subscription2).size mustBe 0

      there was one(transportCtx).write(argThat(matchMessage(new Message("foo", "", 10, content11, Map.empty))))
      there was one(transportCtx).write(argThat(matchMessage(new Message("foo", "", 10, content21, Map.empty))))
      there was one(transportCtx).write(argThat(matchMessage(new Message("foo", "", 10, content31, Map.empty))))

      there was one(transportCtx).write(argThat(matchMessage(new Message("baz", "", 10, content12, Map.empty))))
      there was one(transportCtx).write(argThat(matchMessage(new Message("baz", "", 10, content22, Map.empty))))
      there was one(transportCtx).write(argThat(matchMessage(new Message("baz", "", 10, content32, Map.empty))))

      subscriber.getState must(be(Actor.State.Suspended))
    }
    "tx commit" in {
      val subscription = Subscription("/foo/bar", subscriber, true, Some("foo"))
      val content1 = "1234567890_1".getBytes
      val content2 = "2345678901_1".getBytes
      val content3 = "2345678901_1".getBytes

      subscriber ! FrameMsg(Subscribe(subscription.id, subscription.expression, subscription.acknowledge, Map.empty))
      subscriber ! FrameMsg(Begin("tx1", Map.empty))

      subscriber.subscriptionMap.size must eventually(3, 1 second)(be(1))
      subscriber.transactionMap.size must eventually(3, 1 second)(be(1))

      subscriber ! Subscriber.Recieve(subscription, 10, content1)
      subscriber ! Subscriber.Recieve(subscription, 10, content2)

      waitForWorkout

      val firstMsgId = subscriber.ackIndexMap.keysIterator.next()
      subscriber ! FrameMsg(Ack(firstMsgId, Some("tx1"), Map.empty))

      waitForWorkout

      val secondMsgId = subscriber.ackIndexMap.keysIterator.next()
      subscriber ! FrameMsg(Ack(secondMsgId, Some("tx1"), Map.empty))

      subscriber.ackIndexMap.size must eventually(10, 100 millis)(be(0))

      subscriber ! FrameMsg(Send("foo/baz", 10, Some("tx1"), content3, Map.empty))

      waitForWorkout

      subscriber ! FrameMsg(Commit("tx1", Map.empty))

      subscriber.transactionMap.size must eventually(3, 1 second)(be(0))

      println(dm.messages)
      dm.messages.size must eventually(10, 100 millis)(be(2))
      dm.messages(0) must_== DestinationManager.Subscribe(subscription)
      dm.messages(1) must_== DestinationManager.Message("foo/baz", 10, content3)

      subscriber.ackIndexMap.size mustBe 0
    }
    "tx rollback" in {
      val subscription = Subscription("/foo/bar", subscriber, true, Some("foo"))
      val content1 = "1234567890_1".getBytes
      val content2 = "2345678901_1".getBytes
      val content3 = "2345678901_1".getBytes

      subscriber ! FrameMsg(Subscribe(subscription.id, subscription.expression, subscription.acknowledge, Map.empty))
      subscriber ! FrameMsg(Begin("tx1", Map.empty))

      subscriber.subscriptionMap.size must eventually(3, 1 second)(be(1))
      subscriber.transactionMap.size must eventually(3, 1 second)(be(1))

      subscriber ! Subscriber.Recieve(subscription, 10, content1)
      subscriber ! Subscriber.Recieve(subscription, 10, content2)

      waitForWorkout

      dm.messages.size must eventually(10, 100 millis)(be(1))

      val firstMsgId = subscriber.ackIndexMap.keysIterator.next()
      subscriber ! FrameMsg(Ack(firstMsgId, Some("tx1"), Map.empty))

      waitForWorkout

      val secondMsgId = subscriber.ackIndexMap.keysIterator.next()
      subscriber ! FrameMsg(Ack(secondMsgId, Some("tx1"), Map.empty))

      dm.messages.size must eventually(10, 100 millis)(be(1))

      subscriber ! FrameMsg(Abort("tx1", Map.empty))

      waitForWorkout

      subscriber.transactionMap.size must eventually(3, 1 second)(be(0))
      dm.messages.size must eventually(10, 100 millis)(be(1))

      subscriber.ackIndexMap.size mustBe 2
      subscriber.ackMap(subscription).size mustBe 2
    }
  }

  def waitForWorkout {
    subscriber.getState must eventually(10, 100 millis)(be(Actor.State.Suspended))
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