package org.mtkachev.stomp.server

import org.specs._
import org.specs.mock.Mockito
import org.mockito.{Mockito => M}

import scala.actors.Actor

import org.specs.util.TimeConversions._
import org.apache.mina.core.session.IoSession

import org.mtkachev.stomp.server.Subscriber._
import org.mtkachev.stomp.server.codec._
import org.mtkachev.stomp.server.Matchers._

/**
 * User: mick
 * Date: 17.03.11
 * Time: 20:24
 */

class SubscriberSpecification extends Specification with Mockito {
  private var subscriber: Subscriber = null
  private var dm: DestinationManager = null
  private var ioSession: IoSession = null

  "subscriber" should {
    doBefore {
      ioSession = mock[IoSession]
      dm = mock[DestinationManager]
      subscriber = new Subscriber(dm, ioSession, "foo", "bar")
    }
    doAfter {
      subscriber ! Subscriber.Stop()
      subscriber.getState must eventually(10, 100 millis)(be(Actor.State.Terminated))
    }
    "subscrbe and unsubscribe" in {
      subscriber ! FrameMsg(Subscribe(Some("foo"), "/foo/bar", true, Map.empty))
      subscriber ! FrameMsg(Subscribe(None, "/baz/ger", false, Map.empty))

      subscriber.subscriptionMap.size must eventually(10, 100 millis)(be(2))

      subscriber.subscriptionMap.keys mustContain Subscription("/foo/bar", subscriber, true, Some("foo"))
      subscriber.subscriptionMap.keys mustContain Subscription("/baz/ger", subscriber, false, None)

      there was one(dm) ! DestinationManager.Subscribe(Subscription("/foo/bar", subscriber, true, Some("foo")))
      there was one(dm) ! DestinationManager.Subscribe(Subscription("/baz/ger", subscriber, false, None))

      subscriber ! FrameMsg(UnSubscribe(Some("foo"), None, Map.empty))
      subscriber ! FrameMsg(UnSubscribe(None, Some("/baz/ger"), Map.empty))

      subscriber.subscriptionMap.size must eventually(10, 100 millis)(be(0))

      there was one(dm) ! DestinationManager.UnSubscribe(Subscription("/foo/bar", subscriber, true, Some("foo")))
      there was one(dm) ! DestinationManager.UnSubscribe(Subscription("/baz/ger", subscriber, false, None))
      subscriber.getState must(be(Actor.State.Suspended))
    }
    "send" in {
      val content = "0123456789".getBytes
      subscriber ! FrameMsg(Send("foo/bar", 10, None, content, Map.empty))
      there was one(dm) ! DestinationManager.Message("foo/bar", 10, content)
      subscriber.getState must(be(Actor.State.Suspended))
    }
    "receive" in {
      subscriber ! FrameMsg(Subscribe(Some("foo"), "/foo/bar", false, Map.empty))
      subscriber ! FrameMsg(Subscribe(None, "/baz/ger", false, Map.empty))
      subscriber.subscriptionMap.size must eventually(10, 100 millis)(be(2))

      val content = "0123456789".getBytes

      subscriber.subscriptionMap.keys.foreach(s => subscriber ! Subscriber.Recieve(s, 10, content))

      there was one(ioSession).write(argThat(matchMessage(new Message("foo", "", 10, content, Map.empty))))
      there was one(ioSession).write(argThat(matchMessage(new Message("/baz/ger", "", 10, content, Map.empty))))

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

      val sIter = subscriber.subscriptionMap().keysIterator
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

      there was one(ioSession).write(argThat(matchMessage(new Message("foo", "", 10, content11, Map.empty))))
      there was one(ioSession).write(argThat(matchMessage(new Message("foo", "", 10, content21, Map.empty))))
      there was one(ioSession).write(argThat(matchMessage(new Message("foo", "", 10, content31, Map.empty))))

      there was one(ioSession).write(argThat(matchMessage(new Message("baz", "", 10, content12, Map.empty))))
      there was one(ioSession).write(argThat(matchMessage(new Message("baz", "", 10, content22, Map.empty))))
      there was one(ioSession).write(argThat(matchMessage(new Message("baz", "", 10, content32, Map.empty))))

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
      subscriber ! FrameMsg(Ack(firstMsgId, None, Map.empty))

      waitForWorkout

      val secondMsgId = subscriber.ackIndexMap.keysIterator.next()
      subscriber ! FrameMsg(Ack(secondMsgId, None, Map.empty))

      subscriber.ackIndexMap.size must eventually(10, 100 millis)(be(0))

      subscriber ! FrameMsg(Send("foo/baz", 10, Some("tx1"), content3, Map.empty))

      waitForWorkout

      there was no(dm) ! any[DestinationManager.Message]

      subscriber ! FrameMsg(Commit("tx1", Map.empty))

      subscriber.transactionMap.size must eventually(3, 1 second)(be(1))
      there was one(dm) ! DestinationManager.Message("foo/baz", 10, content3)
      subscriber.ackIndexMap.size mustBe 0
    }
/*
    "tx rollback" in {
    }
*/
/*
    "test mock call times" in {
      ioSession.write("foo")
      there was one(ioSession).write(anyString)
      ioSession.write("bar")
      there was one(ioSession).write(anyString)
      ioSession.write("baz")
      there was one(ioSession).write(anyString)
    }
*/
  }

  def waitForWorkout {
    subscriber.getState must eventually(10, 100 millis)(be(Actor.State.Suspended))
  }
}