package org.mtkachev.stomp.server

import org.specs._
import org.specs.mock.Mockito
import org.mockito.Matchers._

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
      subscriber.getState must eventually(3, 1 second)(be(Actor.State.Terminated))
    }
    "subscrbe and unsubscribe" in {
      subscriber ! FrameMsg(Subscribe(Some("foo"), "/foo/bar", true, Map.empty))
      subscriber ! FrameMsg(Subscribe(None, "/baz/ger", false, Map.empty))

      subscriber.subscriptionList.size must eventually(3, 1 second)(be(2))

      subscriber.subscriptionList mustContain new Subscription("/foo/bar", subscriber, true, Some("foo"))
      subscriber.subscriptionList mustContain new Subscription("/baz/ger", subscriber, false, None)

      there was one(dm) ! DestinationManager.Subscribe(new Subscription("/foo/bar", subscriber, true, Some("foo")))
      there was one(dm) ! DestinationManager.Subscribe(new Subscription("/baz/ger", subscriber, false, None))

      subscriber ! FrameMsg(UnSubscribe(Some("foo"), None, Map.empty))
      subscriber ! FrameMsg(UnSubscribe(None, Some("/baz/ger"), Map.empty))

      subscriber.subscriptionList.size must eventually(3, 1 second)(be(0))

      there was one(dm) ! DestinationManager.UnSubscribe(new Subscription("/foo/bar", subscriber, true, Some("foo")))
      there was one(dm) ! DestinationManager.UnSubscribe(new Subscription("/baz/ger", subscriber, false, None))
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
      subscriber.subscriptionList.size must eventually(3, 1 second)(be(2))

      val content = "0123456789".getBytes
      subscriber ! Subscriber.Recieve(subscriber.subscriptionList(0), 10, content)
      subscriber ! Subscriber.Recieve(subscriber.subscriptionList(1), 10, content)

      there was one(ioSession).write(argThat(matchMessage(new Message("foo", "", 10, content, Map.empty))))
      there was one(ioSession).write(argThat(matchMessage(new Message("/baz/ger", "", 10, content, Map.empty))))

      subscriber.getState must(be(Actor.State.Suspended))
    }
    "ack" in {
      subscriber ! FrameMsg(Subscribe(Some("foo"), "/foo/bar", true, Map.empty))
      subscriber.subscriptionList.size must eventually(3, 1 second)(be(1))

      val content = "0123456789".getBytes
      val subscription: Subscription = subscriber.subscriptionList(0)

      subscriber ! Subscriber.Recieve(subscription, 10, content)
      subscriber ! Subscriber.Recieve(subscription, 10, content)

      subscriber.ackList.size must eventually(3, 1 second)(be(1))

      there was one(ioSession).write(argThat(matchMessage(new Message("foo", "", 10, content, Map.empty))))
      subscriber ! FrameMsg(Ack(subscriber.ackList(0), None, Map.empty))

      there was one(ioSession).write(argThat(matchMessage(new Message("foo", "", 10, content, Map.empty))))
      subscriber ! FrameMsg(Ack(subscriber.ackList(0), None, Map.empty))
      subscriber.ackList.size must eventually(3, 1 second)(be(0))

      subscriber.getState must(be(Actor.State.Suspended))
    }
/*
    "tx commit" in {
    }
    "tx rollback" in {
    }
*/
  }
}