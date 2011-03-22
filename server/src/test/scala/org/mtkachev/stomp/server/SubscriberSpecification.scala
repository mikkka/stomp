package org.mtkachev.stomp.server

import org.specs._
import org.specs.mock.Mockito

import scala.actors.Actor

import org.specs.util.TimeConversions._
import org.apache.mina.core.session.IoSession

import org.mtkachev.stomp.server.Subscriber._
import org.mtkachev.stomp.server.codec._

/**
 * User: mick
 * Date: 17.03.11
 * Time: 20:24
 */

class SubscriberSpecification extends Specification with Mockito {
  private var subscriber: Subscriber = null
  private var dm: DestinationManager = null
  private var ioSession: IoSession = null

  "queue manager" should {
    doBefore {
      ioSession = mock[IoSession]
      dm = mock[DestinationManager]
      subscriber = new Subscriber(dm, null, "foo", "bar")
    }
    doAfter {
      subscriber ! Subscriber.Stop()
      subscriber.getState must eventually(10, 1 second)(be(Actor.State.Terminated))
    }
    "test subscrbe and unsubscribe" in {
      subscriber ! FrameMsg(Subscribe(Some("foo"), "/foo/bar", true, Map.empty))
      subscriber ! FrameMsg(Subscribe(None, "/baz/ger", false, Map.empty))

      subscriber.subscriptionList.size must eventually(10, 1 second)(be(2))

      subscriber.subscriptionList mustContain new Subscription("/foo/bar", subscriber, true, Some("foo"))
      subscriber.subscriptionList mustContain new Subscription("/baz/ger", subscriber, false, None)

      there was one(dm) ! DestinationManager.Subscribe(new Subscription("/foo/bar", subscriber, true, Some("foo")))
      there was one(dm) ! DestinationManager.Subscribe(new Subscription("/baz/ger", subscriber, false, None))

      println("subscribe test done")

      subscriber ! FrameMsg(UnSubscribe(Some("foo"), None, Map.empty))
      subscriber ! FrameMsg(UnSubscribe(None, Some("/baz/ger"), Map.empty))

      subscriber.subscriptionList.size must eventually(10, 1 second)(be(0))

      there was one(dm) ! DestinationManager.UnSubscribe(new Subscription("/foo/bar", subscriber, true, Some("foo")))
      there was one(dm) ! DestinationManager.UnSubscribe(new Subscription("/baz/ger", subscriber, false, None))
    }
  }
}