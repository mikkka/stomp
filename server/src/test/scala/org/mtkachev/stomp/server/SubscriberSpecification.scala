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

      subscriber.subscriptionsList.size must eventually(10, 100 millis)(be_==(2))

      subscriber.subscriptionsList must contain(Subscription("/foo/bar", subscriber, true, Some("foo")))
      subscriber.subscriptionsList must contain(Subscription("/baz/ger", subscriber, false, None))

      dm.messages.size must_== 2
      dm.messages must contain(DestinationManager.Subscribe(Subscription("/foo/bar", subscriber, true, Some("foo"))))
      dm.messages must contain(DestinationManager.Subscribe(Subscription("/baz/ger", subscriber, false, None)))

      subscriber ! FrameMsg(UnSubscribe(Some("foo"), None, None))
      subscriber ! FrameMsg(UnSubscribe(None, Some("/baz/ger"), None))

      subscriber.subscriptionsList.size must eventually(10, 100 millis)(be_==(0))

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
      dm.messages exists (_ match {
        case DestinationManager.Dispatch("foo/bar", x) => x.contentLength == 10 && x.body == content
        case _ => false
      })

      subscriber.getState must(be(Actor.State.Suspended))
      there was one(transportCtx).write(new Receipt("foobar"))

      success
    }
    "receive" in new SubscriberSpecScope {
      subscriber ! FrameMsg(Subscribe(Some("foo"), "/foo/bar", false, None))
      subscriber ! FrameMsg(Subscribe(None, "/baz/ger", true, None))
      subscriber.subscriptionsList.size must eventually(10, 100 millis)(be_==(2))

      val content = "0123456789".getBytes

      subscriber.subscriptionsList.foreach(s => subscriber ! Subscriber.Receive(destination, s, new Envelope("id42", 10, content)))

      there was one(transportCtx).write(argThat(matchMessage(new Message("foo", "", 10, content))))
      there was one(transportCtx).write(argThat(matchMessage(new Message("/baz/ger", "", 10, content))))

      destination.messages.size must eventually(10, 100 millis)(be_==(1))
      destination.messages(0) must_== Destination.Ack(subscriber.subscriptionsList(1), List("id42"))

      subscriber.getState must(be(Actor.State.Suspended))

      success
    }
    "ack" in new SubscriberSpecScope {
      subscriber ! FrameMsg(Subscribe(Some("foo"), "/foo/bar", true, None))
      subscriber ! FrameMsg(Subscribe(Some("baz"), "/baz/bar", true, None))
      subscriber.subscriptionsList.size must eventually(10, 100 millis)(be_==(2))

      val content11 = "1234567890_1".getBytes
      val content12 = "1234567890_2".getBytes

      val sIter = subscriber.subscriptionsList.iterator
      val subscription1 = sIter.next()
      val subscription2 = sIter.next()

      val envelope1 = Envelope(10, content11)
      val envelope2 = Envelope(10, content12)
      val srecv1 = Subscriber.Receive(destination, subscription1, envelope1)
      val srecv2 = Subscriber.Receive(destination, subscription2, envelope2)
      subscriber ! srecv1
      subscriber ! srecv2

      subscriber.pendingAcksMap.size must eventually(10, 100 millis)(be_==(2))
      subscriber.pendingAcksMap.values must contain(srecv1)
      subscriber.pendingAcksMap.values must contain(srecv2)

      subscriber.pendingAcksMap(envelope1.id) must_== srecv1
      subscriber.pendingAcksMap(envelope2.id) must_== srecv2

      subscriber.pendingAcksMap.foreach {kv =>
        subscriber ! FrameMsg(Ack(kv._1, None, None))
      }

      destination.messages.size must eventually(10, 100 millis)(be_==(2))
      destination.messages must contain(Destination.Ack(subscriber.subscriptionsList(0), List(envelope1.id)))
      destination.messages must contain(Destination.Ack(subscriber.subscriptionsList(1), List(envelope2.id)))

      there was one(transportCtx).write(argThat(matchMessage(new Message("foo", "", 10, content11))))
      there was one(transportCtx).write(argThat(matchMessage(new Message("baz", "", 10, content12))))

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

      subscriber.subscriptionsList.size must eventually(3, 1 second)(be_==(1))
      subscriber.transactionsMap.size must eventually(3, 1 second)(be_==(1))


      val envelope1 = Envelope(10, content1)
      val envelope2 = Envelope(10, content2)
      val srecv1 = Subscriber.Receive(destination, subscription, envelope1)
      val srecv2 = Subscriber.Receive(destination, subscription, envelope2)
      subscriber ! srecv1
      subscriber ! srecv2

      waitForWorkout

      val firstMsgId = subscriber.pendingAcksMap.keysIterator.next()
      subscriber ! FrameMsg(Ack(firstMsgId, Some("tx1"), None))

      waitForWorkout
      //got ready msg
      destination.messages.size must eventually(10, 100 millis)(be_==(1))
      destination.messages(0) must_== Destination.Ready(subscription)

      val secondMsgId = subscriber.pendingAcksMap.keysIterator.next()
      subscriber ! FrameMsg(Ack(secondMsgId, Some("tx1"), None))

      subscriber.pendingAcksMap.size must eventually(10, 100 millis)(be_==(0))
      //got ready msg
      destination.messages.size must eventually(10, 100 millis)(be_==(2))
      destination.messages(1) must_== Destination.Ready(subscription)

      subscriber ! FrameMsg(Send("foo/baz", 10, Some("tx1"), None, content3))

      waitForWorkout

      subscriber ! FrameMsg(Commit("tx1", None))

      subscriber.transactionsMap.size must eventually(3, 1 second)(be_==(0))

      dm.messages.size must eventually(10, 100 millis)(be_==(2))
      dm.messages(0) must_== DestinationManager.Subscribe(subscription)
      dm.messages(1) must beLike {case DestinationManager.Dispatch("foo/baz", Envelope(_, 10, content3)) => ok}

      subscriber.pendingAcksMap.size must_== 0

      //got 2 acks after commit
      destination.messages.size must eventually(10, 100 millis)(be_==(4))
      destination.messages(2) must_== Destination.Ack(subscription, List(firstMsgId))
      destination.messages(3) must_== Destination.Ack(subscription, List(secondMsgId))

      success
    }
    "tx rollback" in new SubscriberSpecScope {
      val subscription = Subscription("/foo/bar", subscriber, true, Some("foo"))
      val content1 = "1234567890_1".getBytes
      val content2 = "2345678901_1".getBytes
      val content3 = "2345678901_1".getBytes

      subscriber ! FrameMsg(Subscribe(subscription.id, subscription.expression, subscription.acknowledge, None))
      subscriber ! FrameMsg(Begin("tx1", None))

      subscriber.subscriptionsList.size must eventually(3, 1 second)(be_==(1))
      subscriber.transactionsMap.size must eventually(3, 1 second)(be_==(1))


      val envelope1 = Envelope(10, content1)
      val envelope2 = Envelope(10, content2)
      val srecv1 = Subscriber.Receive(destination, subscription, envelope1)
      val srecv2 = Subscriber.Receive(destination, subscription, envelope2)
      subscriber ! srecv1
      subscriber ! srecv2

      waitForWorkout

      dm.messages.size must eventually(10, 100 millis)(be_==(1))

      val firstMsgId = subscriber.pendingAcksMap.keysIterator.next()
      subscriber ! FrameMsg(Ack(firstMsgId, Some("tx1"), None))

      waitForWorkout
      //got ready msg
      destination.messages.size must eventually(10, 100 millis)(be_==(1))
      destination.messages(0) must_== Destination.Ready(subscription)

      val secondMsgId = subscriber.pendingAcksMap.keysIterator.next()
      subscriber ! FrameMsg(Ack(secondMsgId, Some("tx1"), None))

      dm.messages.size must eventually(10, 100 millis)(be_==(1))
      //got ready msg
      destination.messages.size must eventually(10, 100 millis)(be_==(2))
      destination.messages(1) must_== Destination.Ready(subscription)

      subscriber ! FrameMsg(Send("foo/baz", 10, Some("tx1"), None, content3))

      waitForWorkout

      subscriber ! FrameMsg(Abort("tx1", None))

      subscriber.transactionsMap.size must eventually(3, 1 second)(be_==(0))

      dm.messages.size must eventually(10, 100 millis)(be_==(1))

      subscriber.pendingAcksMap.size must_== 0

      //got 2 fails after commit
      destination.messages.size must eventually(10, 100 millis)(be_==(4))
      destination.messages(2) must_==
        Destination.Fail(subscription, List(Destination.Dispatch(Envelope(firstMsgId, 10, content1))))
      destination.messages(3) must_==
        Destination.Fail(subscription, List(Destination.Dispatch(Envelope(secondMsgId, 10, content2))))

      success
    }
  }

  trait SubscriberSpecScope extends Around with Scope with Mockito {
    val dm = new MockDestinationManager
    val destination = new MockDestination("foo")
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
      destination ! "poison"
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

  class MockDestination(override val name: String) extends Destination(name) {
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