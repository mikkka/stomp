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
import org.mtkachev.stomp.server.persistence.{StorePersisterWorker, InMemoryStore, Persister}

/**
 * User: mick
 * Date: 17.03.11
 * Time: 20:24
 */

class SubscriberSpecification extends Specification {
  "subscriber" should {
    "subscribe and unsubscribe" in new SubscriberSpecScope {
      subscriber ! FrameMsg(Subscribe(Some("foo"), "/foo/bar", ackMode = true, None))
      subscriber ! FrameMsg(Subscribe(None, "/baz/ger", ackMode = false, None))

      subscriber.subscriptionsList.size must eventually(10, 100 millis)(be_==(2))

      subscriber.subscriptionsList must contain(Subscription("/foo/bar", subscriber, acknowledge = true, Some("foo")))
      subscriber.subscriptionsList must contain(Subscription("/baz/ger", subscriber, acknowledge = false, None))

      dm.messages.size must_== 2
      dm.messages must contain(DestinationManager.Subscribe(Subscription("/foo/bar", subscriber, acknowledge = true, Some("foo"))))
      dm.messages must contain(DestinationManager.Subscribe(Subscription("/baz/ger", subscriber, acknowledge = false, None)))

      subscriber ! FrameMsg(UnSubscribe(Some("foo"), None, None))
      subscriber ! FrameMsg(UnSubscribe(None, Some("/baz/ger"), None))

      subscriber.subscriptionsList.size must eventually(10, 100 millis)(be_==(0))

      dm.messages.size must_== 4
      dm.messages must contain(DestinationManager.UnSubscribe(Subscription("/foo/bar", subscriber, acknowledge = true, Some("foo"))))
      dm.messages must contain(DestinationManager.UnSubscribe(Subscription("/baz/ger", subscriber, acknowledge = false, None)))

      subscriber.getState must be(Actor.State.Suspended)

      success
    }
    "send" in new SubscriberSpecScope {
      val content = "0123456789".getBytes
      subscriber ! FrameMsg(Send("foo/bar", 10, None, Some("foobar"), content))

      dm.messages.size must eventually(10, 100 millis)(be_==(1))
      dm.messages exists {
        case DestinationManager.Dispatch("foo/bar", x) => x.contentLength == 10 && x.body == content
        case _ => false
      }

      subscriber.getState must be(Actor.State.Suspended)
      there was one(transportCtx).write(new Receipt("foobar"))

      success
    }
    "receive" in new SubscriberSpecScope {
      subscriber ! FrameMsg(Subscribe(Some("foo"), "/foo/bar", ackMode = false, None))
      subscriber ! FrameMsg(Subscribe(None, "/baz/ger", ackMode = true, None))
      subscriber.subscriptionsList.size must eventually(10, 100 millis)(be_==(2))

      val content = "0123456789".getBytes

      subscriber.subscriptionsList.foreach(s => subscriber ! Subscriber.Receive(destination, s, new Envelope("id42", 10, content)))

      there was one(transportCtx).write(argThat(matchMessage(new Message("foo", "", 10, content))))
      there was one(transportCtx).write(argThat(matchMessage(new Message("/baz/ger", "", 10, content))))

      destination.messages.size must eventually(10, 100 millis)(be_==(1))
      destination.messages(0) must_== Destination.Ack(subscriber.subscriptionsList(1), List("id42"))

      subscriber.getState must be(Actor.State.Suspended)

      success
    }
    "ack" in new SubscriberSpecScope {
      subscriber ! FrameMsg(Subscribe(Some("foo"), "/foo/bar", ackMode = true, None))
      subscriber ! FrameMsg(Subscribe(Some("baz"), "/baz/bar", ackMode = true, None))
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

      subscriber.getState must be(Actor.State.Suspended)

      success
    }
    "tx commit" in new SubscriberSpecScope {
      val subscription = Subscription("/foo/bar", subscriber, acknowledge = true, Some("foo"))
      val content1 = "1234567890_1".getBytes
      val content2 = "2345678901_1".getBytes
      val content3 = "2345678901_1".getBytes
      val content3seq = content3.toSeq

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

      waitForWorkout()

      val firstMsgId = subscriber.pendingAcksMap.keysIterator.next()
      subscriber ! FrameMsg(Ack(firstMsgId, Some("tx1"), None))

      waitForWorkout()
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

      waitForWorkout()

      subscriber ! FrameMsg(Commit("tx1", None))

      subscriber.transactionsMap.size must eventually(3, 1 second)(be_==(0))

      dm.messages.size must eventually(10, 100 millis)(be_==(2))
      dm.messages(0) must_== DestinationManager.Subscribe(subscription)
      dm.messages(1) must beLike {case DestinationManager.Dispatch("foo/baz", Envelope(_, 10, `content3seq`)) => ok}

      subscriber.pendingAcksMap.size must_== 0

      //got 2 acks after commit
      destination.messages.size must eventually(10, 100 millis)(be_==(4))
      destination.messages(2) must_== Destination.Ack(subscription, List(firstMsgId))
      destination.messages(3) must_== Destination.Ack(subscription, List(secondMsgId))

      success
    }
    "tx rollback" in new SubscriberSpecScope {
      val subscription = Subscription("/foo/bar", subscriber, acknowledge = true, Some("foo"))
      val content1 = "content1__".getBytes
      val content2 = "content2__".getBytes
      val content3 = "content3__".getBytes

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

      waitForWorkout()

      dm.messages.size must eventually(10, 100 millis)(be_==(1))

      subscriber ! FrameMsg(Ack(envelope1.id, Some("tx1"), None))

      waitForWorkout()
      //got ready msg
      destination.messages.size must eventually(10, 100 millis)(be_==(1))
      destination.messages(0) must_== Destination.Ready(subscription)

      subscriber ! FrameMsg(Ack(envelope2.id, Some("tx1"), None))

      dm.messages.size must eventually(10, 100 millis)(be_==(1))

      //got ready msg
      destination.messages.size must eventually(10, 100 millis)(be_==(2))
      destination.messages(1) must_== Destination.Ready(subscription)

      subscriber ! FrameMsg(Send("foo/baz", 10, Some("tx1"), None, content3))

      waitForWorkout()

      subscriber ! FrameMsg(Abort("tx1", None))

      subscriber.transactionsMap.size must eventually(3, 1 second)(be_==(0))

      dm.messages.size must eventually(10, 100 millis)(be_==(1))

      subscriber.pendingAcksMap.size must_== 0

      //got 2 fails after rollback
      destination.messages.size must eventually(10, 100 millis)(be_==(4))
      destination.messages(2) must_==
        Destination.Fail(subscription, List(Destination.Dispatch(Envelope(envelope1.id, 10, content1))))
      destination.messages(3) must_==
        Destination.Fail(subscription, List(Destination.Dispatch(Envelope(envelope2.id, 10, content2))))

      success
    }
    "on disconnect all resources should be correctly closed" in new SubscriberSpecScope {
      val subscription = Subscription("/foo/bar", subscriber, acknowledge = true, Some("foo"))
      val content1 = "content1__".getBytes
      val content2 = "content2__".getBytes

      subscriber ! FrameMsg(Subscribe(subscription.id, subscription.expression, subscription.acknowledge, None))

      val envelope1 = Envelope(10, content1)
      val envelope2 = Envelope(10, content2)

      //1st msg wo tx but no ack
      val srecv1 = Subscriber.Receive(destination, subscription, envelope1)
      subscriber ! srecv1

      //2nd msg within tx, got ack, but no tx commit
      subscriber ! FrameMsg(Begin("tx1", None))
      val srecv2 = Subscriber.Receive(destination, subscription, envelope2)
      subscriber ! srecv2

      waitForWorkout()
      subscriber.pendingAcksMap.size must eventually(10, 100 millis)(be_==(2))

      //val secondMsgId = subscriber.pendingAcksMap.find(_._2.envelope.body == content2).get._1
      subscriber ! FrameMsg(Ack(envelope2.id, Some("tx1"), None))

      waitForWorkout()
      subscriber.pendingAcksMap.size must eventually(10, 100 millis)(be_==(1))

      //disconnect

      subscriber ! FrameMsg(Disconnect(None))

      // should be ready (tx ack), fail1 (msg1 return), fail2 (msg2 return)
      destination.messages.size must eventually(10, 100 millis)(be_==(3))
      destination.messages must containAllOf(List(
        Destination.Ready(subscription),
        Destination.Fail(subscription, List(Destination.Dispatch(envelope1)), ready = false),
        Destination.Fail(subscription, List(Destination.Dispatch(envelope2)), ready = false)
      ))
      // check dest manager for unsubscribe
      dm.messages must contain(DestinationManager.UnSubscribe(subscription))

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

    def waitForWorkout() {
      subscriber.getState must eventually(10, 100 millis)(be(Actor.State.Suspended))
    }
  }

  class MockDestinationManager extends DestinationManager(dest => new MockDestination(dest)) {
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

  class MockDestination(override val name: String)
    extends Destination(name, 1024, new Persister(new StorePersisterWorker(new InMemoryStore))) {
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