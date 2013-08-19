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
    "handle subscrbe and unsubscribe" in new DestinationSpecScope {
    }
    "dispatch message when there are ready subscription" in new DestinationSpecScope {
    }
    "dispatch message with there are no ready subscription (store it)" in new DestinationSpecScope {
    }
    "dispatch message from queue" in new DestinationSpecScope {
    }
    "ack and dispatch message from queue" in new DestinationSpecScope {
    }
    "ready and dispatch message from queue" in new DestinationSpecScope {
    }
    "fail and dispatch message from queue" in new DestinationSpecScope {
    }
  }

  trait DestinationSpecScope extends Around with Scope with Mockito {
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
  }
}
