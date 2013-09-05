package org.mtkachev.stomp.server.persistence

import org.specs2.mutable.{Around, Specification}
import org.specs2.specification.Scope
import org.specs2.mock.Mockito
import org.specs2.execute.AsResult
import org.specs2.execute.Result._
import Persister._
import org.mtkachev.stomp.server.{Destination, Envelope}
import org.mtkachev.stomp.server.persistence.ImMemoryPersister.{Out, In}
import org.specs2.matcher.ThrownMessages

/**
 * User: mick
 * Date: 04.09.13
 * Time: 19:45
 */
class ImMemoryPersisterSpecification extends Specification with ThrownMessages {
  "in memory persister" should {
    "store events" in new ImMemoryPersisterSpecScope {

      persister ! StoreList(List(
        Envelope("in1", 1, "q".getBytes),
        Envelope("in2", 2, "aa".getBytes),
        Envelope("in3", 3, "zzz".getBytes)
      ))
      persister ! Remove("in2")
      persister ! Remove("in3")
      persister ! StoreOne(Envelope("in4", 4, "xxxx".getBytes))
      persister ! Remove("in1")

      persister.storeView.size must eventually(10, 1 second) (be_==(7))
      val storeView = persister.storeView.toList.map {
        case x: In => (x.id, x.body.toSeq)
        case x: Out => x.id
      }
      storeView must_== List(
        ("in1", "q".getBytes.toSeq),
        ("in2", "aa".getBytes.toSeq),
        ("in3", "zzz".getBytes.toSeq),
        ("in2"),
        ("in3"),
        ("in4", "xxxx".getBytes.toSeq),
        ("in1")
      )
    }
    "store 5 In's and 3 Out's and return 4 Envelopes on load query(4)" in new ImMemoryPersisterSpecScope {
      persister ! StoreOne(Envelope("in1", 4, "qwer".getBytes))
      persister ! Remove("inn1")
      persister ! StoreOne(Envelope("in2", 4, "asdf".getBytes))
      persister ! Remove("inn2")
      persister ! StoreOne(Envelope("in3", 4, "xzcv".getBytes))
      persister ! Remove("inn3")
      persister ! StoreOne(Envelope("in4", 4, "tyui".getBytes))
      persister ! StoreOne(Envelope("in5", 4, "ghjk".getBytes))

      val ans = persister !? Load(4)
      ans match {
        case loaded: Destination.Loaded => {
          loaded.envelopes.map{e => (e.id, e.contentLength, e.body.toSeq)} must_==
          Vector(
            ("in1", 4, "qwer".getBytes.toSeq),
            ("in2", 4, "asdf".getBytes.toSeq),
            ("in3", 4, "xzcv".getBytes.toSeq),
            ("in4", 4, "tyui".getBytes.toSeq)
          )

          persister.storeView.size must_== 1
          persister.storeView(0).id must_== "in5"
        }
        case _ => {
          fail("wanted Destination.Loaded but got something completely different")
        }
      }
    }
    "store 3 In's and 2 Out's and return 3 Envelopes on load query(4)" in new ImMemoryPersisterSpecScope {
      persister ! StoreOne(Envelope("in1", 4, "qwer".getBytes))
      persister ! Remove("inn1")
      persister ! StoreOne(Envelope("in2", 4, "asdf".getBytes))
      persister ! Remove("inn2")
      persister ! StoreOne(Envelope("in3", 4, "xzcv".getBytes))

      val ans = persister !? Load(4)
      ans match {
        case loaded: Destination.Loaded => {
          loaded.envelopes.map{e => (e.id, e.contentLength, e.body.toSeq)} must_==
            Vector(
              ("in1", 4, "qwer".getBytes.toSeq),
              ("in2", 4, "asdf".getBytes.toSeq),
              ("in3", 4, "xzcv".getBytes.toSeq)
            )

          persister.storeView.size must_== 0
        }
        case _ => {
          fail("wanted Destination.Loaded but got something completely different")
        }
      }
    }
  }

  trait ImMemoryPersisterSpecScope extends Around with Scope with Mockito {
    val persister = new ImMemoryPersister

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
      persister ! Stop()
      success
    }
  }
}
