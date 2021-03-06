package org.mtkachev.stomp.server.persistence

import org.specs2.mutable.{Around, Specification}
import org.specs2.matcher.ThrownMessages
import java.io.File
import org.mtkachev.stomp.server.Envelope
import org.specs2.specification.Scope
import org.specs2.execute.AsResult
import org.specs2.execute.Result._

/**
 * User: mick
 * Date: 10.07.14
 * Time: 19:07
 */
class FSStoreSpecification extends Specification with ThrownMessages {
  "fs store" should {
    "simpleJournalFsStore store, then init" in new WithFileSpecScope {
      val store1 = FSStore.simpleJournalFsStore(tmpStore)

      store1.init().size should_== 0

      store1.store(new Envelope("01", 1, "q".getBytes),      fail = false, move = true)
      store1.remove(List("01"))
      store1.store(new Envelope("02", 2, "qw".getBytes),     fail = false, move = true)
      store1.store(new Envelope("03", 3, "qwe".getBytes),    fail = false, move = true)
      store1.store(new Envelope("03", 3, "qwe".getBytes),    fail = true, move = true)
      store1.remove(List("02", "03"))
      store1.store(new Envelope("04", 4, "qwer".getBytes),   fail = false, move = true)
      store1.remove(List("04"))
      store1.store(new Envelope("05", 5, "qwert".getBytes),  fail = false, move = true)
      store1.store(new Envelope("06", 6, "qwerty".getBytes), fail = false, move = true)
      store1.remove(List("05"))

      store1.shutdown()

      val store2 = FSStore.simpleJournalFsStore(tmpStore)
      val initRes = store2.init()

      initRes should_== Vector(new Envelope("06", 6, "qwerty".getBytes))
    }
  }

  trait WithFileSpecScope extends Around with Scope {
    val tmpStore = File.createTempFile("simple_store_spec", ".log")
    tmpStore.deleteOnExit()

    def around[T: AsResult](t: => T) = {
      issues(
        List(
          implicitly[AsResult[T]].asResult(t),
          cleanUp
        ),
        ";"
      )
    }

    def cleanUp = {
      tmpStore.delete()
    }
  }

}
