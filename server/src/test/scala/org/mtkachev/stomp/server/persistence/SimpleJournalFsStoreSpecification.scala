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
class SimpleJournalFsStoreSpecification extends Specification with ThrownMessages {
  "simple fs persister" should {
    "store and retive events with move = false" in new WithFileSpecScope {
      val store = new SimpleJournalFsStore(tmpStore)

      store.init().size should_== 0

      store.store(new Envelope("01", 3, "qwe".getBytes), fail = false, move = false)
      store.store(new Envelope("02", 4, "qwer".getBytes), fail = false, move = false)
      store.store(new Envelope("03", 5, "qwert".getBytes), fail = false, move = false)

      store.load(3) should_== Vector(
        new Envelope("01", 3, "qwe".getBytes),
        new Envelope("02", 4, "qwer".getBytes),
        new Envelope("03", 5, "qwert".getBytes))


      store.shutdown()
    }
    "store and retive events online, return left events and become empty after reinit" in new WithFileSpecScope {
      val store = new SimpleJournalFsStore(tmpStore)

      store.init().size should_== 0
      store.load(22).size should_== 0

      store.store(new Envelope("01", 4, "q".getBytes),      fail = false, move = false)
      store.store(new Envelope("02", 4, "qw".getBytes),     fail = false, move = false)
      store.store(new Envelope("03", 4, "qwe".getBytes),    fail = false, move = false)
      store.store(new Envelope("04", 4, "qwer".getBytes),   fail = false, move = false)
      store.store(new Envelope("05", 4, "qwert".getBytes),  fail = false, move = false)
      store.store(new Envelope("06", 4, "qwerty".getBytes), fail = false, move = false)

      store.load(3) should_== Vector(
        new Envelope("03", 4, "qwer".getBytes),
        new Envelope("04", 4, "qwer".getBytes),
        new Envelope("05", 4, "qwer".getBytes))

      store.load(3) should_== Vector(new Envelope("06", 4, "qwer".getBytes))

      store.load(3).size should_== 0
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
