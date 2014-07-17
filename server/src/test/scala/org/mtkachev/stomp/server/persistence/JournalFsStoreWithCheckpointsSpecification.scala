package org.mtkachev.stomp.server.persistence

import org.specs2.mutable.{Around, Specification}
import org.specs2.matcher.ThrownMessages
import java.io.File
import org.mtkachev.stomp.server.Envelope
import org.specs2.specification.Scope
import org.specs2.execute.AsResult
import org.specs2.execute.Result._
import java.nio.file.Files
import scala.util.Random

/**
 * User: mick
 * Date: 17.07.14
 * Time: 17:07
 */
class JournalFsStoreWithCheckpointsSpecification extends Specification with ThrownMessages {
  "fs store" should {
    "journal with checkpoints should store, create checkpoints and reinit after restart" in new WithFileSpecScope {
      val iterationsCount = 10000
      val store1 = FSStore.journalFsStoreWithCheckpoints(tmpStore, 349)
      store1.init()

      var lastAddId = 0
      val lastRemoveId = 0
      val random = new Random()
      for(i <- 1 to iterationsCount) {
        val bytes = random.nextString(random.nextInt(256) + 1).getBytes
        store1.store(new Envelope(lastAddId.toString, bytes.length, bytes), fail = false, move = true)
        lastAddId = lastAddId + 1
      }
      store1.shutdown()

      val store2 = FSStore.journalFsStoreWithCheckpoints(tmpStore, 349)
      store2.init().size should_== 10000
    }
  }

  trait WithFileSpecScope extends Around with Scope {
    val tmpStorePath = Files.createTempDirectory("JournalFsStoreWithCheckpointsSpecification_")
    val tmpStore = tmpStorePath.toFile

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
      tmpStore.listFiles().foreach(_.delete())
      tmpStore.delete()
    }
  }
}
