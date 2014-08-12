package org.mtkachev.stomp.server.persistence

import org.specs2.mutable.{Around, Specification}
import org.specs2.matcher.ThrownMessages
import org.mtkachev.stomp.server.Envelope
import org.specs2.specification.Scope
import org.specs2.execute.AsResult
import org.specs2.execute.Result._
import java.nio.file.Files
import scala.util.Random

/**
 * User: mick
 * Date: 11.08.14
 * Time: 19:22
 */
class AheadLogFsStoreSpecification extends Specification with ThrownMessages {
  "fs store" should {
    "should store and load messages in parallel" in new WithFileSpecScope {
      val alStore = FSStore.aheadLogFsStore(tmpStore, 65536)
      val messagesCount = 10000
      val random = new Random()

      var lastFailedReadId = -1
      var lastNewbieReadId = 0
      var messageSet = Set.empty[Envelope]

      for (i <- 1 to messagesCount) {
        val bytes = random.nextString(random.nextInt(256) + 1).getBytes
        val envelope = Envelope(i.toString, bytes.length, bytes)
        alStore.store(envelope, i % 2 == 0, move = false)
        messageSet = messageSet + envelope

        if (i % 20 == 0) {
          val readMsgs = alStore.load(20)
          readMsgs.size should_== 20
          for (msg <- readMsgs) {
            messageSet = messageSet - msg
            val id = msg.id.toInt

            ((id == lastFailedReadId + 2) || (id == lastNewbieReadId + 2)) must beTrue

            if (lastFailedReadId + 2 == id) lastFailedReadId = id
            else if (lastNewbieReadId + 2 == id) lastNewbieReadId = id
          }
        }
      }
      messageSet.isEmpty must beTrue
      alStore.shutdown()
    }

    "should store and load messages after reinit" in new WithFileSpecScope {
      val alStore = FSStore.aheadLogFsStore(tmpStore, 65536)
      val messagesCount = 10000
      val random = new Random()
    }
  }


  trait WithFileSpecScope extends Around with Scope {
    val tmpStorePath = Files.createTempDirectory("AheadLogFsStoreSpecification_")
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
      true
    }
  }
}
