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
      val alStore1 = FSStore.aheadLogFsStore(tmpStore, 65536)
      val messagesCount = 10000
      val onlineLoadEndMessageCount = messagesCount / 3
      val random = new Random()

      var lastFailedWriteId = 0
      var lastNewbieWriteId = 0

      var lastFailedReadId = 0
      var lastNewbieReadId = 0

      var messageSet = Set.empty[Envelope]
      var lastLoad = 1

      def createId(failed: Boolean) = if (failed) {
        lastFailedWriteId = lastFailedWriteId + 1
        "f" + lastFailedWriteId
      } else {
        lastNewbieWriteId = lastNewbieWriteId + 1
        "n" + lastNewbieWriteId
      }
      def isFailed(prob: Double) = random.nextDouble() < prob
      def createBody() = random.nextString(random.nextInt(256) + 1).getBytes

      def checkId(strId: String) {
        val id = strId.drop(1).toInt
        if (strId.startsWith("f")) {
          //check if ordered
          (id == lastFailedReadId + 1) must beTrue
          lastFailedReadId = id
        } else {
          //check if ordered
          (id == lastNewbieReadId + 1) must beTrue
          lastNewbieReadId = id
        }
      }
      def checkSet(msg: Envelope) {
        messageSet.contains(msg) must beTrue
        messageSet = messageSet - msg
      }

      //store and write in parallel
      for (i <- 1 to messagesCount) {
        val bytes = createBody()
        val failed = isFailed(0.25)
        val id = createId(failed)

        val envelope = Envelope(id, bytes.length, bytes)
        alStore1.store(envelope, failed, move = false)
        messageSet = messageSet + envelope

        //check for "online loads"
        if (i < onlineLoadEndMessageCount && random.nextDouble() < 0.05) {
          val quantity = i - lastLoad
          val readMsgs = alStore1.load(quantity)
          readMsgs.size should_== quantity
          lastLoad = i
          for (msg <- readMsgs) {
            //check if valid message (generated!!!)
            checkSet(msg)
            checkId(msg.id)
          }
        }
      }
      alStore1.shutdown()
      //check store behaviour after reinit
      val storeSize = lastNewbieWriteId - lastNewbieReadId
      val alStore2 = FSStore.aheadLogFsStore(tmpStore, 65536)

      //read all, check for goodness and no "failed" ins were encounered
      for (i <- 1 to storeSize) {
        val quantity = random.nextInt(10) + 1
        val readMsgs = alStore2.load(quantity)
        for (msg <- readMsgs) {
          checkSet(msg)
          msg.id.startsWith("n") must beTrue
          checkId(msg.id)
        }
      }

      //check all good was read!
      messageSet.forall(_.id.startsWith("f")) must beTrue

      //store and write with lag
      messageSet = Set.empty
      lastLoad = 1
      lastFailedReadId = lastFailedWriteId
      for (i <- 1 to messagesCount) {
        val bytes = createBody()
        val failed = isFailed(0.25)
        val id = createId(failed)

        val envelope = Envelope(id, bytes.length, bytes)
        alStore2.store(envelope, failed, move = false)
        messageSet = messageSet + envelope

        if (random.nextDouble() < 0.001) {
          val quantity = (i - lastLoad) / 5
          val readMsgs = alStore2.load(quantity)
          readMsgs.size should_== quantity
          lastLoad = i
          for (msg <- readMsgs) {
            //check if valid message (generated!!!)
            checkSet(msg)
            checkId(msg.id)
          }
        }
      }
      alStore2.shutdown()
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
