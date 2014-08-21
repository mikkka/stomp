package org.mtkachev.stomp.server

import org.mtkachev.stomp.server.codec._
import scala.util.Random
import org.mtkachev.stomp.server.DestinationManager.{Stop => DMStop, Subscribe, Dispatch}
import java.util.concurrent.{LinkedBlockingQueue, CountDownLatch}
import org.mtkachev.stomp.server.Subscriber.{Stop => SubscriberStop, FrameMsg}
import com.typesafe.scalalogging.slf4j.StrictLogging
import java.util.UUID
import org.mtkachev.stomp.server.persistence._
import java.nio.file.Files
import java.io.File
import org.mtkachev.stomp.server.codec.Ack
import org.mtkachev.stomp.server.codec.Message
import org.mtkachev.stomp.server.DestinationManager.Dispatch
import org.mtkachev.stomp.server.codec.Commit
import scala.Some
import org.mtkachev.stomp.server.DestinationManager.Subscribe
import org.mtkachev.stomp.server.Subscriber.FrameMsg
import org.mtkachev.stomp.server.codec.Abort
import org.mtkachev.stomp.server.codec.Begin

/**
 * User: mick
 * Date: 20.05.14
 * Time: 19:07
 */
/**
 * one queue -> many subscribers message flow emulation
 */
object MessageFLowSimulationApp extends App with StrictLogging {
  val messageSourceSleepMin = args(0).toInt //ex. 10
  val messageSourceSleepMax = args(1).toInt //ex. 50
  val messageSourceMessagePerCycle = args(2).toInt //ex. 1000
  val messageSourceCyclesCount = args(3).toInt //ex. 5
  val subscriberCount = args(4).toInt //ex. 3

  val txSizeMin = args(5).toInt // ex. 5
  val txSizeMax = args(6).toInt // ex. 10
  val txStartProbability = args(7).toDouble // ex. 0.1
  val txSuccessProbability = args(8).toDouble  // ex 0.9

  val storeDir = if (args.size == 10) new File(args(9))
  else Files.createTempDirectory("MessageFLowSimulation_").toFile

  val mainRand = new Random()

  def time = System.currentTimeMillis()

  val subscriberSleep = ((1.0 * (messageSourceSleepMin + messageSourceSleepMax) / 2) * subscriberCount).toInt

  val store = FSStore.journalAndAheadLogStore(storeDir, 100, 102400)
  val prevMessages = store.init()

  println(s"prev messages size: ${prevMessages.size}")
  val dm = new DestinationManager(destName =>
    new Destination(destName, 256,
      new Persister(new BackgroundThreadPersisterWorker(new StorePersisterWorker(store))), prevMessages))

  dm.start()

  val messageProducer = new MessageProducer(dm, messageSourceSleepMin, messageSourceSleepMax,
    messageSourceMessagePerCycle, messageSourceCyclesCount)

  val messageCount = messageSourceMessagePerCycle * messageSourceCyclesCount
  val sendLatch = new CountDownLatch(messageCount)

  class MessageProducer(dm: DestinationManager,
                        sleepMin: Int, sleepMax: Int,
                        messagesPerCycle: Int, cyclesCount: Int) {
    val halfMessagesPerCycle = messagesPerCycle / 2
    @volatile var msgCounter = 0
    val queueRand = new Random()
    val bodyRand = new Random()
    val sleepRand = new Random()

    def posInCycle = msgCounter % messagesPerCycle
    def isSlowCycle = (msgCounter / halfMessagesPerCycle) % 2 == 1

    def sleepTime = sleepRand.nextInt(if (isSlowCycle) sleepMax else sleepMin)

    val sendingThread = new Thread(new Runnable {
      override def run() {
        while (msgCounter < messageCount) {
          val body = new Array[Byte](10)
          bodyRand.nextBytes(body)
          dm ! Dispatch("queue", Envelope(10, body))
          logger.debug(s"sender: sent")
          msgCounter = msgCounter + 1
          sendLatch.countDown()
          val slippa = sleepTime
          Thread.sleep(slippa)
        }
      }
    })

    def start() {
      sendingThread.start()
    }
  }

  val recvCounterSync = "recvCounterSync"
  val recvLatch = new CountDownLatch(messageCount)
  var recvCounter = 0

  class SubscriberBrain(acknowledge: Boolean, id: Int,
                        txSizeMin: Int, txSizeMax: Int, txStartProbability: Double, txSuccessProbability: Double) extends TransportCtx {
    @volatile var _s: Subscriber = null
    @volatile var running = false

    val txSyncLock = new String("txSyncLock")
    @volatile var txId = Option.empty[String]
    var txMsgLef = 0

    val sleepRand = new Random()
    val txStartRand = new Random()
    val txSuccessRand = new Random()
    val txSizeRand = new Random()

    def sleep = sleepRand.nextInt(subscriberSleep)

    case class AnswerTask(delay: Int, msg: FrameMsg)
    val taskQueue = new LinkedBlockingQueue[AnswerTask]()

    def write(msg: Frame) {
      logger.debug(s"client $id: recv ${msg}")
      msg match {
        case out: Message =>
          if(acknowledge) {
            taskQueue.add(AnswerTask(sleep, FrameMsg(Ack(out.messageId, txId, None))))
          } else
            txSyncLock.synchronized {
              if(txId.isEmpty) {
                if(txStartRand.nextDouble() < txStartProbability) {
                  txId = Some(UUID.randomUUID().toString)
                  txMsgLef = txSizeRand.nextInt(txSizeMax - txSizeMin) + txSizeMin
                  logger.debug(s"client $id: begin tx ${txId.get} with size ${txMsgLef}")
                  taskQueue.add(AnswerTask(sleep, FrameMsg(Begin(txId.get, None))))
                }
              } else {
                txMsgLef = txMsgLef - 1
                if (txMsgLef <= 0) {
                  txMsgLef = 0
                  if(txSuccessRand.nextDouble() < txSuccessProbability) {
                    logger.debug(s"client $id: commit tx ${txId.get}")
                    taskQueue.add(AnswerTask(sleep, FrameMsg(Commit(txId.get, None))))
                  } else {
                    logger.debug(s"client $id: abort tx ${txId.get}")
                    taskQueue.add(AnswerTask(sleep, FrameMsg(Abort(txId.get, None))))
                  }
                  txId = Option.empty[String]
                }
              }
            }
          recvLatch.countDown()
      }

      recvCounterSync.synchronized {
        recvCounter = recvCounter + 1
      }
    }

    def isClosing: Boolean = false

    def close() {
      running = false
    }

    def open() {
      running = true
      sendingThread.start()
    }

    def setSubscriber(subscriber: Subscriber) {
      _s = subscriber
    }

    val sendingThread = new Thread(new Runnable {
      override def run() {
        while (running) {
          val sendParams = taskQueue.take()
          Thread.sleep(sendParams.delay)
          _s ! sendParams.msg
          logger.debug(s"client $id: answer ${sendParams.msg}")
        }
      }
    })
  }

  //start it up!!!
  //subscribe
  val brainsUndSubs = (1 to subscriberCount).map{num =>
    //val acknowledge = mainRand.nextBoolean()
    val acknowledge = true
    val brain = new SubscriberBrain(acknowledge = acknowledge, num,
      txSizeMin, txSizeMax, txStartProbability, txSuccessProbability)
    val subscriber = Subscriber(dm, brain, "foo" + num, "bar" + num)
    brain.setSubscriber(subscriber)
    dm ! Subscribe(Subscription("queue", subscriber, acknowledge = acknowledge, None))
    brain.open()

    (brain, subscriber)
  }
  //ololo messaging!
  messageProducer.start()

  sendLatch.await()
  logger.debug("all sent")
  recvLatch.await()
  logger.debug("all recvd")

  brainsUndSubs.foreach(_._2 ! SubscriberStop())
  dm ! DMStop()

  logger.debug("done")
  recvCounterSync.synchronized {
    logger.debug("recv'ed: " + recvCounter)
  }

  System.exit(0)
}
