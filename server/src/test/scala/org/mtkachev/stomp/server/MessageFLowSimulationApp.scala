package org.mtkachev.stomp.server

import org.mtkachev.stomp.server.codec._
import scala.util.Random
import org.mtkachev.stomp.server.DestinationManager.{Stop => DMStop, Subscribe, Dispatch}
import java.util.concurrent.{LinkedBlockingQueue, CountDownLatch}
import org.mtkachev.stomp.server.Subscriber.{Stop => SubscriberStop, FrameMsg}
import com.typesafe.scalalogging.slf4j.StrictLogging

/**
 * User: mick
 * Date: 20.05.14
 * Time: 19:07
 */
/**
 * one queue -> many subscribers message flow emulation
 */
object MessageFLowSimulationApp extends App with StrictLogging {
  val messageSourceSleepMax = 50
  val messageSourceSleepMin = 10
  val messageSourceMessagePerCycle = 1000
  val messageSourceCyclesCount = 5
  val subscriberCount = 3
  val mainRand = new Random()

  def time = System.currentTimeMillis()

  val subscriberSleep = ((1.0 * (messageSourceSleepMin + messageSourceSleepMax) / 2) * subscriberCount).toInt

  val dm = new DestinationManager
  dm.start()

  val messageProducer = new MessageProducer(dm, messageSourceSleepMin, messageSourceSleepMax,
    messageSourceMessagePerCycle, messageSourceCyclesCount)

  val messageCount = messageSourceMessagePerCycle * messageSourceCyclesCount
  val sendLatch = new CountDownLatch(messageCount)
  val recvLatch = new CountDownLatch(messageCount)

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
  var recvCounter = 0

  class SubscriberBrain(acknowledge: Boolean, id: Int) extends TransportCtx {
    @volatile var _s: Subscriber = null
    @volatile var running = false
    @volatile var isTx = false

    val sleepRand = new Random()
    def sleep = sleepRand.nextInt(subscriberSleep)

    case class AnswerTask(delay: Int, msg: FrameMsg)
    val taskQueue = new LinkedBlockingQueue[AnswerTask]()

    def write(msg: Frame) {
      logger.debug(s"client $id: recv ${msg}")
      msg match {
        case out: Message =>
          if(acknowledge) taskQueue.add(AnswerTask(sleep, FrameMsg(Ack(out.messageId, None, None))))
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
    val brain = new SubscriberBrain(acknowledge = acknowledge, num)
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
