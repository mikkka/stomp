package org.mtkachev.stomp.server.persistence

import org.mtkachev.stomp.server.{Destination, Envelope}
import scala.actors.{OutputChannel, Actor}
import scala.collection.GenTraversableOnce
import org.mtkachev.stomp.server.persistence.Persister._
import org.mtkachev.stomp.server.persistence.Persister.Load
import org.mtkachev.stomp.server.persistence.Persister.Remove
import org.mtkachev.stomp.server.persistence.Persister.StoreList
import org.mtkachev.stomp.server.persistence.Persister.StoreOne

/**
 * User: mick
 * Date: 02.09.13
 * Time: 18:48
 */

trait PersisterWorker {
  def dispatchOp(op: (Op, OutputChannel[Any])) : Unit
  def shutdown(): Unit
}

class StorePersisterWorker(store: Store) extends PersisterWorker {
  def dispatchOp(ops: (Op, OutputChannel[Any])) {
    val (op, sender) = ops
    op match {
      case Load(quantity) =>
        sender ! Destination.Loaded(store.load(quantity))

      case StoreOne(msg, fail, move) =>
        store.store(msg, fail, move)

      case StoreList(msgs, fail, move) =>
        store.store(msgs, fail, move)

      case Remove(id) =>
        store.remove(id)
    }
  }

  def shutdown() {
    store.shutdown()
  }
}

class BackgroundThreadPersisterWorker(delegate: PersisterWorker) extends PersisterWorker {
  import java.util.concurrent.{TimeUnit, ArrayBlockingQueue}

  private val worker = new Runnable {
    @volatile private var isStopped = false
    val opsQueue = new ArrayBlockingQueue[(Op, OutputChannel[Any])](1024)

    override def run(): Unit = {
      while (!isStopped) {
        val (op, sender) = opsQueue.poll(100, TimeUnit.MILLISECONDS)
        // null op is POISON PILL
        if(op != null)
          delegate.dispatchOp((op, sender))
        else
          isStopped = true
      }
    }

    def addOp(op: (Op, OutputChannel[Any])) {
      if(!isStopped)
        opsQueue.add(op)
    }
  }

  val workerThread = new Thread(worker)
  workerThread.start()

  override def dispatchOp(op: (Op, OutputChannel[Any])) {
    worker.addOp(op)
  }

  override def shutdown(): Unit = {
//    null op is POISON PILL
    worker.addOp((null, null))
  }
}

class Persister(wrk: PersisterWorker) extends Actor {
  start()

  override def act() {
    loop {
      react {
        case op: Op =>
          wrk.dispatchOp((op, sender))
        case Stop =>
          exit()
          wrk.shutdown()
      }
    }
  }
}

object Persister {
  trait Op
  sealed case class Load(quantity: Int) extends Op
  sealed case class StoreOne(msg: Envelope, fail: Boolean, move: Boolean) extends Op
  sealed case class StoreList(msgList: Traversable[Envelope], fail: Boolean, move: Boolean) extends Op
  sealed case class Remove(id: Traversable[String]) extends Op
  sealed case class Stop()
}
