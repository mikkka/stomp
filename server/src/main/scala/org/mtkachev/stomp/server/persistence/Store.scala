package org.mtkachev.stomp.server.persistence

import org.mtkachev.stomp.server.Envelope
import scala.annotation.tailrec

/**
 * User: mick
 * Date: 02.07.14
 * Time: 18:11
 */
trait Store {
  def store(msg: Envelope, fail: Boolean, move: Boolean): Unit
  def store(msgList: Traversable[Envelope], fail: Boolean, move: Boolean): Unit
  def remove(id: Traversable[String]): Unit
  def load(quantity: Int): Vector[Envelope]

  def init(): Vector[Envelope]
  def shutdown(): Unit
}

object InMemoryStore {
  trait Event {
    val id: String
  }

  case class In(id: String, body: Array[Byte]) extends Event
  case class Out(id: String) extends Event
}

class InMemoryStore extends Store {
  import org.mtkachev.stomp.server.persistence.InMemoryStore._

  private var store = Vector.empty[Event]
  def view = store

  def load(quant: Int): Vector[Envelope] = {
    @tailrec
    def loadIter(counter: Int, acc: Vector[In]): Vector[In] = {
      if (counter == 0 || store.isEmpty) acc
      else {
        val (part, newStore) = store.splitAt(counter)
        store = newStore
        val ins = part.collect { case x: In => x}
        val newAcc = acc ++ ins
        loadIter(counter - ins.size, newAcc)
      }
    }

    loadIter(quant, Vector.empty[In]).map(x => Envelope(x.id, x.body.size, x.body))
  }

  def store(msg: Envelope, fail: Boolean, move: Boolean) {
    if (!move)
      store = store :+ In(msg.id, msg.body)
  }


  def store(msgList: Traversable[Envelope], fail: Boolean, move: Boolean) {
    if (!move)
      store = store ++ msgList.map(msg => In(msg.id, msg.body))
  }

  def remove(id: Traversable[String]) {
    store = store ++ id.map(Out)
  }

  def init(): Vector[Envelope] = Vector.empty

  def shutdown() {}
}