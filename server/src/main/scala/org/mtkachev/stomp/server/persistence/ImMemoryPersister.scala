package org.mtkachev.stomp.server.persistence

import org.mtkachev.stomp.server.persistence.Persister._
import org.mtkachev.stomp.server.persistence.ImMemoryPersister._
import org.mtkachev.stomp.server.Destination.Loaded
import org.mtkachev.stomp.server.Envelope

/**
 * User: mick
 * Date: 02.09.13
 * Time: 18:42
 */
class ImMemoryPersister extends Persister {
  private var store = Vector.empty[Event]
  def storeView = store.view

  start()

  override def act() {
    loop {
      react {
        case Load(quantity) => {
          sender ! Loaded(load(quantity).map(x => Envelope(x.id, x.body.size, x.body)))
        }
        case StoreOne(msg) => {
          store = store :+ In(msg.id, msg.body)
        }
        case StoreList(msgs) => {
          store = store ++ msgs.map(msg => In(msg.id, msg.body))
        }
        case Remove(id) => {
          store = store :+ Out(id)
        }
        case Stop => {
          exit()
        }
      }
    }
  }

  def load(quant: Int): Vector[In] = {
    def loadIter(counter: Int, acc: Vector[In]): Vector[In] = {
      if(counter == 0 || store.isEmpty) acc
      else {
        val (part, newStore) = store.splitAt(counter)
        store = newStore
        val ins = part.collect {case x: In => x}
        val newAcc = acc ++ ins
        loadIter(counter - ins.size, newAcc)
      }
    }

    loadIter(quant, Vector.empty[In])
  }
}

object ImMemoryPersister {
  trait Event {
    val id: String
  }
  case class In(id: String, body: Array[Byte]) extends Event
  case class Out(id: String) extends Event
}