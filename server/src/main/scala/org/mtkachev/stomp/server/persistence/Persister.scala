package org.mtkachev.stomp.server.persistence

import org.mtkachev.stomp.server.Envelope
import scala.actors.Actor
import scala.collection.GenTraversableOnce

/**
 * User: mick
 * Date: 02.09.13
 * Time: 18:48
 */

trait Persister extends Actor

object Persister {
  case class Load(quantity: Int)
  case class StoreOne(msg: Envelope, move: Boolean)
  case class StoreList(msgList: Traversable[Envelope], move: Boolean)
  case class Remove(id: Traversable[String])
  case class Stop()
}
