package org.mtkachev.stomp.server.persistence

import org.mtkachev.stomp.server.Envelope
import scala.actors.Actor

/**
 * User: mick
 * Date: 02.09.13
 * Time: 18:48
 */

trait Persister extends Actor

object Persister {
  case class Load(quantity: Int)
  case class StoreOne(msg: Envelope)
  case class StoreList(msgList: List[Envelope])
  case class Remove(id: String)
  case class Stop()
}
