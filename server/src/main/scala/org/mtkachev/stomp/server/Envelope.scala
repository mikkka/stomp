package org.mtkachev.stomp.server

import java.util.UUID

/**
 * User: mick
 * Date: 28.03.13
 * Time: 18:31
 */
case class Envelope(id: String, contentLength: Int, body: Seq[Byte]) {
  override def toString = s"Envelope($id,$contentLength,${new String(body.toArray)}})"
}

object Envelope {
  def apply(contentLength: Int, body: Array[Byte]) = {
    val id = UUID.randomUUID.toString
    new Envelope(id, contentLength, body)
  }
}
