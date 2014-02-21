package org.mtkachev.stomp.server

import java.util.UUID

/**
 * User: mick
 * Date: 28.03.13
 * Time: 18:31
 */
case class Envelope(id: String, contentLength: Int, body: Array[Byte]) {
  override def toString = s"Envelope($id,$contentLength,${new String(body)}})"
}

object Envelope {
  def apply(contentLength: Int, body: Array[Byte]) = {
    val id = UUID.randomUUID.toString
    new Envelope(id, contentLength, body)
  }
}
