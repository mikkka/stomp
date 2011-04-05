package org.mtkachev.stomp.server

/**
 * User: mick
 * Date: 20.01.11
 * Time: 0:29
 *
 * подписка
 */
case class Subscription(expression: String, subscriber: Subscriber, acknowledge: Boolean, id: Option[String]) {
  def matches(queue: Destination): Boolean = queue.name == expression

  def message(contentLength: Int, body: Array[Byte]) {
    subscriber ! Subscriber.Recieve(this, contentLength, body)
  }

  def destination() : String = id match {
    case Some(dest) => dest
    case None => expression
  }
}