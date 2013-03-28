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

  def message(destination: Destination, envelope: Envelope) {
    subscriber ! Subscriber.Receive(destination, this, envelope)
  }

  def subscribed(destination: Destination) {
    subscriber ! Subscriber.Subscribed(destination, this)
  }

  val destination = id match {
    case Some(dest) => dest
    case None => expression
  }
}