package org.mtkachev.stomp.server

import org.specs2.matcher.{Expectable, Matcher}

import org.mtkachev.stomp.server.codec.Message

/**
 * User: mick
 * Date: 30.03.11
 * Time: 19:19
 */

object Matchers {
  def matchMessage(pattern: Message) = new Matcher[Message] {
    def apply[S <: Message](s: Expectable[S]) = {
      result(
        s.value.destination == pattern.destination
          && s.value.contentLength == pattern.contentLength
          && s.value.body == pattern.body,
        pattern + " is matching pattern",
        pattern + " is matching pattern",
        s)
    }
  }
}