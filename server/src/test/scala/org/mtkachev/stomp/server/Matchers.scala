package org.mtkachev.stomp.server

import org.mtkachev.stomp.server.codec.Message
import org.hamcrest.{Description, BaseMatcher, Matcher}

/**
 * User: mick
 * Date: 30.03.11
 * Time: 19:19
 */

object Matchers {
  def matchMessage(pattern: Message): Matcher[Message] = new BaseMatcher[Message] {
    def matches(o: AnyRef) = {
      val msg = o.asInstanceOf[Message]
      msg.destination == pattern.destination && msg.contentLength == pattern.contentLength && msg.body == pattern.body
    }
    def describeTo(desc: Description) = {desc.appendText("should match " + pattern)}
  }
}