package org.mtkachev.stomp.server.codec

import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.buffer.ChannelBuffers._
import java.nio.charset.Charset


class StompEncoder extends OneToOneEncoder{
  val charset = Charset.forName("ISO-8859-1")

  override def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = msg match {
    case s: String => {
      copiedBuffer(s, charset)
    }
    case s: OutFrame => {
      val sb = StringBuilder.newBuilder
      s match {
        case f: Connected => {
          sb.append("CONNECTED\n").
            append("session: ").append(f.sessionId).append("\n").
            append("\n").append('\0')

          copiedBuffer(sb, charset)
        }
        case f: Message => {
          sb.append("MESSAGE\n").
            append("destination: ").append(f.destination).append("\n").
            append("message-id: ").append(f.messageId).append("\n").
            append("\n")
          copiedBuffer(Array.concat(sb.toString().getBytes, f.body, Array[Byte](0)))
        }
      }
    }
    case other => other
  }
}