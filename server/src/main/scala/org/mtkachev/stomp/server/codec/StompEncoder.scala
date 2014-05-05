package org.mtkachev.stomp.server.codec

import io.netty.handler.codec.MessageToMessageEncoder
import io.netty.channel.ChannelHandlerContext
import java.nio.charset.Charset
import io.netty.buffer.Unpooled._

class StompEncoder extends MessageToMessageEncoder[AnyRef] {
  val charset = Charset.forName("ISO-8859-1")

  override def encode(ctx: ChannelHandlerContext, msg: AnyRef) = msg match {
    case s: String =>
      copiedBuffer(s, charset)

    case s: OutFrame =>
      val sb = StringBuilder.newBuilder
      s match {
        case f: Connected =>
          sb.append("CONNECTED\n").
            append("session: ").append(f.sessionId).append("\n").
            append("\n").append('\0')

          copiedBuffer(sb, charset)

        case f: Message =>
          sb.append("MESSAGE\n").
            append("destination: ").append(f.destination).append("\n").
            append("message-id: ").append(f.messageId).append("\n").
            append("\n")
          copiedBuffer(Array.concat(sb.toString().getBytes, f.body, Array[Byte](0)))

        case f: Receipt =>
          sb.append("RECEIPT\n").
            append("receipt: ").append(f.receiptId).append("\n").
            append("\n").append('\0')

          copiedBuffer(sb, charset)
      }
    case other => other
  }
}