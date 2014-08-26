package org.mtkachev.stomp.codec

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
          copiedBuffer(Array.concat(sb.toString().getBytes, f.body.toArray, Array[Byte](0)))

        case f: Receipt =>
          sb.append("RECEIPT\n").
            append("receipt: ").append(f.receiptId).append("\n").
            append("\n").append('\0')

          copiedBuffer(sb, charset)
      }
    case s: InFrame => {
      val sb = StringBuilder.newBuilder
      s match {
        case f: ErrorIn =>
          sb.append("ERROR\n").
            append("message: ").append(f.messageType).append("\n").
            append("\n").append('\0')

          copiedBuffer(sb, charset)

        case f: Connect =>
          sb.append("CONNECTED\n").
            append("login: ").append(f.login).append("\n").
            append("passcode: ").append(f.password).append("\n").
            append("\n").append('\0')
          copiedBuffer(sb, charset)

        case f: Send =>
          sb.append("SEND\n").
            append("destination: ").append(f.destination).append("\n").
            append("content-length: ").append(f.contentLength).append("\n")
          f.transactionId.foreach(transactionId => sb.append("transaction: ").append(transactionId).append("\n"))
          f.receipt.foreach(receipt => sb.append("receipt: ").append(receipt).append("\n"))
          sb.append("\n")
          copiedBuffer(Array.concat(sb.toString().getBytes, f.body.toArray, Array[Byte](0)))

        case f: Subscribe =>
          sb.append("SUBSCRIBE\n").
            append("destination: ").append(f.expression).append("\n")
          f.receipt.foreach(receipt => sb.append("receipt: ").append(receipt).append("\n"))
          f.id.foreach(id => sb.append("id: ").append(id).append("\n"))
          if (f.ackMode) sb.append("ack: ").append("client").append("\n")
          sb.append("\n").append('\0')

        case f: UnSubscribe =>
          sb.append("UNSUBSCRIBE\n").
            append("destination: ").append(f.expression).append("\n")
          f.receipt.foreach(receipt => sb.append("receipt: ").append(receipt).append("\n"))
          f.id.foreach(id => sb.append("id: ").append(id).append("\n"))
          sb.append("\n").append('\0')

        case f: Begin =>
          sb.append("BEGIN\n").
            append("transaction: ").append(f.transactionId).append("\n")
          f.receipt.foreach(receipt => sb.append("receipt: ").append(receipt).append("\n"))
          sb.append("\n").append('\0')

        case f: Commit =>
          sb.append("COMMIT\n").
            append("transaction: ").append(f.transactionId).append("\n")
          f.receipt.foreach(receipt => sb.append("receipt: ").append(receipt).append("\n"))
          sb.append("\n").append('\0')

        case f: Abort =>
          sb.append("ABORT\n").
            append("transaction: ").append(f.transactionId).append("\n")
          f.receipt.foreach(receipt => sb.append("receipt: ").append(receipt).append("\n"))
          sb.append("\n").append('\0')

        case f: Ack =>
          sb.append("ACK\n").
            append("message-id: ").append(f.messageId).append("\n")
          f.transactionId.foreach(transactionId => sb.append("transaction: ").append(transactionId).append("\n"))
          f.receipt.foreach(receipt => sb.append("receipt: ").append(receipt).append("\n"))
          sb.append("\n").append('\0')

        case f: Disconnect =>
          sb.append("DISCONNECT\n").append("\n").append('\0')
      }
    }
  }
}