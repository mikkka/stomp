package org.mtkachev.stomp.server.codec

import org.apache.mina.core.session.IoSession
import org.apache.mina.filter.codec.{ProtocolEncoderOutput, ProtocolEncoder}

import org.apache.mina.core.buffer.IoBuffer
import net.lag.naggati._

/**
 * User: mick
 * Date: 11.08.2010
 * Time: 17:20:26
 */

object StompCodec {
  val maxHeadersSize = 20

  val encoder = new ProtocolEncoder {
    def dispose(session: IoSession) {
    }

    def encode(session: IoSession, message: Any, out: ProtocolEncoderOutput) {
      message match {
        case s: String => {
          out.write(IoBuffer.wrap(s.getBytes))
        }
        case s: OutFrame => {
          val sb = new StringBuilder
          s match {
            case f: Connected => {
              sb.append("CONNECTED\n").
                append("session: ").append(f.sessionId).append("\n").
                append("\n").append('\0')

              out.write(IoBuffer.wrap(sb.toString().getBytes))

            }
            case f: Message => {
              sb.append("MESSAGE\n").
                append("destination: ").append(f.destination).append("\n").
                append("message-id: ").append(f.messageId).append("\n").
                append("\n")

              val bytes = Array.concat(sb.toString().getBytes, f.body, Array[Byte](0))
              out.write(IoBuffer.wrap(bytes))
            }
          }
        }
        case any => {
          out.write(IoBuffer.wrap(any.toString.getBytes))
        }
      }
    }
  }

  val decoder = new Decoder(StompDecoder.firstStep)
}