package org.mtkachev.stomp.codec

import collection.immutable.HashMap
import org.mtkachev.scanetty.decode.Rules._
import org.mtkachev.scanetty.decode.{Rule, Decoder}

/**
 * User: mick
 * Date: 06.09.2010
 * Time: 18:27:43
 */
class StompDecoder extends Decoder {
  def charset = "ISO-8859-1"

  override def start = readLine(charset) { messageType =>
    startMessageDecode(messageType)
  }

  def startMessageDecode(messageType: String): Rule = {
    if(!messageType.trim.isEmpty) {
      readNextHeader(messageType.trim(), new HashMap[String, String]())
    } else {
      readLine(charset) {messageType =>
        startMessageDecode(messageType)
      }
    }
  }

  def readNextHeader(messageType: String, headers: Map[String, String]): Rule = {
    readLine(charset) {line =>
      if(!line.isEmpty) {
        val parts = line.split("\\:")
        if(parts.length == 2) {
          readNextHeader(messageType, headers + (parts(0).trim() -> parts(1).trim()))
        } else {
          readNextHeader(messageType, headers)
        }
      } else {
        readBody(messageType, headers)
      }
    }
  }

  def readBody(messageType: String, headers: Map[String, String]): Rule = {
    if(headers.contains("content-length")) {
      def bytesToRead = headers("content-length").toInt + 1
      readBytes(bytesToRead) {bytes =>
        stop(FrameBuilder.composeFrame(messageType, headers, cutBody(bytes)))
      }
    } else
      readUntil(0.toByte) {bytes =>
        stop(FrameBuilder.composeFrame(
            messageType, headers + ("content-length" -> bytes.size.toString), bytes))
      }
  }

  private def cutBody(bytes: Array[Byte]) = bytes.slice(0, bytes.length - 1)
}
