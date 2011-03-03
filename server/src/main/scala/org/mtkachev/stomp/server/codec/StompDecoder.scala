package org.mtkachev.stomp.server.codec

import net.lag.naggati._
import net.lag.naggati.Steps._
import collection.immutable.HashMap

/**
 * User: mick
 * Date: 06.09.2010
 * Time: 18:27:43
 */

object StompDecoder {
  val firstStep = readLine(true, "ISO-8859-1") { messageType =>
    startMessageDecode(messageType)
  }

  def startMessageDecode(messageType: String): Step = {
    if(!messageType.trim.isEmpty) {
      readNextHeader(messageType.trim(), new HashMap[String, String]())
    } else {
      readLine(true, "ISO-8859-1") {messageType =>
        startMessageDecode(messageType)
      }
    }
  }

  def readNextHeader(messageType: String, headers: Map[String, String]): Step = {
    readLine(true, "ISO-8859-1") {line =>
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

  def readBody(messageType: String, headers: Map[String, String]): Step = {
    val startBody = state.buffer.position
    if(headers.contains("content-length")) {
      def bytesToRead = headers("content-length").toInt + 1
      readBytes(bytesToRead) { 
        val byteBuffer = new Array[Byte](bytesToRead)
        state.buffer.get(byteBuffer)
        state.out.write(
          FrameBuilder.composeFrame(messageType, headers, cutBody(byteBuffer)))
        End
      }
    } else
      readDelimiter(0.toByte) {read =>
        val byteBuffer = new Array[Byte](read)
        state.buffer.get(byteBuffer)
        state.out.write(
          FrameBuilder.composeFrame(
            messageType, headers + ("content-length" -> (read - 1).toString), cutBody(byteBuffer)))

        End
      }
  }

  private def cutBody(bytes: Array[Byte]) = bytes.slice(0, bytes.length - 1)
}