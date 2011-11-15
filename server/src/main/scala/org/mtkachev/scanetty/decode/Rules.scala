package org.mtkachev.scanetty.decode

import java.io.OutputStream

abstract class Rule extends (DecodeState => Rule)
class Stop(val res: AnyRef) extends Rule {
  def apply(s: DecodeState) = sys.error("go fuck ya self")
}

object Rules {
  def readBytes(bytes: Int)(func: Array[Byte] => Rule): Rule = {
    new Rule {
      def apply(state: DecodeState) = {
        val tmp = new Array[Byte](bytes)
        state.buffer.readBytes(tmp)
        func.apply(tmp)
      }
    }
  }

  def readBytes(bytes: Int, os: OutputStream)(func: OutputStream => Rule): Rule = {
    new Rule {
      def apply(state: DecodeState) = {
        state.buffer.readBytes(os, bytes)
        func.apply(os)
      }
    }
  }

  def readUntil(delim: Byte)(func: Array[Byte] => Rule): Rule = {
    new Rule {
      def apply(state: DecodeState) = {
        def collect: List[Byte] = state.buffer.readByte() match {
          case b if delim == b => List()
          case b => b :: collect
        }
        func(collect.toArray)
      }
    }
  }

  def readLine(encoding: String)(func: String => Rule): Rule = readUntil('\n'.toByte) {bytes =>
    func(new String(bytes, encoding))
  }

  def stop(res: AnyRef): Rule = {
    new Rule {
      def apply(state: DecodeState) = new Stop(res)
    }
  }
}