package org.mtkachev.scanetty.decode

import io.netty.handler.codec.{ReplayingDecoder}
import io.netty.channel.{ChannelHandlerContext}
import io.netty.buffer.ByteBuf

abstract class Decoder extends ReplayingDecoder[Void] {
  private[this] var current = start

  def start: Rule

  override def decode(ctx: ChannelHandlerContext, buffer: ByteBuf) = {
    def innerDecode(state: DecodeState, rule: Rule): AnyRef = rule match {
      case st: Stop => st.res
      case r =>
        current = r.apply(state)
        checkpoint()
        innerDecode(state, current)
    }
    //reset state there
    val res = innerDecode(new DecodeState(ctx, buffer), current)
    current = start
    res
  }
}