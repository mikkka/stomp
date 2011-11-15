package org.mtkachev.scanetty.decode

import org.jboss.netty.handler.codec.replay.{VoidEnum, ReplayingDecoder}
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.buffer.ChannelBuffer

abstract class Decoder extends ReplayingDecoder[VoidEnum] {
  val start: Rule
  private[this] var current = start

  override def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer, state: VoidEnum) = {
    def innerDecode(state: DecodeState, rule: Rule): AnyRef = rule match {
      case st: Stop => st.res
      case r => {
        current = r.apply(state)
        checkpoint()
        innerDecode(state, current)
      }
    }
    //reset state there
    val res = innerDecode(new DecodeState(ctx, buffer), current)
    current = start
    res
  }
}