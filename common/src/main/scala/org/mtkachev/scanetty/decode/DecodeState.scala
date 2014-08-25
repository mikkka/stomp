package org.mtkachev.scanetty.decode

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext

case class DecodeState(ctx: ChannelHandlerContext, buffer: ByteBuf)