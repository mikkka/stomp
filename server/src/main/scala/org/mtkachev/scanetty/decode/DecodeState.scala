package org.mtkachev.scanetty.decode

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.ChannelHandlerContext

case class DecodeState(ctx: ChannelHandlerContext, buffer: ChannelBuffer)