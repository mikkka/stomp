package org.mtkachev.stomp.client

import io.netty.channel.{ChannelHandlerContext, ChannelInboundMessageHandlerAdapter}
import org.mtkachev.stomp.codec.OutFrame

/**
 * User: mick
 * Date: 26.08.14
 * Time: 17:50
 */
class ClientEventHanlder(onDisconnect: () => Unit, onMessage: OutFrame => Unit) extends ChannelInboundMessageHandlerAdapter[OutFrame] {
  def messageReceived(ctx: ChannelHandlerContext, msg: OutFrame) {
    onMessage(msg)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable) {
    e.getCause.printStackTrace()
    ctx.channel().close()
  }

  override def channelUnregistered(ctx: ChannelHandlerContext) {
    onDisconnect()
  }

  override def channelInactive(ctx: ChannelHandlerContext) {
    onDisconnect()
  }
}
