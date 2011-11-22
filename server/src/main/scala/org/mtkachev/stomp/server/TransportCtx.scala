package org.mtkachev.stomp.server

import codec.Frame
import org.jboss.netty.channel.ChannelHandlerContext

trait TransportCtx {
  def write(msg: Frame)
  def isClosing: Boolean
  def close()
  def setSubscriber(subscriber: Subscriber)
}

class NettyTransportCtx(val ctx: ChannelHandlerContext) extends TransportCtx {
  @volatile private var closing = false
  def write(msg: Frame) {
    ctx.getChannel.write(msg)
  }

  def isClosing = closing

  def close() {
    closing = true
    ctx.getChannel.close()
  }

  def setSubscriber(subscriber: Subscriber) {
    ctx.setAttachment(subscriber)
  }
}

object NettyTransportCtx {
  def setSubscriber(ctx: ChannelHandlerContext, subscriber: Subscriber) {
    ctx.setAttachment(subscriber)
  }

  def getSubscriber(ctx: ChannelHandlerContext): Subscriber =
    ctx.getAttachment.asInstanceOf[Subscriber]

  def apply(ctx: ChannelHandlerContext) = new NettyTransportCtx(ctx)
}