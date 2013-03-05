package org.mtkachev.stomp.server

import codec.Frame
import io.netty.channel.ChannelHandlerContext
import io.netty.util.AttributeKey

trait TransportCtx {
  def write(msg: Frame)
  def isClosing: Boolean
  def close()
  def setSubscriber(subscriber: Subscriber)
}

class NettyTransportCtx(val ctx: ChannelHandlerContext) extends TransportCtx {
  @volatile private var closing = false
  def write(msg: Frame) {
    ctx.channel.write(msg)
  }

  def isClosing = closing

  def close() {
    closing = true
    ctx.channel.close()
  }

  def setSubscriber(subscriber: Subscriber) {
    NettyTransportCtx.setSubscriber(ctx, subscriber)
  }
}

object NettyTransportCtx {
  val subscriberKey = new AttributeKey[Subscriber]("_subsrcriber")

  def setSubscriber(ctx: ChannelHandlerContext, subscriber: Subscriber) {
    ctx.attr(subscriberKey).set(subscriber)
  }

  def getSubscriber(ctx: ChannelHandlerContext): Subscriber =
    ctx.attr(subscriberKey).get()

  def apply(ctx: ChannelHandlerContext) = new NettyTransportCtx(ctx)
}