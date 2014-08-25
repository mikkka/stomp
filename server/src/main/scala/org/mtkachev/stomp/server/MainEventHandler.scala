package org.mtkachev.stomp.server

import io.netty.channel._
import org.mtkachev.stomp.codec._
import org.mtkachev.stomp.codec.Connect
import org.mtkachev.stomp.codec.Disconnect

class MainEventHandler(val subscriberManager: SubscriberManager, val queueManager: DestinationManager)
  extends ChannelInboundMessageHandlerAdapter[InFrame] {

  override def messageReceived(ctx: ChannelHandlerContext, msg: InFrame) {
    handle(msg, ctx)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable) {
    e.getCause.printStackTrace()
    ctx.channel().close()
  }

  override def channelUnregistered(ctx: ChannelHandlerContext) {disconnect(ctx)}
  override def channelInactive(ctx: ChannelHandlerContext) {disconnect(ctx)}

  def disconnect(ctx: ChannelHandlerContext) {
    handle(Disconnect(None), ctx)
  }

  private def handle(msg : InFrame, ctx: ChannelHandlerContext) {
    NettyTransportCtx.getSubscriber(ctx) match {
      case subscriber: Subscriber => msg match {
        case msg: ConnectedStateFrame =>
          subscriber ! Subscriber.FrameMsg(msg)

        case any => handleErrorMessage(msg)
      }
      case _ => msg match {
        case msg: Connect =>
          subscriberManager ! SubscriberManager.Connect(queueManager, NettyTransportCtx(ctx), msg.login, msg.password)

        case any => handleErrorMessage(msg)
      }
    }
  }

  private def handleErrorMessage(msg: AnyRef) {
    msg match {
      case msg: ErrorIn => println("error parse :" + msg)
      case any => println("error not supported : " + msg)
    }
  }
}