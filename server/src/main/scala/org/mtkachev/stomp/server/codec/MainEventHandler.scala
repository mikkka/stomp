package org.mtkachev.stomp.server.codec

import org.jboss.netty.channel._
import org.mtkachev.stomp.server.{DestinationManager, TransportCtx, SubscriberManager, Subscriber}

class MainEventHandler(val subscriberManager: SubscriberManager, val queueManager: DestinationManager) extends SimpleChannelHandler {
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    handle(e.getMessage, ctx)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    e.getCause.printStackTrace();
    e.getChannel.close();
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {disconnect(ctx)}
  override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {disconnect(ctx)}

  def disconnect(ctx: ChannelHandlerContext) {
    handle(Disconnect(Map()), ctx)
  }

  private def handle(msg : AnyRef, ctx: ChannelHandlerContext) {
    TransportCtx.getSubscriber(ctx) match {
      case subscriber: Subscriber => msg match {
        case msg: ConnectedStateFrame => {
          subscriber ! Subscriber.FrameMsg(msg)
        }
        case any => {
          handleErrorMessage(msg)
        }
      }
      case _ => msg match {
        case msg: Connect => {
          subscriberManager ! SubscriberManager.Connect(queueManager, TransportCtx(ctx), msg.login, msg.password)
        }
        case any => {
          handleErrorMessage(msg)
        }
      }
    }
  }

  private def handleErrorMessage(msg: AnyRef) {
    msg match {
      case msg: ErrorIn => {
        println("error parse :" + msg)
      }
      case any => {
        println("error not supported : " + msg)
      }
    }
  }
}