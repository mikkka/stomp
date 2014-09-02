package org.mtkachev.stomp.client

import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import org.mtkachev.stomp.codec.{OutFrame, StompEncoder, StompDecoder}

/**
 * User: mick
 * Date: 26.08.14
 * Time: 17:47
 */
class ClientChannelInitializer(onDisconnect: () => Unit, onMessage: OutFrame => Unit)
  extends ChannelInitializer[SocketChannel] {

  def initChannel(ch: SocketChannel) {
    val p = ch.pipeline()
    p.addLast(new StompEncoder)
    p.addLast(new StompDecoder)
    p.addLast(new ClientEventHanlder(onDisconnect, onMessage))
  }
}
