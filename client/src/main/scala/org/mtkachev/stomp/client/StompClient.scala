package org.mtkachev.stomp.client

import org.mtkachev.stomp.codec._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.ChannelOption
import io.netty.bootstrap.Bootstrap
import io.netty.channel.socket.nio.NioSocketChannel

/**
 * User: mick
 * Date: 25.08.14
 * Time: 19:30
 */
class StompClient(
    host: String, port: Int,
    onDisconnect: () => Unit,
    onMessage: OutFrame => Unit) {

  val workerGroup = new NioEventLoopGroup()
  val bootstrap = new Bootstrap()
  bootstrap.group(workerGroup).
  channel(classOf[NioSocketChannel]).
  handler(new ClientChannelInitializer(onDisconnect, onMessage)).
  option(ChannelOption.TCP_NODELAY.asInstanceOf[ChannelOption[Any]], true).
  option(ChannelOption.SO_KEEPALIVE.asInstanceOf[ChannelOption[Any]], true)

  val channel = bootstrap.connect(host, port).sync().channel()
  channel.write(Connect("", ""))

  def send(msg: InFrame) {
    channel.write(msg)
    channel.flush()
  }

  def disconnect() {
    channel.write(Disconnect).sync()
    channel.flush().sync()
    channel.close().sync()
    bootstrap.shutdown()
    workerGroup.shutdown()
  }
}
