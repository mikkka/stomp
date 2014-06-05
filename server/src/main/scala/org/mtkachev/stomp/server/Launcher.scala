package org.mtkachev.stomp.server

import codec.{MainEventHandler, StompDecoder, StompEncoder}

import io.netty.bootstrap.ServerBootstrap

import io.netty.channel._
import nio.NioEventLoopGroup
import socket.nio.NioServerSocketChannel
import socket.SocketChannel


object Launcher extends App {
  val listenPort = 23456

  val destinationManager = new DestinationManager(new SimpleDestinationFactory(1024))
  val subscriberManager = new SubscriberManager

  destinationManager.start()
  subscriberManager.start()

  val bootstrap = new ServerBootstrap()
  try {
  bootstrap.
    group(new NioEventLoopGroup(), new NioEventLoopGroup()).
    channel(classOf[NioServerSocketChannel]).
    childHandler(new StompServerInitializer(destinationManager, subscriberManager)).
    childOption(ChannelOption.TCP_NODELAY.asInstanceOf[ChannelOption[Any]], true).
    childOption(ChannelOption.SO_KEEPALIVE.asInstanceOf[ChannelOption[Any]], true)

    val ch = bootstrap.bind(listenPort).sync().channel()
    println("stomp serv: starting on port " + listenPort)
    ch.closeFuture().sync();
  } finally {
    bootstrap.shutdown()
  }

  class StompServerInitializer(destinationManager: DestinationManager,
                               subscriberManager: SubscriberManager) extends ChannelInitializer[SocketChannel] {
    def initChannel(ch: SocketChannel) {
      val p = ch.pipeline()
      p.addLast(new StompDecoder)
      p.addLast(new MainEventHandler(subscriberManager, destinationManager))
      p.addLast(new StompEncoder)
    }
  }
}