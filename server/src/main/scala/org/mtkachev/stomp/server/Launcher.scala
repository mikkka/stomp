package org.mtkachev.stomp.server

import codec.{MainEventHandler, StompDecoder, StompEncoder}

import io.netty.bootstrap.ServerBootstrap

import io.netty.channel._
import nio.NioEventLoopGroup
import socket.nio.NioServerSocketChannel
import socket.SocketChannel
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.mtkachev.stomp.server.persistence.{StorePersisterWorker, InMemoryStore, Persister}


object Launcher extends App with StrictLogging {
  val config = ConfigFactory.load

  val listenPort = config.getInt("listenPort")
  val queueDefaultSize = config.getInt("queue.size")

  val destinationManager = new DestinationManager(destionationFactory)
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
    logger.info("stomp serv: starting on port " + listenPort)
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

  def destionationFactory(name: String): Destination = {
    val queueSize = if (config.hasPath(s"queue.$name.size")) config.getInt(s"queue.$name.size") else queueDefaultSize
    new Destination(name, queueSize, new Persister(new StorePersisterWorker(new InMemoryStore)))
  }
}