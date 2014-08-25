package org.mtkachev.stomp.server

import io.netty.bootstrap.ServerBootstrap

import scala.collection.JavaConversions._
import io.netty.channel._
import nio.NioEventLoopGroup
import socket.nio.NioServerSocketChannel
import socket.SocketChannel
import com.typesafe.config.{Config, ConfigObject, ConfigFactory}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.mtkachev.stomp.server.persistence._
import java.io.File
import org.mtkachev.stomp.codec.{StompEncoder, StompDecoder}


object Launcher extends App with StrictLogging {
  val config = ConfigFactory.load

  val listenPort = config.getInt("listenPort")
  val queueDefaultSize = config.getInt("queue.size")
  val preinitedDestConfigs = config.getConfig("queue").root().collect { case (k: String, v: ConfigObject) => k -> v}
  val preinitedDests = preinitedDestConfigs.map { case (name, conf) =>
    name -> {
      val _conf = conf.toConfig

      def getQueueConfOrDef(name: String) = if (_conf.hasPath(name)) _conf else config.getConfig("queue")
      val t = getQueueConfOrDef("type").getString("type")
      val queueSize = getQueueConfOrDef("size").getInt("size")

      if (t == "mem") {
        logger.info(s"init $name in memory destination")
        new Destination(name, queueSize, new Persister(new StorePersisterWorker(new InMemoryStore)))
      } else if (t == "fs") {
        logger.info(s"init $name fs destination")
        val workdir = getQueueConfOrDef("workdir").getString("workdir")
        val queueSize = getQueueConfOrDef("size").getInt("size")
        val journalChunk = getQueueConfOrDef("size").getInt("size")
        val aheadChunk = getQueueConfOrDef("size").getInt("size")

        val store = FSStore.journalAndAheadLogStore(new File(workdir), journalChunk, aheadChunk)
        logger.info(s"reading prev messages...")
        val prevMessages = store.init()
        logger.info(s"done")

        new Destination(name, queueSize,
          new Persister(new BackgroundThreadPersisterWorker(new StorePersisterWorker(store))), prevMessages)
      } else {
        throw new IllegalArgumentException(s"unknown queue type $t")
      }
    }
  }

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
    preinitedDests.getOrElse(name, {
      val queueSize = if (config.hasPath(s"queue.$name.size")) config.getInt(s"queue.$name.size") else queueDefaultSize
      new Destination(name, queueSize, new Persister(new StorePersisterWorker(new InMemoryStore)))
    })
  }
}