package org.mtkachev.stomp.server

import codec.{MainEventHandler, StompDecoder, StompEncoder}
import java.util.concurrent.Executors

import java.net.InetSocketAddress

import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel._


object Launcher extends App {
  val charset = "ISO-8859-1"

  val listenAddress = "0.0.0.0"
  val listenPort = 23456

  val destinationManager = new DestinationManager
  val subscriberManager = new SubscriberManager

  destinationManager.start()
  subscriberManager.start()

  val factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool())
  val bootstrap = new ServerBootstrap(factory)

  bootstrap.setPipelineFactory(
    new ChannelPipelineFactory() {
      override def getPipeline = Channels.pipeline(
        new StompDecoder,
        new MainEventHandler(subscriberManager, destinationManager),
        new StompEncoder)
    }
  )

  bootstrap.setOption("child.tcpNoDelay", true);
  bootstrap.setOption("child.keepAlive", true);
  bootstrap.bind(new InetSocketAddress(listenPort));

  println("stomp serv: up and listening on " + listenAddress + ":" + listenPort)
}