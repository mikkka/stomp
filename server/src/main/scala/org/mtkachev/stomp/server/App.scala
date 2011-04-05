package org.mtkachev.stomp.server

import codec.{StompCodec, MessageHandler}
import org.apache.mina.transport.socket.nio.{NioProcessor, NioSocketAcceptor}
import java.net.InetSocketAddress
import net.lag.naggati.IoHandlerActorAdapter
import org.apache.mina.filter.codec.ProtocolCodecFilter
import java.util.concurrent.Executors

object App extends Application {

  val listenAddress = "0.0.0.0"
  val listenPort = 23456

  def setMaxThreads() {
    val maxThreads = (Runtime.getRuntime.availableProcessors * 2)
    System.setProperty("actors.maxPoolSize", maxThreads.toString)
  }

  def initializeAcceptor() {
    var acceptorExecutor = Executors.newCachedThreadPool()
    var acceptor =
      new NioSocketAcceptor(acceptorExecutor, new NioProcessor(acceptorExecutor))
    acceptor.setBacklog(1000)
    acceptor.setReuseAddress(true)
    acceptor.getSessionConfig.setTcpNoDelay(true)
    acceptor.getFilterChain.addLast("codec",
            new ProtocolCodecFilter(StompCodec.encoder, StompCodec.decoder))
    acceptor.setHandler(
            new IoHandlerActorAdapter(session =>
              new MessageHandler(session, queueManager, subscriberManager)))
    acceptor.bind(new InetSocketAddress(listenAddress, listenPort))
  }

  setMaxThreads()

  val queueManager = new DestinationManager()
  val subscriberManager = new SubscriberManager()

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run() {
      queueManager ! DestinationManager.Stop()
      subscriberManager ! SubscriberManager.Stop()
    }
  })

  initializeAcceptor()
  println("stomp serv: up and listening on " + listenAddress + ":" + listenPort)
}