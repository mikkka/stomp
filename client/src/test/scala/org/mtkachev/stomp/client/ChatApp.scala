package org.mtkachev.stomp.client

import org.mtkachev.stomp.codec.{OutFrame, Message, Subscribe, Send}

/**
 * User: mick
 * Date: 01.09.14
 * Time: 18:09
 */
object ChatApp extends App {
  val connectR = "^connect ([\\w\\d\\.]+) (\\d+)$".r
  val sendR = "^send ([\\w\\d\\.]+) (.+)$".r
  val subscribeR = "^subs ([\\w\\d\\.]+)$".r

  var input = ""
  var isConnected = args.length == 2

  val onMessage: OutFrame => Unit = {
    case m: Message =>
      val msgId = m.messageId
      val msg = new String(m.body.toArray)
      println(s"\n:[$msgId] $msg")
      print("> ")
    case _ =>
      println(s"\n: sys msg")
      print("> ")
  }

  val onDisconnect = {() => isConnected = false}

  var client: StompClient = if(args.length == 2) new StompClient(args(0), args(1).toInt, onDisconnect, onMessage)
                            else null

  do {
    print("> ")
    input = scala.io.StdIn.readLine().trim
    input match {
      case connectR(host, port) =>
        if (!isConnected) {
          client = new StompClient(host, port.toInt, onDisconnect, onMessage)
          isConnected = true
        } else println("already connected")

      case "disconnect" =>
        if (isConnected) {
          client.disconnect()
        } else println("not connected")

      case sendR(dest, msg) =>
        if (isConnected) {
          val bytes = msg.getBytes()
          client.send(Send(dest, bytes.length, None, None, bytes))
        } else println("not connected")

      case subscribeR(dest) =>
        if (isConnected) {
          client.send(Subscribe(None, dest, ackMode = false, None))
        } else println("not connected")

      case _ =>
    }
  } while (input != "quit")

  if (isConnected) {
    client.disconnect()
  }
}
