package org.mtkachev.stomp.server

import actors.Actor
import java.util.UUID
import scala.None
import scala.collection.mutable.ListBuffer
import org.mtkachev.stomp.codec._
import org.mtkachev.stomp.codec.Connected
import org.mtkachev.stomp.server.Subscriber.Stop
import org.mtkachev.stomp.codec.Message
import org.mtkachev.stomp.server.Subscriber.Subscribed
import scala.Some
import org.mtkachev.stomp.server.Subscriber.FrameMsg
import org.mtkachev.stomp.codec.Receipt
import org.mtkachev.stomp.server.Subscriber.Receive
import org.mtkachev.stomp.server.Subscriber.OnConnect

/**
 * User: mick
 * Date: 19.10.2010
 * Time: 20:08:57
 */

class Subscriber(val qm: DestinationManager, val transport: TransportCtx,
                 val login: String, val password: String) extends Actor {

  //* awaiting acks
  private var pendingAcks = Map.empty[String, Receive]
  private var transactions = Map.empty[String, Transaction]
  private var subscriptions = List.empty[Subscription]

  def pendingAcksMap = pendingAcks
  def transactionsMap = transactions
  def subscriptionsList = subscriptions


  val sessionId = UUID.randomUUID.toString

  start()

  def act() {
    loop {
      react {

        case msg: FrameMsg =>
          msg.frame.receipt match {
            case Some(receiptId) =>
              receipt(receiptId)
            case _ => ()
          }

          msg.frame match {
            case frame: Disconnect =>
              if(!transport.isClosing) transport.close()
              abortAllTx(ready = false)
              pendingAcks.map(_._2).foreach(fail(_, ready = false))
              subscriptions.foreach(s => qm ! DestinationManager.UnSubscribe(s))
              exit()

            case frame: Subscribe =>
              val subscription = new Subscription(frame.expression, this, frame.ackMode, frame.id)
              qm ! DestinationManager.Subscribe(subscription)
              subscriptions = subscription :: subscriptions

            case frame: UnSubscribe =>
              val s = frame match {
                case UnSubscribe(None, Some(expression), _) =>
                  subscriptions.find(s => s.expression != expression)
                case UnSubscribe(id: Some[String], None, _) =>
                  subscriptions.find(s => s.id != id)
                case any => None
              }
              if(!s.isEmpty) {
                qm ! DestinationManager.UnSubscribe(s.get)
                subscriptions = subscriptions.filterNot(_ == s.get)
              }

            case frame: Send =>
              if(!doWithTx(frame.transactionId, tx => tx.addSend(frame))) {
                send(frame)
              }

            case frame: Ack =>
              if(!doWithTx(frame.transactionId, tx => tx.addAck(frame.messageId))) {
                doAck(frame.messageId)
              }

            case frame: Begin =>
              transactions.get(frame.transactionId) match {
                case None => transactions = transactions + (frame.transactionId -> new Transaction)
                case _ =>
              }
              ()

            case frame: Commit =>
              commitTx(frame.transactionId)

            case frame: Abort =>
              abortTx(frame.transactionId, ready = true)
          }


        case msg: Subscribed =>
          msg.destination ! Destination.Ready(msg.subscription)

        case msg: Receive =>
          receive(msg.subscription, msg.envelope)
          if(!msg.subscription.acknowledge) {
            ack(msg)
          } else {
            addToPendingAcks(msg)
          }
          ()

        case msg: OnConnect =>
          transport.write(new Connected(this.sessionId))
          ()

        case msg: Stop =>
          exit()
          transport.close()
          ()
      }
    }
  }

  def receive(subscription: Subscription, envelope: Envelope): Message = {
    receive(message(subscription, envelope))
  }

  def receive(msg: Message): Message = {
    transport.write(msg)
    msg
  }

  def receipt(receiptId: String) {
    transport.write(Receipt(receiptId))
  }

  def send(frame: Send) {
    qm ! DestinationManager.Dispatch(frame.destination, Envelope(frame.contentLength, frame.body))
  }

  def ack(receive: Receive) {
    receive.destination ! Destination.Ack(receive.subscription, List(receive.envelope.id))
  }

  def fail(receive: Receive, ready: Boolean) {
    receive.destination ! Destination.Fail(receive.subscription, List(Destination.Dispatch(receive.envelope)), ready)
  }

  def ready(receive: Receive) {
    receive.destination ! Destination.Ready(receive.subscription)
  }

  def message(subscription: Subscription, envelope: Envelope) =
    new Message(subscription.destination, envelope.id, envelope.contentLength, envelope.body)

  def addToPendingAcks(msg: Receive) {
    pendingAcks = pendingAcks + (msg.envelope.id -> msg)
  }

  def doAck(messageId: String) {
    //ACKING EVERYTIME!!!
    pendingAcks.get(messageId).foreach(ack)
    pendingAcks = pendingAcks - messageId
  }

  def commitTx(txKey: String) {
    doWithTx(Some(txKey), tx => tx.commit())
    transactions = transactions - txKey
  }

  def abortAllTx(ready: Boolean) {
    transactions.foreach(_._2.abort(ready))
    transactions = Map.empty
  }

  def abortTx(txKey: String, ready: Boolean) {
    doWithTx(Some(txKey), tx => tx.abort(ready))
    transactions = transactions - txKey
  }

  def doWithTx(txKeyOpt: Option[String], f: Transaction => Unit): Boolean = txKeyOpt match {
    case Some(txKey) => transactions.get(txKey) match {
      case Some(tx) => f(tx); true
      case _ => false
    }
    case _ => false
  }

  class Transaction {
    val sends = ListBuffer.empty[Send]
    val acks = ListBuffer.empty[Receive]

    clear()

    def clear() {
      sends.clear()
      acks.clear()
    }

    def commit() {
      sends.foreach(send)
      acks.foreach(ack)
      clear()
    }

    def abort(ready: Boolean) {
      acks.foreach(fail(_, ready))
    }

    def addSend(send: Send) {
      sends.append(send)
    }

    def addAck(messageId: String) {
      pendingAcks.get(messageId).foreach{rcv =>
        acks.append(rcv)
        ready(rcv)
      }
      pendingAcks = pendingAcks - messageId
    }
  }
}

object Subscriber {
  def apply(qm: DestinationManager, transport: TransportCtx, login: String, password: String) =
    new Subscriber(qm, transport, login, password)

  case class FrameMsg(frame: ConnectedStateFrame)

  case class Receive(destination: Destination, subscription: Subscription, envelope: Envelope)
  case class Subscribed(destination: Destination, subscription: Subscription)

  case class Stop()

  case class OnConnect()
}
