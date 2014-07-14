package org.mtkachev.stomp.server.codec

/**
 * User: mick
 * Date: 16.08.2010
 * Time: 16:36:14
 */

abstract class Frame()
abstract class OutFrame() extends Frame()

case class Connected(sessionId: String) extends OutFrame()
case class Message(destination: String, messageId: String, contentLength: Int, body: Seq[Byte]) extends OutFrame()
case class Receipt(receiptId: String) extends OutFrame()



abstract sealed class InFrame() extends Frame()

case class ErrorIn(messageType: String, body: Seq[Byte], headers: Map[String,String]) extends InFrame()
case class Connect(login: String, password: String) extends InFrame()

trait ConnectedStateFrame {
  val receipt: Option[String]
}

case class Send(
        destination: String,
        contentLength: Int,
        transactionId: Option[String],
        receipt: Option[String],
        body: Array[Byte]) extends InFrame with ConnectedStateFrame

case class Subscribe(id: Option[String], expression: String, ackMode: Boolean, receipt: Option[String]) extends InFrame with ConnectedStateFrame

case class UnSubscribe(id: Option[String], expression: Option[String], receipt: Option[String]) extends InFrame with ConnectedStateFrame

case class Begin(transactionId: String, receipt: Option[String]) extends InFrame with ConnectedStateFrame

case class Commit(transactionId: String, receipt: Option[String]) extends InFrame with ConnectedStateFrame

case class Abort(transactionId: String, receipt: Option[String]) extends InFrame with ConnectedStateFrame

case class Ack(messageId: String, transactionId: Option[String], receipt: Option[String]) extends InFrame with ConnectedStateFrame

case class Disconnect(receipt: Option[String]) extends InFrame with ConnectedStateFrame
