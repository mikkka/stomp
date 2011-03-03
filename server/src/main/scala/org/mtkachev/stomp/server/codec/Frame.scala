package org.mtkachev.stomp.server.codec

/**
 * User: mick
 * Date: 16.08.2010
 * Time: 16:36:14
 */

abstract sealed class Frame(additional: Map[String,String])
abstract sealed class OutFrame(additional: Map[String,String]) extends Frame(additional)

case class Connected(sessionId: String, additional: Map[String,String]) extends OutFrame(additional)

case class Message(
        destination: String,
        messageId: String,
        contentLength: Int,
        body: Array[Byte],
        additional: Map[String,String]
        )
        extends OutFrame(additional)

case class Receipt(receiptId: String, additional: Map[String,String]) extends OutFrame(additional)



abstract sealed class InFrame(additional: Map[String,String]) extends Frame(additional)

case class ErrorIn(messageType: String, body: Array[Byte], headers: Map[String,String]) extends InFrame(headers)

case class Connect(login: String, password: String, additional: Map[String,String]) extends InFrame(additional)

case class ConnectedStateFrame(headers: Map[String,String]) extends InFrame(headers)

case class Send(
        destination: String,
        contentLength: Int,
        transactionId: Option[String],
        body: Array[Byte],
        additional: Map[String,String]
        )
        extends ConnectedStateFrame(additional)

case class Subscribe(id: Option[String], expression: String, ackMode: Boolean, additional: Map[String,String]) extends ConnectedStateFrame(additional)

case class UnSubscribe(id: Option[String], expression: Option[String], additional: Map[String,String]) extends ConnectedStateFrame(additional)

case class Begin(transactionId: String, additional: Map[String,String]) extends ConnectedStateFrame(additional)

case class Commit(transactionId: String, additional: Map[String,String]) extends ConnectedStateFrame(additional)

case class Abort(transactionId: String, additional: Map[String,String]) extends ConnectedStateFrame(additional)

case class Ack(messageId: String, transactionId: Option[String], additional: Map[String,String]) extends ConnectedStateFrame(additional)

case class Disconnect(additional: Map[String,String]) extends ConnectedStateFrame(additional)
