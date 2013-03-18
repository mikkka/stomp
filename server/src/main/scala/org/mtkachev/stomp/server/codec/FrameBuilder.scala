package org.mtkachev.stomp.server.codec

/**
 * User: mick
 * Date: 02.09.2010
 * Time: 18:45:48
 */

object FrameBuilder {
  def composeFrame(messageType: String, headers: Map[String, String], body: Array[Byte]) : Frame = {
    val receipt = headers.get("receipt")
    messageType match {
      case "CONNECT" => {
        (headers.get("login"), headers.get("passcode")) match {
          case (Some(login), Some(passcode)) =>
            new Connect(login, passcode)
          case _ =>
            createErrorFrame(messageType, headers, body)
        }
      }
      case "SEND" => {
        (headers.get("destination"), headers.get("content-length"), headers.get("transaction")) match {
          case (Some(destination), Some(contentLength), transactionIdOpt) =>
            new Send(destination, contentLength.toInt, transactionIdOpt, receipt, body)
          case _ =>
            createErrorFrame(messageType, headers, body)
        }
      }
      case "SUBSCRIBE" => {
        (headers.get("destination"), headers.get("ack"), headers.get("id")) match {
          case (Some(destination), Some("client"), id) =>
            return new Subscribe(id, destination, true, receipt)
          case (Some(destination), _, id) =>
            return new Subscribe(id, destination, false, receipt)
          case _ =>
            createErrorFrame(messageType, headers, body)
        }
      }
      case "UNSUBSCRIBE" => {
        (headers.get("id"), headers.get("destination")) match {
          case (id, destination) if id != None || destination != None =>
            return new UnSubscribe(id, destination, receipt)
          case _ =>
            createErrorFrame(messageType, headers, body)
        }
      }
      case "BEGIN" => {
        (headers.get("transaction")) match {
          case (Some(transactionId)) =>
            return new Begin(transactionId, receipt)
          case _ =>
            createErrorFrame(messageType, headers, body)
        }
      }
      case "COMMIT" => {
        (headers.get("transaction")) match {
          case (Some(transactionId)) =>
            return new Commit(transactionId, receipt)
          case _ =>
            createErrorFrame(messageType, headers, body)
        }
      }
      case "ACK" => {
        (headers.get("message-id"), headers.get("transaction")) match {
          case (Some(messageId), transactionId: Option[String]) =>
            return new Ack(messageId, transactionId, receipt)
          case _ =>
            createErrorFrame(messageType, headers, body)
        }
      }
      case "ABORT" => {
        (headers.get("transaction")) match {
          case (Some(transactionId)) =>
            return new Abort(transactionId, receipt)
          case _ =>
            createErrorFrame(messageType, headers, body)
        }
      }
      case "DISCONNECT" => {
        return new Disconnect(receipt)
      }
      case _ => {
        createErrorFrame(messageType, headers, body)
      }
    }
  }

  def createErrorFrame(messageType: String, headers: Map[String, String], body: Array[Byte]) =
    new ErrorIn(messageType, body, headers)
}