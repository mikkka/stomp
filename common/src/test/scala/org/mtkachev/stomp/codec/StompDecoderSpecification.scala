package org.mtkachev.stomp.codec

import io.netty.channel.embedded.EmbeddedByteChannel

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.util.CharsetUtil
import collection.mutable.ListBuffer
import scala.Some

import org.specs2.mutable._
import org.specs2.mock.Mockito
import org.specs2.execute.{AsResult, StandardResults}
import org.specs2.execute.Result._
import org.specs2.specification.Scope


/**
 * User: mick
 * Date: 16.08.2010
 * Time: 20:03:09
 */

class StompDecoderSpecification extends Specification {
  val TEST_CONNECT =
"""CONNECT
login: foo
passcode:bar

""" + '\0'

  val TEST_SEND_WO_LENGTH =
"""SEND
destination: foo/bar/baz
transaction: 123

foo bar baz ger ger!!!""" + "\n\n" + '\0'

  val TEST_SEND_WITH_LENGTH =
"""SEND
destination: foo/bar/baz
content-length: 22

foo bar baz ger ger!!!""" + '\0'

  val TEST_SUBSCRIBE_SIMPLE =
"""SUBSCRIBE
destination: foo/bar/baz

""" + '\0'

  val TEST_SUBSCRIBE_ACK_CLIENT =
"""SUBSCRIBE
destination: foo/bar/baz
ack: client

""" + '\0'

  val TEST_SUBSCRIBE_WITH_ID =
"""SUBSCRIBE
destination: foo/bar/baz
ack: whooi
id: ger

""" + '\0'

  val TEST_UNSUBSCRIBE_WITH_DESTINATION =
"""UNSUBSCRIBE
destination: foo/bar/baz

""" + '\0'

  val TEST_UNSUBSCRIBE_WITH_ID =
"""UNSUBSCRIBE
id: ger

""" + '\0'

  val TEST_BEGIN =
"""BEGIN
transaction: gerTx

""" + '\0'

  val TEST_COMMIT =
"""COMMIT
transaction: gerTx

""" + '\0'

  val TEST_ACK =
"""ACK
message-id: fooBarId
transaction: gerTx

""" + '\0'

  val TEST_ACK_NO_TX =
"""ACK
message-id: fooBarId

""" + '\0'

  val TEST_ABORT =
"""ABORT
transaction: gerTx

""" + '\0'

  val TEST_DISCONNECT =
"""DISCONNECT

""" + '\0'

  def byteBuff(str: String) = Unpooled.copiedBuffer(str, CharsetUtil.ISO_8859_1)

  "StompCodec decoder " should {
    "parse CONNECT command" in new mock {
      embedder.offer(byteBuff(TEST_CONNECT))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case conn : Connect =>
          conn.login mustEqual "foo"
          conn.password mustEqual "bar"
        case _ =>
          failure("expected CONNECT")
      }
    }

    "parse SEND command with content length" in new mock {
      embedder.offer(byteBuff(TEST_SEND_WITH_LENGTH))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case send : Send =>
          send.destination mustEqual "foo/bar/baz"
          send.contentLength mustEqual 22
          new String(send.body) mustEqual "foo bar baz ger ger!!!"
          send.transactionId mustEqual None
        case _ => failure("expected SEND")
      }
    }

    "parse SEND command without content length" in new mock {
      embedder.offer(byteBuff(TEST_SEND_WO_LENGTH))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case send : Send =>
          send.destination mustEqual "foo/bar/baz"
          send.contentLength mustEqual 24
          new String(send.body) mustEqual "foo bar baz ger ger!!!\n\n"
          send.transactionId mustEqual Option("123")
        case _ => failure("expected SEND")
      }
    }

    "parse SUBSCRIBE simple" in new mock {
      embedder.offer(byteBuff(TEST_SUBSCRIBE_SIMPLE))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case subscribe : Subscribe =>
          subscribe.expression mustEqual "foo/bar/baz"
          subscribe.ackMode mustEqual false
          subscribe.id mustEqual None
        case _ => failure("expected SUBSCRIBE")
      }
    }

    "parse SUBSCRIBE with client ack" in new mock {
      embedder.offer(byteBuff(TEST_SUBSCRIBE_ACK_CLIENT))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case subscribe : Subscribe =>
          subscribe.expression mustEqual "foo/bar/baz"
          subscribe.ackMode mustEqual true
          subscribe.id mustEqual None
        case _ => failure("expected SUBSCRIBE")
      }
    }

    "parse SUBSCRIBE with id" in new mock {
      embedder.offer(byteBuff(TEST_SUBSCRIBE_WITH_ID))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case subscribe : Subscribe =>
          subscribe.expression mustEqual "foo/bar/baz"
          subscribe.ackMode mustEqual false
          subscribe.id mustEqual Some("ger")
        case _ => failure("expected SUBSCRIBE")
      }
    }

    "parse UNSUBSCRIBE with id" in new mock {
      embedder.offer(byteBuff(TEST_UNSUBSCRIBE_WITH_ID))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case unsubscribe : UnSubscribe =>
          unsubscribe.expression mustEqual None
          unsubscribe.id mustEqual Some("ger")
        case _ => failure("expected UNSUBSCRIBE")
      }
    }

    "parse UNSUBSCRIBE with destination" in new mock {
      embedder.offer(byteBuff(TEST_UNSUBSCRIBE_WITH_DESTINATION))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case unsubscribe : UnSubscribe =>
          unsubscribe.expression mustEqual Some("foo/bar/baz")
          unsubscribe.id mustEqual None
        case _ => failure("expected UNSUBSCRIBE")
      }
    }

    "parse BEGIN" in new mock {
      embedder.offer(byteBuff(TEST_BEGIN))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case begin : Begin => begin.transactionId mustEqual "gerTx"
        case _ => failure("expected BEGIN")
      }
    }

    "parse COMMIT" in new mock {
      embedder.offer(byteBuff(TEST_COMMIT))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case commit : Commit => commit.transactionId mustEqual "gerTx"
        case _ => failure("expected COMMIT")
      }
    }

    "parse ACK with tx id" in new mock {
      embedder.offer(byteBuff(TEST_ACK))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case ack : Ack =>
          ack.transactionId mustEqual Some("gerTx")
          ack.messageId mustEqual "fooBarId"
        case _ => failure("expected ACK")
      }
    }

    "parse ACK without tx id" in new mock {
      embedder.offer(byteBuff(TEST_ACK_NO_TX))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case ack : Ack =>
          ack.transactionId mustEqual None
          ack.messageId mustEqual "fooBarId"
        case _ => failure("expected ACK")
      }
    }

    "parse ABORT" in new mock {
      embedder.offer(byteBuff(TEST_ABORT))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case abort : Abort => abort.transactionId mustEqual "gerTx"
        case _ => failure("expected ABORT")
      }
    }

    "parse DISCONNECT" in new mock {
      embedder.offer(byteBuff(TEST_DISCONNECT))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case disconnect : Disconnect =>
        case _ => failure("expected DISCONNECT")
      }
    }

    "parse CONNECT SUBSCRIBE SEND DISCONNECT" in new mock {
      embedder.offer(byteBuff(TEST_CONNECT + TEST_SUBSCRIBE_SIMPLE + TEST_SEND_WO_LENGTH +
              "\n\n" + TEST_DISCONNECT))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 4

      (msgs(0), msgs(1), msgs(2), msgs(3)) match {
        case (connect: Connect, subscribe: Subscribe, send: Send, disconnect: Disconnect) =>
          connect.login mustEqual "foo"
          connect.password mustEqual "bar"

          subscribe.expression mustEqual "foo/bar/baz"
          subscribe.ackMode mustEqual false
          subscribe.id mustEqual None

          send.destination mustEqual "foo/bar/baz"
          send.transactionId mustEqual Some("123")

        case _ => failure("expected CONNECT SUBSCRIBE SEND DISCONNECT")
      }
    }
  }

  trait mock extends Scope {
    val embedder = new Embedder(new StompDecoder)

    class Embedder(val decoder: StompDecoder) {
      val channel = new EmbeddedByteChannel(decoder)

      def offer(buf: ByteBuf) {
        channel.writeInbound(buf)
      }

      def pollAll() = {
        val retval = ListBuffer.empty[Any]
        var msg: Any = null
        do {
          msg = channel.readInbound()
          if(msg != null) {
            retval += msg
          }
        } while (msg != null)
        retval
      }
    }
  }
}