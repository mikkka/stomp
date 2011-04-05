package org.mtkachev.stomp.server.codec

import org.mtkachev.stomp.server.codec._
import org.specs._
import org.apache.mina.filter.codec.ProtocolDecoderOutput
import scala.collection.mutable
import net.lag.naggati.Decoder
import org.apache.mina.core.buffer.IoBuffer
import org.apache.mina.core.filterchain.IoFilter
import org.apache.mina.core.session.{DummySession, IoSession}

/**
 * User: mick
 * Date: 16.08.2010
 * Time: 20:03:09
 */

object StompCodecSpecification extends Specification {
  private var fakeSession: IoSession = null
  private var fakeDecoderOutput: ProtocolDecoderOutput = null
  private var written = new mutable.ListBuffer[Frame]


  def quickDecode(decoder: Decoder, s: String) {
    decoder.decode(fakeSession, IoBuffer.wrap(s.getBytes), fakeDecoderOutput)
  }

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

  "StompCodec decoder " should {
    doBefore {
      written.clear()
      fakeSession = new DummySession
      fakeDecoderOutput = new ProtocolDecoderOutput {
        def flush(nextFilter: IoFilter.NextFilter, s: IoSession) {}
        def write(obj: AnyRef) {
          written += obj.asInstanceOf[Frame]
        }
      }
    }

    "parse CONNECT command" in {
      val decoder = StompCodec.decoder
      quickDecode(decoder, TEST_CONNECT)

      written.size mustEqual 1
      def msg = written(0) 
      msg match {
        case conn : Connect => {
          conn.login mustEqual "foo"
          conn.password mustEqual "bar"
        }
        case other => {
          fail("expected CONNECT")
        }
      }
    }

    "parse SEND command with content length" in {
      val decoder = StompCodec.decoder
      quickDecode(decoder, TEST_SEND_WITH_LENGTH)

      written.size mustEqual 1
      def msg = written(0)
      msg match {
        case send : Send => {
          send.destination mustEqual "foo/bar/baz"
          send.contentLength mustEqual 22
          new String(send.body) mustEqual "foo bar baz ger ger!!!"
          send.transactionId mustEqual None
        }
        case other => {
          fail("expected SEND")
        }
      }
    }

    "parse SEND command without content length" in {
      val decoder = StompCodec.decoder
      quickDecode(decoder, TEST_SEND_WO_LENGTH)

      written.size mustEqual 1
      def msg = written(0)
      msg match {
        case send : Send => {
          send.destination mustEqual "foo/bar/baz"
          send.contentLength mustEqual 24
          new String(send.body) mustEqual "foo bar baz ger ger!!!\n\n"
          send.transactionId mustEqual Option("123")
        }
        case other => {
          fail("expected SEND")
        }
      }
    }

    "parse SUBSCRIBE simple" in {
      val decoder = StompCodec.decoder
      quickDecode(decoder, TEST_SUBSCRIBE_SIMPLE)

      written.size mustEqual 1
      def msg = written(0)
      msg match {
        case subscribe : Subscribe => {
          subscribe.expression mustEqual "foo/bar/baz"
          subscribe.ackMode mustEqual false
          subscribe.id mustEqual None
        }
        case other => {
          fail("expected SUBSCRIBE")
        }
      }
    }

    "parse SUBSCRIBE with client ack" in {
      val decoder = StompCodec.decoder
      quickDecode(decoder, TEST_SUBSCRIBE_ACK_CLIENT)

      written.size mustEqual 1
      def msg = written(0)
      msg match {
        case subscribe : Subscribe => {
          subscribe.expression mustEqual "foo/bar/baz"
          subscribe.ackMode mustEqual true
          subscribe.id mustEqual None
        }
        case other => {
          fail("expected SUBSCRIBE")
        }
      }
    }

    "parse SUBSCRIBE with id" in {
      val decoder = StompCodec.decoder
      quickDecode(decoder, TEST_SUBSCRIBE_WITH_ID)

      written.size mustEqual 1
      def msg = written(0)
      msg match {
        case subscribe : Subscribe => {
          subscribe.expression mustEqual "foo/bar/baz"
          subscribe.ackMode mustEqual false
          subscribe.id mustEqual Some("ger")
        }
        case other => {
          fail("expected SUBSCRIBE")
        }
      }
    }

    "parse UNSUBSCRIBE with id" in {
      val decoder = StompCodec.decoder
      quickDecode(decoder, TEST_UNSUBSCRIBE_WITH_ID)

      written.size mustEqual 1
      def msg = written(0)
      msg match {
        case unsubscribe : UnSubscribe => {
          unsubscribe.expression mustEqual None
          unsubscribe.id mustEqual Some("ger")
        }
        case other => {
          fail("expected UNSUBSCRIBE")
        }
      }
    }

    "parse UNSUBSCRIBE with destination" in {
      val decoder = StompCodec.decoder
      quickDecode(decoder, TEST_UNSUBSCRIBE_WITH_DESTINATION)

      written.size mustEqual 1
      def msg = written(0)
      msg match {
        case unsubscribe : UnSubscribe => {
          unsubscribe.expression mustEqual Some("foo/bar/baz")
          unsubscribe.id mustEqual None
        }
        case other => {
          fail("expected UNSUBSCRIBE")
        }
      }
    }

    "parse BEGIN" in {
      val decoder = StompCodec.decoder
      quickDecode(decoder, TEST_BEGIN)

      written.size mustEqual 1
      def msg = written(0)
      msg match {
        case begin : Begin => {
          begin.transactionId mustEqual "gerTx"
        }
        case other => {
          fail("expected BEGIN")
        }
      }
    }

    "parse COMMIT" in {
      val decoder = StompCodec.decoder
      quickDecode(decoder, TEST_COMMIT)

      written.size mustEqual 1
      def msg = written(0)
      msg match {
        case commit : Commit => {
          commit.transactionId mustEqual "gerTx"
        }
        case other => {
          fail("expected COMMIT")
        }
      }
    }

    "parse ACK with tx id" in {
      val decoder = StompCodec.decoder
      quickDecode(decoder, TEST_ACK)

      written.size mustEqual 1
      def msg = written(0)
      msg match {
        case ack : Ack => {
          ack.transactionId mustEqual Some("gerTx")
          ack.messageId mustEqual "fooBarId"
        }
        case other => {
          fail("expected COMMIT")
        }
      }
    }

    "parse ACK without tx id" in {
      val decoder = StompCodec.decoder
      quickDecode(decoder, TEST_ACK_NO_TX)

      written.size mustEqual 1
      def msg = written(0)
      msg match {
        case ack : Ack => {
          ack.transactionId mustEqual None
          ack.messageId mustEqual "fooBarId"
        }
        case other => {
          fail("expected COMMIT")
        }
      }
    }

    "parse ABORT" in {
      val decoder = StompCodec.decoder
      quickDecode(decoder, TEST_ABORT)

      written.size mustEqual 1
      def msg = written(0)
      msg match {
        case abort : Abort => {
          abort.transactionId mustEqual "gerTx"
        }
        case other => {
          fail("expected COMMIT")
        }
      }
    }

    "parse DISCONNECT" in {
      val decoder = StompCodec.decoder
      quickDecode(decoder, TEST_DISCONNECT)

      written.size mustEqual 1
      def msg = written(0)
      msg match {
        case disconnect : Disconnect => {
        }
        case other => {
          fail("expected COMMIT")
        }
      }
    }

    "parse CONNECT SUBSCRIBE DISCONNECT" in {
      val decoder = StompCodec.decoder
      quickDecode(decoder, TEST_CONNECT + TEST_SUBSCRIBE_SIMPLE + TEST_SEND_WO_LENGTH +
              "\n\n" + TEST_DISCONNECT)

      written.size mustEqual 4

      def connectMsg = written(0)
      connectMsg match {
        case connect : Connect => {
        }
        case other => {
          fail("expected Connect")
        }
      }
      
      def subscribeMsg = written(1)
      subscribeMsg match {
        case subscribe : Subscribe => {
          subscribe.expression mustEqual "foo/bar/baz"
          subscribe.ackMode mustEqual false
          subscribe.id mustEqual None
        }
        case other => {
          fail("expected CONNECT")
        }

        def sendMsg = written(2)
        sendMsg match {
          case send : Send => {
          }
          case other => {
            fail("expected SEND")
          }
        }

        def disconnectMsg = written(3)
        disconnectMsg match {
          case disconnect : Disconnect => {
          }
          case other => {
            fail("expected DISCONNECT")
          }
        }
      }
    }
  }
}