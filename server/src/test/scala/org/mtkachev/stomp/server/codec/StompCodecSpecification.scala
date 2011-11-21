package org.mtkachev.stomp.server.codec

import scala.collection.mutable

import org.specs._
import org.specs.Specification
import org.jboss.netty.handler.codec.embedder.DecoderEmbedder

import org.mtkachev.stomp.server.codec._
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.util.CharsetUtil


/**
 * User: mick
 * Date: 16.08.2010
 * Time: 20:03:09
 */

object StompCodecSpecification extends Specification {
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

  val embedder = new DecoderEmbedder[Frame](new StompDecoder)

  def byteBuff(str: String) = ChannelBuffers.copiedBuffer(str, CharsetUtil.ISO_8859_1)

  "StompCodec decoder " should {
/*
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
*/

    "parse CONNECT command" in {
      embedder.offer(byteBuff(TEST_CONNECT))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case conn : Connect => {
          conn.login mustEqual "foo"
          conn.password mustEqual "bar"
        }
        case _ => {
          fail("expected CONNECT")
        }
      }
    }

    "parse SEND command with content length" in {
      embedder.offer(byteBuff(TEST_SEND_WITH_LENGTH))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case send : Send => {
          send.destination mustEqual "foo/bar/baz"
          send.contentLength mustEqual 22
          new String(send.body) mustEqual "foo bar baz ger ger!!!"
          send.transactionId mustEqual None
        }
        case _ => {
          fail("expected SEND")
        }
      }
    }

    "parse SEND command without content length" in {
      embedder.offer(byteBuff(TEST_SEND_WO_LENGTH))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case send : Send => {
          send.destination mustEqual "foo/bar/baz"
          send.contentLength mustEqual 24
          new String(send.body) mustEqual "foo bar baz ger ger!!!\n\n"
          send.transactionId mustEqual Option("123")
        }
        case _ => {
          fail("expected SEND")
        }
      }
    }

    "parse SUBSCRIBE simple" in {
      embedder.offer(byteBuff(TEST_SUBSCRIBE_SIMPLE))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case subscribe : Subscribe => {
          subscribe.expression mustEqual "foo/bar/baz"
          subscribe.ackMode mustEqual false
          subscribe.id mustEqual None
        }
        case _ => {
          fail("expected SUBSCRIBE")
        }
      }
    }

    "parse SUBSCRIBE with client ack" in {
      embedder.offer(byteBuff(TEST_SUBSCRIBE_ACK_CLIENT))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case subscribe : Subscribe => {
          subscribe.expression mustEqual "foo/bar/baz"
          subscribe.ackMode mustEqual true
          subscribe.id mustEqual None
        }
        case _ => {
          fail("expected SUBSCRIBE")
        }
      }
    }

    "parse SUBSCRIBE with id" in {
      embedder.offer(byteBuff(TEST_SUBSCRIBE_WITH_ID))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case subscribe : Subscribe => {
          subscribe.expression mustEqual "foo/bar/baz"
          subscribe.ackMode mustEqual false
          subscribe.id mustEqual Some("ger")
        }
        case _ => {
          fail("expected SUBSCRIBE")
        }
      }
    }

    "parse UNSUBSCRIBE with id" in {
      embedder.offer(byteBuff(TEST_UNSUBSCRIBE_WITH_ID))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case unsubscribe : UnSubscribe => {
          unsubscribe.expression mustEqual None
          unsubscribe.id mustEqual Some("ger")
        }
        case _ => {
          fail("expected UNSUBSCRIBE")
        }
      }
    }

    "parse UNSUBSCRIBE with destination" in {
      embedder.offer(byteBuff(TEST_UNSUBSCRIBE_WITH_DESTINATION))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case unsubscribe : UnSubscribe => {
          unsubscribe.expression mustEqual Some("foo/bar/baz")
          unsubscribe.id mustEqual None
        }
        case _ => {
          fail("expected UNSUBSCRIBE")
        }
      }
    }

    "parse BEGIN" in {
      embedder.offer(byteBuff(TEST_BEGIN))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case begin : Begin => {
          begin.transactionId mustEqual "gerTx"
        }
        case _ => {
          fail("expected BEGIN")
        }
      }
    }

    "parse COMMIT" in {
      embedder.offer(byteBuff(TEST_COMMIT))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case commit : Commit => {
          commit.transactionId mustEqual "gerTx"
        }
        case _ => {
          fail("expected COMMIT")
        }
      }
    }

    "parse ACK with tx id" in {
      embedder.offer(byteBuff(TEST_ACK))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case ack : Ack => {
          ack.transactionId mustEqual Some("gerTx")
          ack.messageId mustEqual "fooBarId"
        }
        case _ => {
          fail("expected COMMIT")
        }
      }
    }

    "parse ACK without tx id" in {
      embedder.offer(byteBuff(TEST_ACK_NO_TX))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case ack : Ack => {
          ack.transactionId mustEqual None
          ack.messageId mustEqual "fooBarId"
        }
        case _ => {
          fail("expected COMMIT")
        }
      }
    }

    "parse ABORT" in {
      embedder.offer(byteBuff(TEST_ABORT))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case abort : Abort => {
          abort.transactionId mustEqual "gerTx"
        }
        case _ => {
          fail("expected COMMIT")
        }
      }
    }

    "parse DISCONNECT" in {
      embedder.offer(byteBuff(TEST_DISCONNECT))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 1
      msgs(0) match {
        case disconnect : Disconnect => {
        }
        case _ => {
          fail("expected COMMIT")
        }
      }
    }

    "parse CONNECT SUBSCRIBE DISCONNECT" in {
      embedder.offer(byteBuff(TEST_CONNECT + TEST_SUBSCRIBE_SIMPLE + TEST_SEND_WO_LENGTH +
              "\n\n" + TEST_DISCONNECT))
      val msgs = embedder.pollAll()
      msgs.size mustEqual 4

      def connectMsg = msgs(0)
      connectMsg match {
        case connect : Connect => {
        }
        case _ => {
          fail("expected Connect")
        }
      }
      
      def subscribeMsg = msgs(1)
      subscribeMsg match {
        case subscribe : Subscribe => {
          subscribe.expression mustEqual "foo/bar/baz"
          subscribe.ackMode mustEqual false
          subscribe.id mustEqual None
        }
        case _ => {
          fail("expected CONNECT")
        }

        def sendMsg = msgs(2)
        sendMsg match {
          case send : Send => {
          }
          case _ => {
            fail("expected SEND")
          }
        }

        def disconnectMsg = msgs(3)
        disconnectMsg match {
          case disconnect : Disconnect => {
          }
          case _ => {
            fail("expected DISCONNECT")
          }
        }
      }
    }
  }
}