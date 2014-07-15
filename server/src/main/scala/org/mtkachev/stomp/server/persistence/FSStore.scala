package org.mtkachev.stomp.server.persistence

import org.mtkachev.stomp.server.Envelope
import scala.annotation.tailrec
import scala.collection.mutable

/**
 * User: mick
 * Date: 15.07.14
 * Time: 18:56
 */
object FSStore {
  import scala.pickling._
  import binary._
  import java.io._

  private trait Reader {
    val fin: FileInputStream

    def read(): Option[Record] = {
      val lengthAndFlagBytes = new Array[Byte](5)
      val headerBytesRead = fin.read(lengthAndFlagBytes)
      if (headerBytesRead == 5) {
        val lengthBuf = java.nio.ByteBuffer.
          allocate(4).
          put(lengthAndFlagBytes, 0, 4)
        lengthBuf.flip()
        val length = lengthBuf.getInt()
        val flag = lengthAndFlagBytes(4)
        val serializedArray = new Array[Byte](length)
        val bodyBytesRead = fin.read(serializedArray)
        if (bodyBytesRead == length) {
          flag match {
            case 0 => Some(serializedArray.unpickle[Out])
            case 1 => Some(serializedArray.unpickle[In])
          }
        } else None
      } else None
    }
  }

  private trait Writer {
    val fout: FileOutputStream

    def write(rec: Record): Int = {
      val flag: Byte = rec match {
        case x: Out => 0
        case x: In => 1
      }
      val serializedArray = rec.pickle.value
      val length = serializedArray.length
      val lengthBytes = java.nio.ByteBuffer.allocate(4).putInt(length).array()
      val bytesToWrite = (lengthBytes :+ flag) ++ serializedArray
      fout.write(bytesToWrite)
      bytesToWrite.length
    }
  }

  /**
   * works only for infinite destination (i.e. no paging ever!)
   * @param file
   */
  private class SimpleJournalFsStore(file: File) extends Store with Reader with Writer {
    override val fin = new FileInputStream(file)
    override val fout = new FileOutputStream(file, true)

    override def store(msg: Envelope, fail: Boolean, move: Boolean) {
      if(!move) throw new IllegalStateException("can't do paging op!")
      else write(In(msg.id, msg.body.toArray))
    }

    override def store(msgList: Traversable[Envelope], fail: Boolean, move: Boolean) {
      if(!move) throw new IllegalStateException("can't do paging op!")
      else for (msg <- msgList) store(msg, fail, move)
    }

    override def remove(idList: Traversable[String]) {
      for (id <- idList) write(Out(id))
    }

    override def load(quantity: Int): Vector[Envelope] = throw new IllegalStateException("can't do paging op!")

    override def init(): Vector[Envelope] = {
      @tailrec
      def loadIter(inMap: mutable.LinkedHashMap[String, Array[Byte]]): mutable.LinkedHashMap[String, Array[Byte]] = {
        read() match {
          case Some(x: In) =>
            loadIter(inMap += (x.id -> x.body))
          case Some(x: Out) =>
            loadIter(inMap -= x.id)
          case None =>
            inMap
        }
      }
      loadIter(mutable.LinkedHashMap.empty[String, Array[Byte]]).
        map(entry => Envelope(entry._1, entry._2.size, entry._2)).toVector
    }

    override def shutdown() {
      fout.flush()
      fout.close()
      fin.close()
    }
  }

  /**
   * works only for infinite destination (i.e. no paging ever!)
   * @param file
   */
  def simpleJournalFsStore(file: File): Store = new SimpleJournalFsStore(file)
}