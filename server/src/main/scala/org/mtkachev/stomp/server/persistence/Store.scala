package org.mtkachev.stomp.server.persistence

import org.mtkachev.stomp.server.Envelope
import scala.annotation.tailrec
import java.io.File
import scala.collection.mutable

/**
 * User: mick
 * Date: 02.07.14
 * Time: 18:11
 */
trait Store {
  def store(msg: Envelope, fail: Boolean, move: Boolean): Unit

  /**
   * store messages list
   * @param msgList messages list
   * @param fail message was added to destination because of fail op (i.e. it was already seen)
   * @param move reader should not load this message in normal mode (it should be moved forward), but should load in init()
   */
  def store(msgList: Traversable[Envelope], fail: Boolean, move: Boolean): Unit

  def remove(id: Traversable[String]): Unit

  def load(quantity: Int): Vector[Envelope]

  def init(): Vector[Envelope]

  def shutdown(): Unit
}

sealed trait Record {
  val id: String
}

case class In(id: String, body: Array[Byte]) extends Record

case class Out(id: String) extends Record

class InMemoryStore extends Store {
  private var store = Vector.empty[Record]

  def view = store

  def load(quant: Int): Vector[Envelope] = {
    @tailrec
    def loadIter(counter: Int, acc: Vector[In]): Vector[In] = {
      if (counter == 0 || store.isEmpty) acc
      else {
        val (part, newStore) = store.splitAt(counter)
        store = newStore
        val ins = part.collect { case x: In => x}
        val newAcc = acc ++ ins
        loadIter(counter - ins.size, newAcc)
      }
    }

    loadIter(quant, Vector.empty[In]).map(x => Envelope(x.id, x.body.size, x.body))
  }

  def store(msg: Envelope, fail: Boolean, move: Boolean) {
    if (!move)
      store = store :+ In(msg.id, msg.body)
  }


  def store(msgList: Traversable[Envelope], fail: Boolean, move: Boolean) {
    if (!move)
      store = store ++ msgList.map(msg => In(msg.id, msg.body))
  }

  def remove(id: Traversable[String]) {
    store = store ++ id.map(Out)
  }

  def init(): Vector[Envelope] = Vector.empty

  def shutdown() {}
}

class SimpleJournalFsStore(file: File) extends Store {

  import scala.pickling._
  import binary._
  import java.io._

  val fout = new FileOutputStream(file, true)
  val fin = new FileInputStream(file)

  def write(rec: Record): Int = {
    val flag: Byte = rec match {
      case x: Out => 0
      case x: In => 1
    }
    val serializedArray = rec.pickle.value
    val length = serializedArray.length + 1
    val lengthBytes = java.nio.ByteBuffer.allocate(4).putInt(length).array()
    val bytesToWrite = (lengthBytes :+ flag) ++ serializedArray
    fout.write(bytesToWrite)
    bytesToWrite.length
  }

  def read(): Option[Record] = {
    val lengthAndFlagBytes = new Array[Byte](5)
    val headerBytesRead = fin.read(lengthAndFlagBytes)
    if (headerBytesRead == 5) {
      val length = java.nio.ByteBuffer.allocate(4).put(lengthAndFlagBytes, 0, 4).getInt
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

  override def store(msg: Envelope, fail: Boolean, move: Boolean) {
    val bytesWrote = write(In(msg.id, msg.body))
    if (move) {
      fin.skip(bytesWrote)
    }
  }

  override def store(msgList: Traversable[Envelope], fail: Boolean, move: Boolean) {
    for (msg <- msgList) store(msg, fail, move)
  }

  override def remove(idList: Traversable[String]) {
    for (id <- idList) write(Out(id))
  }

  override def load(quantity: Int): Vector[Envelope] = {
    @tailrec
    def loadIter(counter: Int, acc: Vector[In]): Vector[In] =
      if (counter == 0) acc
      else {
        read() match {
          case Some(x: In) => loadIter(counter - 1, acc :+ x)
          case Some(x: Out) => loadIter(counter, acc)
          case None => acc
        }
      }

    loadIter(quantity, Vector.empty[In]).map(x => Envelope(x.id, x.body.size, x.body))
  }

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
