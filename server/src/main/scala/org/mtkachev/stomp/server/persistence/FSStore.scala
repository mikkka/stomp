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
      if (!move) throw new IllegalStateException("can't do paging op!")
      else write(In(msg.id, msg.body.toArray))
    }

    override def store(msgList: Traversable[Envelope], fail: Boolean, move: Boolean) {
      if (!move) throw new IllegalStateException("can't do paging op!")
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

  private class JournalFsStoreWithCheckpoints(workdir: File, journalChunkSize: Int) extends Store {
    val lock = new String("opLock")
    /**
     * <timestamp>.journal
     * <timestamp>.checkpoint
     *
     */
    var journalStore: SimpleJournalFsStore = null
    var journalSize = 0

    override def load(quantity: Int): Vector[Envelope] = throw new IllegalStateException("can't do paging op!")

    override def store(msg: Envelope, fail: Boolean, move: Boolean) {
      checkFiles
      lock synchronized {
        journalStore.store(msg, fail = fail, move = move)
        journalSize = journalSize + 1
      }
    }

    override def remove(id: Traversable[String]) {
      checkFiles
      lock synchronized {
        journalStore.remove(id)
        journalSize = journalSize + 1
      }
    }

    override def store(msgList: Traversable[Envelope], fail: Boolean, move: Boolean) {
      msgList.foreach(store(_, fail, move))
    }

    override def init(): Vector[Envelope] = {
      if (workdir.isFile) {
        throw new IllegalArgumentException("workdir is file! directory needed")
      } else if (workdir.isDirectory) {
        val checkpointFile = lastChechpointFile
        val journalFiles = listJournalFiles

        val messages = scan(checkpointFile, journalFiles)
        writeCheckpoint(messages, journalFiles)
        journalStore = new SimpleJournalFsStore(createJournalFile)
        messages.map(entry => Envelope(entry._1, entry._2.size, entry._2)).toVector
      } else {
        workdir.mkdirs()
        Vector.empty
      }
    }

    private def writeCheckpoint(content: mutable.LinkedHashMap[String, Array[Byte]], journalToDel: Seq[File]) {
      val checkpoint = createCheckpointFile
      val checkpointStore = new SimpleJournalFsStore(checkpoint)
      content.foreach(ent => checkpointStore.write(In(ent._1, ent._2)))
      checkpointStore.shutdown()

      //burn junk
      //burn old checkpoints
      workdir.listFiles().
        filter(f => f.getName.endsWith(".checkpoint") && f.getName != checkpoint.getName).foreach(_.delete())
      //burn journals
      journalToDel.foreach(_.delete())
    }

    private def lastChechpointFile = {
      val checkpoints = workdir.listFiles().filter(_.getName.endsWith(".checkpoint")).sortBy(_.getName)
      if (checkpoints.isEmpty) None
      else Some(checkpoints.last)
    }

    private def listJournalFiles = workdir.listFiles().filter(_.getName.endsWith(".journal")).sortBy(_.getName)

    private def createCheckpointFile = {
      val newFile = new File(workdir, System.currentTimeMillis() + ".checkpoint")
      newFile.createNewFile()
      newFile
    }

    private def createJournalFile = {
      val newFile = new File(workdir, System.currentTimeMillis() + ".journal")
      newFile.createNewFile()
      newFile
    }

    private def checkFiles() {
      lock synchronized {
        if (journalSize >= journalChunkSize) {
          val checkpointFile = lastChechpointFile
          val journalFiles = listJournalFiles
          journalStore = new SimpleJournalFsStore(createJournalFile)
          journalSize = 0

          //TODO: do this in background thread
          writeCheckpoint(scan(checkpointFile, journalFiles), journalFiles)
        }
      }
    }

    private def scan(checkpoint: Option[File], journals: Seq[File]): mutable.LinkedHashMap[String, Array[Byte]] = {
      val checkpointData =
        if (checkpoint.isEmpty) mutable.LinkedHashMap.empty[String, Array[Byte]]
        else {
          val checkpointStore = new SimpleJournalFsStore(checkpoint.get)
          scan(checkpointStore, mutable.LinkedHashMap.empty[String, Array[Byte]])
        }
      journals.foldLeft(checkpointData) { (map, journal) =>
        val store = new SimpleJournalFsStore(journal)
        scan(store, map)
      }
    }

    @tailrec
    private def scan(store: SimpleJournalFsStore,
                     inMap: mutable.LinkedHashMap[String, Array[Byte]]): mutable.LinkedHashMap[String, Array[Byte]] = {
      store.read() match {
        case Some(x: In) =>
          scan(store, inMap += (x.id -> x.body))
        case Some(x: Out) =>
          scan(store, inMap -= x.id)
        case None =>
          inMap
      }
    }


    override def shutdown(): Unit = journalStore.shutdown()
  }


  /**
   * works only for infinite destination (i.e. no paging ever!)
   * @param file
   */
  def simpleJournalFsStore(file: File): Store = new SimpleJournalFsStore(file)

  /**
   * works only for infinite destination (i.e. no paging ever!)
   * @param file
   */
  def journalFsStoreWithCheckpoints(file: File, journalChunkSize: Int): Store =
    new JournalFsStoreWithCheckpoints(file, journalChunkSize)
}