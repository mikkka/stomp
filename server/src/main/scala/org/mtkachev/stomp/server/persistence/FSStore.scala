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

  private trait Serializer[-T] {
    def serialize(t: T): (Array[Byte], Byte)
  }

  private trait Deserializer[+T] {
    def deserialize(body: Array[Byte], flag: Byte): Option[T]
  }

  private class InOutSerializer extends Serializer[Record] {
    override def serialize(rec: Record): (Array[Byte], Byte) = {
      val flag: Byte = rec match {
        case x: Out => 0
        case x: In => 1
      }
      val serializedArray = rec.pickle.value
      (serializedArray, flag)
    }
  }

  private class InOutDeserializer extends Deserializer[Record] {
    override def deserialize(body: Array[Byte], flag: Byte): Option[Record] = flag match {
      case 0 => Some(body.unpickle[Out])
      case 1 => Some(body.unpickle[In])
      case _ => None
    }
  }

  private trait Reader {
    def fin: FileInputStream

    def read[T]()(implicit s: Deserializer[T]): Option[(T,Int)] = {
      if (fin != null) {
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
          if (bodyBytesRead == length) s.deserialize(serializedArray, flag).map((_, 5 + length))
          else None
        } else None
      } else None
    }

  }

  private trait Writer {
    def fout: FileOutputStream

    def write[T](rec: T)(implicit s: Serializer[T]): Int = {
      if (fout != null) {
        val (serializedArray, flag) = s.serialize(rec)
        val length = serializedArray.length
        val lengthBytes = java.nio.ByteBuffer.allocate(4).putInt(length).array()
        val bytesToWrite = (lengthBytes :+ flag) ++ serializedArray
        fout.write(bytesToWrite)
        bytesToWrite.length
      } else 0
    }
  }

  /**
   * works only for infinite destination (i.e. no paging ever!)
   * @param file
   */
  private class SimpleJournalFsStore(file: File) extends Store with Reader with Writer {
    override val fin = new FileInputStream(file)
    override val fout = new FileOutputStream(file, true)

    implicit val serializer = new InOutSerializer
    implicit val deserializer = new InOutDeserializer

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
          case Some((x: In, _)) =>
            loadIter(inMap += (x.id -> x.body))
          case Some((x: Out, _)) =>
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
    implicit val serializer = new InOutSerializer
    implicit val deserializer = new InOutDeserializer

    class BackgroundCheckpointWorker extends Runnable {
      var submitLock = new String("submitLock")
      var nextTask: (Option[File], Seq[File]) = null
      @volatile var isRunning = false

      def submitNextTask(checkpoint: Option[File], journals: Seq[File]) {
        submitLock synchronized {
          nextTask = (checkpoint, journals)
          submitLock.notifyAll()
        }
      }

      override def run() {
        while (isRunning) {
          var task: (Option[File], Seq[File]) = null
          submitLock synchronized {
            while (task == null && isRunning) {
              task = nextTask
              if (task == null) submitLock.wait()
            }
            nextTask = null
          }
          if (task != null) writeCheckpoint(scan(task._1, task._2), task._2)
        }
      }

      def shutdown() {
        isRunning = false
        submitLock synchronized {
          submitLock.notifyAll()
        }
      }
    }

    val checkpointWorker = new BackgroundCheckpointWorker
    val checkpointWorkerThread = new Thread(checkpointWorker)
    checkpointWorkerThread.start()
    /**
     * <timestamp>.journal
     * <timestamp>.checkpoint
     *
     */
    var journalStore: SimpleJournalFsStore = null
    var journalSize = 0

    override def load(quantity: Int): Vector[Envelope] = throw new IllegalStateException("can't do paging op!")

    override def store(msg: Envelope, fail: Boolean, move: Boolean) {
      if (!move) throw new IllegalStateException("can't do paging op!")
      checkFiles()
      journalStore.store(msg, fail = fail, move = move)
      journalSize = journalSize + 1
    }

    override def store(msgList: Traversable[Envelope], fail: Boolean, move: Boolean) {
      if (!move) throw new IllegalStateException("can't do paging op!")
      msgList.foreach(store(_, fail, move))
    }

    override def remove(id: Traversable[String]) {
      checkFiles()
      journalStore.remove(id)
      journalSize = journalSize + 1
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
      if (journalSize >= journalChunkSize) {
        val checkpointFile = lastChechpointFile
        val journalFiles = listJournalFiles

        journalStore = new SimpleJournalFsStore(createJournalFile)
        journalSize = 0

        checkpointWorker.submitNextTask(checkpointFile, journalFiles)
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
        case Some((x: In, _)) =>
          scan(store, inMap += (x.id -> x.body))
        case Some((x: Out, _)) =>
          scan(store, inMap -= x.id)
        case None =>
          inMap
      }
    }

    override def shutdown() {
      journalStore.shutdown()
      checkpointWorker.shutdown()
    }
  }

  case class LoadBookmark(filename: String, position: Long)

  private class AheadLogFsStore(workdir: File, aheadLogChunkBytesSize: Int) extends Store {
    //new store
    //failed store
    private class ChunkedAheadLogReadWriter(ext: String, startReaderBookmark: Option[LoadBookmark]) extends Writer with Reader {

      implicit val serializer = new InOutSerializer
      implicit val deserializer = new InOutDeserializer

      private var _fout: FileOutputStream = null
      override def fout: FileOutputStream = _fout

      private var _currentFinFile: File = null
      private var _fin: FileInputStream = null
      override def fin: FileInputStream = _fin

      var currentBytesWrite = 0L
      var currentBytesRead = 0L

      private def initWriter() {
        val chunks = chunkFilesList()
        if(!chunks.isEmpty) {
          _fout = new FileOutputStream(chunks.last)
          currentBytesWrite = chunks.last.length()
        }
      }

      private def initReader() {
        startReaderBookmark.foreach{bookmark =>
          if(_currentFinFile.getName == bookmark.filename) {
            _fin.skip(bookmark.position)
          }
        }
      }

      initWriter()
      checkWriteRoll()
      readRoll()
      initReader()

      def writeIn(rec: In) {
        checkWriteRoll()
        currentBytesWrite = currentBytesWrite + write(rec)
      }
      /**
       * read data from chunk files. if chunk is exhausted - delete it!
       *
       * @param quantity - data quantity
       * @return
       *         loaded ins,
       *         bookmark for future navigation if file still exists
       */
      def load(quantity: Int): (Vector[In], Option[LoadBookmark]) = {

        @tailrec
        def readSingle(acc: Vector[In], bytesRead: Int, rest: Int): (Vector[In], Int, Boolean) =
          if(rest == 0) (acc, bytesRead, false)
          else
            read() match {
              case Some((x: In, br)) =>
                readSingle(acc :+ x, bytesRead + br, rest - 1)
              case Some((x: Out, _)) =>
                readSingle(acc, bytesRead, rest)
              case None =>
                (acc, bytesRead, true)
            }

        @tailrec
        def rollingRead(acc: Vector[In]): (Vector[In], Option[LoadBookmark]) = {
          val (ins, bytesRead, fileComplete) = readSingle(acc, 0, quantity - acc.size)
          if (fileComplete) {
            completeFin()
            if (!readRoll()) {
              (ins, None)
            } else {
              rollingRead(ins)
            }
          } else {
            currentBytesRead = currentBytesRead + bytesRead
            (ins,
              Some(
                LoadBookmark(_currentFinFile.getName,
                  currentBytesRead)))
          }
        }
        rollingRead(Vector.empty)
      }

      private def checkWriteRoll() {
        if(fout == null || currentBytesWrite >= aheadLogChunkBytesSize) {
          closeFout()
          val newChunk = createChunkFile()
          _fout = new FileOutputStream(newChunk)
          readRoll()
        }
      }

      // can be rolled only if current fin is closed i.e. _fin == null
      private def readRoll() = {
        if (_fin == null) {
          val chunks = chunkFilesList()
          if (!chunks.isEmpty) {
            _currentFinFile = chunks.head
            _fin = new FileInputStream(_currentFinFile)
          }
          !chunks.isEmpty
        } else false
      }

      def chunkFilesList() = workdir.listFiles().filter(_.getName.endsWith("." + ext)).sortBy(_.getName)

      private def createChunkFile() = {
        val newFile = new File(workdir, System.currentTimeMillis() + "." + ext)
        newFile.createNewFile()
        currentBytesWrite = 0
        newFile
      }

      def closeFout() {
        if(_fout != null) {
          _fout.flush()
          _fout.close()
          _fout = null
        }
      }

      def closeFin() {
        if (_fin != null) {
          _fin.close()
        }
      }

      def completeFin() {
        val chunks = chunkFilesList()
        //check if we attempt to close null fin or fin is used as fout
        if(_fin != null && chunks.size > 1) {
          _fin.close()
          _fin = null
          _currentFinFile.delete()
          _currentFinFile = null
          currentBytesRead = 0

        }
      }

      def shutdown() {
        closeFout()
        closeFin()
      }
    }

    private val loadsSerializer = new Serializer[LoadBookmark] with Deserializer[LoadBookmark] {
      def serialize(x: LoadBookmark): (Array[Byte], Byte) = {
        (x.pickle.value, 0)
      }

      def deserialize(body: Array[Byte], flag: Byte): Option[LoadBookmark] = {
        Some(body.unpickle[LoadBookmark])
      }
    }

    private val NEWBIE = "newbie"
    private val FAILED = "failed"
    private val LOADS_FILE = workdir.getAbsolutePath + "/loads.log"

    private def cleanTrash() {
      workdir.listFiles().filter(_.getName.endsWith(FAILED)).foreach(_.delete())
      new File(LOADS_FILE).delete()
    }

    private def lastLoadBookmark(): Option[LoadBookmark] = {
      implicit val deserializer = loadsSerializer
      val loadsFile = new File(LOADS_FILE)
      if (loadsFile.isFile) {
        val loadsReader = new Reader {
          override val fin: FileInputStream = new FileInputStream(LOADS_FILE)
        }
        def iter(prevRead: Option[LoadBookmark]): Option[LoadBookmark] = {
          loadsReader.read() match {
            case Some((b, pos)) => iter(Some(b))
            case None => prevRead
          }
        }
        iter(None)
      } else None
    }

    private val newbieReadWriter = new ChunkedAheadLogReadWriter(NEWBIE, lastLoadBookmark())

    cleanTrash()
    private val failedReadWriter = new ChunkedAheadLogReadWriter(FAILED, None)
    private val loadsWriter = new Writer {
      override val fout: FileOutputStream = new FileOutputStream(LOADS_FILE)
    }

    override def store(msg: Envelope, fail: Boolean, move: Boolean) {
      if (move) throw new IllegalStateException("should be move op!")

      val in = In(msg.id, msg.body.toArray)
      if (fail) failedReadWriter.writeIn(in)
      else newbieReadWriter.writeIn(in)
    }

    override def store(msgList: Traversable[Envelope], fail: Boolean, move: Boolean) {
      if (move) throw new IllegalStateException("should be move op!")
      msgList.foreach(store(_, fail, move))
    }

    override def remove(id: Traversable[String]): Unit = throw new IllegalStateException("can't do remove op!")

    override def load(quantity: Int): Vector[Envelope] = {
      implicit val serializer = loadsSerializer
      //first load from failed
      //then load from newbie and save newbie bookmark
      val (failed, _) = failedReadWriter.load(quantity)
      val failedSize = failed.size
      if (failedSize < quantity) {
        val (newbie, nBookmarkOpt) = newbieReadWriter.load(quantity - failedSize)
        nBookmarkOpt.foreach(loadsWriter.write(_))
        (failed ++ newbie).map(in => Envelope(in.id, in.body.length, in.body))
      } else failed.map(in => Envelope(in.id, in.body.length, in.body))
    }

    override def init(): Vector[Envelope] = {
      Vector.empty
    }

    override def shutdown() {
      newbieReadWriter.shutdown()
      failedReadWriter.shutdown()
      loadsWriter.fout.flush()
      loadsWriter.fout.close()
    }
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

  def aheadLogFsStore(file: File, aheadLogChunkBytesSize: Int): Store =
    new AheadLogFsStore(file, aheadLogChunkBytesSize)
}