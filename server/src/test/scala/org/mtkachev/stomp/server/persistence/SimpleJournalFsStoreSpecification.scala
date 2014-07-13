package org.mtkachev.stomp.server.persistence

import org.specs2.mutable.Specification
import org.specs2.matcher.ThrownMessages
import java.io.File

/**
 * User: mick
 * Date: 10.07.14
 * Time: 19:07
 */
class SimpleJournalFsStoreSpecification  extends Specification with ThrownMessages {
  "simple fs persister" should {
    "store and retive events online, return left events and become empty after reinit" in  {
      val tmpStore = File.createTempFile("simple_store_spec", ".log")
      val store = new SimpleJournalFsStore(tmpStore)
      val msgs1 = store.init()
      msgs1.size should_== 0

      store.load(22).size should_== 0
    }
  }
}
