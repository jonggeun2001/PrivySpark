package io.github.jonggeun2001.privyspark.review.collect

import io.github.jonggeun2001.privyspark.PrivySparkSpecFixtures
import io.github.jonggeun2001.privyspark.fsio.FaultInjectingLocalFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.AccessControlException
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

@RunWith(classOf[JUnitRunner])
class ReviewCollectLockSpec extends AnyFunSuite with PrivySparkSpecFixtures {
  private def withDirectory(body: Path => Unit): Unit = {
    val root = Files.createTempDirectory("privyspark-lock-")
    try body(root) finally deleteRecursively(root)
  }

  test("lock exists during collection and is released after returning the result") {
    withDirectory { root =>
      val result = ReviewCollectLock.withLock(new Configuration(), root.toString) {
        assert(Files.exists(root.resolve(".collect.lock")))
        42
      }
      assert(result == 42)
      assert(!Files.exists(root.resolve(".collect.lock")))
    }
  }

  test("an existing lock prevents entry and is never deleted by the rejected collector") {
    withDirectory { root =>
      val lock = root.resolve(".collect.lock")
      writeText(lock, "owned by another collector")
      var entered = false
      intercept[IllegalStateException] {
        ReviewCollectLock.withLock(new Configuration(), root.toString) { entered = true }
      }
      assert(!entered)
      assert(new String(Files.readAllBytes(lock), StandardCharsets.UTF_8) == "owned by another collector")
    }
  }

  test("collection failure releases the lock and propagates the same error") {
    withDirectory { root =>
      val original = new IOException("collection failed")
      val actual = intercept[IOException] {
        ReviewCollectLock.withLock(new Configuration(), root.toString) { throw original }
      }
      assert(actual eq original)
      assert(!Files.exists(root.resolve(".collect.lock")))
      assert(ReviewCollectLock.withLock(new Configuration(), root.toString) { "retry" } == "retry")
    }
  }

  test("permission denied when creating a lock prevents collection") {
    withDirectory { root =>
      val conf = FaultInjectingLocalFileSystem.configuration("create", ".collect.lock")
      var entered = false
      intercept[AccessControlException] {
        ReviewCollectLock.withLock(conf, root.toString) { entered = true }
      }
      assert(!entered)
      assert(!Files.exists(root.resolve(".collect.lock")))
    }
  }

  test("failed lock metadata write removes the partial lock without entering collection") {
    withDirectory { root =>
      val conf = FaultInjectingLocalFileSystem.configuration("create", ".collect.lock", "write")
      var entered = false
      val error = intercept[IOException] {
        ReviewCollectLock.withLock(conf, root.toString) { entered = true }
      }
      assert(error.getMessage == "injected write failure")
      assert(!entered)
      assert(!Files.exists(root.resolve(".collect.lock")))
    }
  }

  test("permission denied during release does not replace the collection failure") {
    withDirectory { root =>
      val conf = FaultInjectingLocalFileSystem.configuration("delete", ".collect.lock")
      val original = new IOException("collection failed")
      val actual = intercept[IOException] {
        ReviewCollectLock.withLock(conf, root.toString) { throw original }
      }
      assert(actual eq original)
      assert(Files.exists(root.resolve(".collect.lock")))
    }
  }
}
