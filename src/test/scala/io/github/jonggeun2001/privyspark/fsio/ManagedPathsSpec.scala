package io.github.jonggeun2001.privyspark.fsio

import io.github.jonggeun2001.privyspark.PrivySparkSpecFixtures
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.AccessControlException
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

@RunWith(classOf[JUnitRunner])
class ManagedPathsSpec extends AnyFunSuite with PrivySparkSpecFixtures {
  private def withDirectory(body: Path => Unit): Unit = {
    val root = Files.createTempDirectory("privyspark-managed-paths-")
    try body(root) finally deleteRecursively(root)
  }

  test("rename creates missing parents and preserves file contents") {
    withDirectory { root =>
      val source = root.resolve("source")
      val target = root.resolve("nested/target")
      writeText(source, "original data")
      ManagedPaths.renameManagedPath(new Configuration(), source.toString, target.toString)
      assert(!Files.exists(source))
      assert(new String(Files.readAllBytes(target), StandardCharsets.UTF_8) == "original data")
      assert(ManagedPaths.pathExists(new Configuration(), target.toString))
    }
  }

  test("delete recursively removes existing outputs and accepts missing paths") {
    withDirectory { root =>
      val directory = Files.createDirectories(root.resolve("output/nested"))
      writeText(directory.resolve("part.csv"), "payload")
      val conf = new Configuration()
      ManagedPaths.deleteManagedPath(conf, root.resolve("output").toString)
      ManagedPaths.deleteManagedPath(conf, root.resolve("missing").toString)
      assert(!ManagedPaths.pathExists(conf, root.resolve("output").toString))
    }
  }

  test("cross-filesystem rename is rejected before moving the source") {
    withDirectory { root =>
      val source = root.resolve("source")
      writeText(source, "preserve")
      val conf = new Configuration(false)
      conf.set("fs.otherfile.impl", classOf[OtherLocalFileSystem].getName)
      conf.setBoolean("fs.otherfile.impl.disable.cache", true)
      intercept[IllegalArgumentException] {
        ManagedPaths.renameManagedPath(conf, source.toString, "otherfile:///target")
      }
      assert(Files.exists(source))
    }
  }

  Seq("deny", "false").foreach { failure =>
    test(s"rename $failure keeps the source intact") {
      withDirectory { root =>
        val source = root.resolve("source")
        writeText(source, "preserve")
        val conf = FaultInjectingLocalFileSystem.configuration("rename", "source", failure)
        if (failure == "deny") {
          intercept[AccessControlException] {
            ManagedPaths.renameManagedPath(conf, source.toString, root.resolve("target").toString)
          }
        } else {
          intercept[IllegalStateException] {
            ManagedPaths.renameManagedPath(conf, source.toString, root.resolve("target").toString)
          }
        }
        assert(new String(Files.readAllBytes(source), StandardCharsets.UTF_8) == "preserve")
        assert(!Files.exists(root.resolve("target")))
      }
    }
  }

  test("parent creation failure preserves source and reports failure") {
    withDirectory { root =>
      val source = root.resolve("source")
      writeText(source, "preserve")
      val conf = FaultInjectingLocalFileSystem.configuration("mkdirs", "blocked", "false")
      intercept[IllegalStateException] {
        ManagedPaths.renameManagedPath(conf, source.toString, root.resolve("blocked/target").toString)
      }
      assert(Files.exists(source))
      assert(!Files.exists(root.resolve("blocked")))
    }
  }

  Seq("deny", "false").foreach { failure =>
    test(s"delete $failure retains output while staging cleanup continues to the next path") {
      withDirectory { root =>
        val blocked = root.resolve("blocked")
        val removable = root.resolve("removable")
        writeText(blocked, "preserve")
        writeText(removable, "temporary")
        val conf = FaultInjectingLocalFileSystem.configuration("delete", "blocked", failure)
        if (failure == "deny") intercept[AccessControlException] {
          ManagedPaths.deleteManagedPath(conf, blocked.toString)
        } else intercept[IllegalStateException] {
          ManagedPaths.deleteManagedPath(conf, blocked.toString)
        }
        ManagedPaths.cleanupStagingPaths(conf, Seq(blocked.toString, removable.toString))
        assert(Files.exists(blocked))
        assert(!Files.exists(removable))
      }
    }
  }
}
