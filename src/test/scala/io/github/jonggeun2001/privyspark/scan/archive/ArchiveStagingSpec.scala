package io.github.jonggeun2001.privyspark.scan.archive

import io.github.jonggeun2001.privyspark.PrivySparkSpecFixtures
import io.github.jonggeun2001.privyspark.fsio.FaultInjectingLocalFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.AccessControlException
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.file.Files

@RunWith(classOf[JUnitRunner])
class ArchiveStagingSpec extends AnyFunSuite with PrivySparkSpecFixtures {
  test("safe entries retain nested names and normalize Windows separators") {
    val root = new Path("/staging/archive")
    assert(ArchiveStaging.safeResolveArchiveEntryPath(root, "nested/고객 data.csv") ==
      Some(new Path("/staging/archive/nested/고객 data.csv")))
    assert(ArchiveStaging.safeResolveArchiveEntryPath(root, "nested\\customers.csv") ==
      Some(new Path("/staging/archive/nested/customers.csv")))
  }

  test("empty and traversal entries cannot resolve under the staging root") {
    val root = new Path("/staging/archive")
    Seq(null, "", "/", ".", "..", "../outside.csv", "a/../../outside.csv", "a/./b.csv",
      "a\\..\\outside.csv", "/outside.csv", "/staging/archive-sibling/file.csv").foreach { entry =>
      withClue(s"entry=$entry: ") {
        assert(ArchiveStaging.safeResolveArchiveEntryPath(root, entry).isEmpty)
      }
    }
  }

  test("entry parent creation accepts existing directories and rejects existing files") {
    val directory = Files.createTempDirectory("privyspark-archive-parent-")
    val target = new Path(directory.resolve("nested/entry.csv").toString)
    val fs = target.getFileSystem(new Configuration())
    try {
      assert(ArchiveStaging.ensureArchiveEntryParent(fs, target) == Right(()))
      assert(Files.isDirectory(directory.resolve("nested")))
      assert(ArchiveStaging.ensureArchiveEntryParent(fs, target) == Right(()))
      writeText(directory.resolve("blocked"), "preserve")
      val blocked = ArchiveStaging.ensureArchiveEntryParent(fs, new Path(directory.resolve("blocked/entry.csv").toString))
      assert(blocked.left.get.contains("not a directory"))
      assert(Files.isRegularFile(directory.resolve("blocked")))
    } finally deleteRecursively(directory)
  }

  Seq("false", "deny").foreach { failure =>
    test(s"parent creation $failure does not claim extraction can proceed") {
      val directory = Files.createTempDirectory("privyspark-archive-denied-")
      val target = new Path(directory.resolve("blocked/entry.csv").toString)
      val fs = target.getFileSystem(FaultInjectingLocalFileSystem.configuration("mkdirs", "blocked", failure))
      try {
        if (failure == "deny") intercept[AccessControlException] {
          ArchiveStaging.ensureArchiveEntryParent(fs, target)
        } else {
          assert(ArchiveStaging.ensureArchiveEntryParent(fs, target).left.get.contains("creation failed"))
        }
        assert(!Files.exists(directory.resolve("blocked")))
      } finally {
        fs.close()
        deleteRecursively(directory)
      }
    }
  }
}
