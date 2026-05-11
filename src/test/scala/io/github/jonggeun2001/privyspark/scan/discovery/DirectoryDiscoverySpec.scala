package io.github.jonggeun2001.privyspark.scan.discovery

import io.github.jonggeun2001.privyspark.PrivySparkSpecFixtures
import io.github.jonggeun2001.privyspark.config.IgnoreMatcher
import io.github.jonggeun2001.privyspark.util.RpcGate
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

@RunWith(classOf[JUnitRunner])
class DirectoryDiscoverySpec extends AnyFunSuite with PrivySparkSpecFixtures {
  test("discover returns files in stable order and prunes ignored directories") {
    val inputDir = Files.createTempDirectory("privyspark-discovery-")
    val dataDir = Files.createDirectories(inputDir.resolve("data"))
    val backupDir = Files.createDirectories(inputDir.resolve("backup"))

    try {
      writeText(dataDir.resolve("b.csv"), "name,email\nbob,bob@example.com\n")
      writeText(dataDir.resolve("a.csv"), "name,email\nalice,alice@example.com\n")
      writeText(inputDir.resolve("_SUCCESS"), "done\n")
      writeText(backupDir.resolve("old.csv"), "name,email\nold,old@example.com\n")

      val rootPath = new Path(inputDir.toString)
      val fs = rootPath.getFileSystem(new Configuration())
      val (files, ignored) = DirectoryDiscovery.discover(
        fs,
        rootPath,
        inputDir.toString,
        IgnoreMatcher.fromSources(Seq("_SUCCESS", "backup"), None),
        parallelism = 2
      )

      assert(files.map(file => new Path(file.path).getName) == Seq("a.csv", "b.csv"))
      assert(files.map(_.size) == Seq(
        "name,email\nalice,alice@example.com\n".getBytes(java.nio.charset.StandardCharsets.UTF_8).length.toLong,
        "name,email\nbob,bob@example.com\n".getBytes(java.nio.charset.StandardCharsets.UTF_8).length.toLong
      ))
      assert(ignored.map { case (path, _) => new Path(path).getName }.sorted == Seq("_SUCCESS", "backup"))
      assert(ignored.forall(_._2.nonEmpty))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("resolvePreScanProgressInterval caps progress logging interval") {
    assert(DirectoryDiscovery.resolvePreScanProgressInterval(0) == 1)
    assert(DirectoryDiscovery.resolvePreScanProgressInterval(20) == 20)
    assert(DirectoryDiscovery.resolvePreScanProgressInterval(20000) == 10000)
  }

  test("discover gates parallel listStatus calls") {
    val rootPath = new Path("/input")
    val fs = rootPath.getFileSystem(new Configuration())
    val childDirectories = (1 to 8).map(index => new Path(rootPath, s"d$index"))
    val activeListCalls = new AtomicInteger(0)
    val peakListCalls = new AtomicInteger(0)

    val (files, ignored) = DirectoryDiscovery.discover(
      fs,
      rootPath,
      rootPath.toString,
      IgnoreMatcher.empty,
      parallelism = 8,
      rpcGate = Some(new RpcGate(2)),
      listStatusOverride = Some { directory =>
        if (directory.toString == rootPath.toString) {
          childDirectories.map(path => new FileStatus(0L, true, 1, 0L, 0L, path)).toArray
        } else {
          val active = activeListCalls.incrementAndGet()
          updatePeak(peakListCalls, active)
          try {
            Thread.sleep(20L)
            Array.empty[FileStatus]
          } finally {
            activeListCalls.decrementAndGet()
          }
        }
      }
    )

    assert(files.isEmpty)
    assert(ignored.isEmpty)
    assert(peakListCalls.get() <= 2)
  }

  private def updatePeak(peak: AtomicInteger, candidate: Int): Unit = {
    var updated = false
    while (!updated) {
      val current = peak.get()
      updated = candidate <= current || peak.compareAndSet(current, candidate)
    }
  }
}
