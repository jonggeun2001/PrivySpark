package io.github.jonggeun2001.privyspark.scan.discovery

import io.github.jonggeun2001.privyspark.PrivySparkSpecFixtures
import io.github.jonggeun2001.privyspark.config.IgnoreMatcher
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.file.Files

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
}
