package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.progress.{InFlightMarker, ProgressRunManager}
import org.apache.hadoop.conf.Configuration
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class ProgressInFlightMarkerSpec extends AnyFunSuite {
  test("prepareProgressRun creates and exposes the in-flight marker directory") {
    val outputDir = withTempDir("privyspark-progress-run")
    val conf = new Configuration()

    try {
      val progressRun = ProgressRunManager.prepareProgressRun(
        conf,
        outputDir.toString,
        "/datasets/input",
        "2026-04-23T12:00:00.000Z"
      )

      val expected = outputDir.resolve(s"_progress/${progressRun.runId}/in-flight")
      assert(progressRun.inFlightPath == expected.toString)
      assert(Files.isDirectory(expected))
    } finally {
      deleteRecursively(outputDir)
    }
  }

  test("InFlightMarker creates a scoped marker while work is running and removes it after success") {
    val markerRoot = withTempDir("privyspark-in-flight-success")
    val inFlightDir = markerRoot.resolve("run-123/in-flight")
    Files.createDirectories(inFlightDir)
    val conf = new Configuration()

    try {
      val result = InFlightMarker.run(
        conf,
        inFlightDir.toString,
        "group",
        "s3://bucket/customer group.parquet",
        Map("format" -> "parquet", "schemaSignature" -> "schema-1")
      ) {
        val markers = jsonFiles(inFlightDir)
        assert(markers.size == 1)
        assert(markers.head.getFileName.toString.startsWith("s3___bucket_customer_group.parquet-"))

        val markerJson = new String(Files.readAllBytes(markers.head), StandardCharsets.UTF_8)
        assert(markerJson.contains(""""runId":"run-123""""))
        assert(markerJson.contains(""""scope":"group""""))
        assert(markerJson.contains(""""identifier":"s3://bucket/customer group.parquet""""))
        assert(markerJson.contains(""""format":"parquet""""))
        assert(markerJson.contains(""""schemaSignature":"schema-1""""))
        assert(markerJson.contains(""""threadName":""""))
        assert(markerJson.trim.matches(""".*"startedAtEpochMs":[0-9]+.*"""))
        "done"
      }

      assert(result == "done")
      assert(jsonFiles(inFlightDir).isEmpty)
    } finally {
      deleteRecursively(markerRoot)
    }
  }

  test("InFlightMarker removes the marker after failure without hiding the original exception") {
    val markerRoot = withTempDir("privyspark-in-flight-failure")
    val inFlightDir = markerRoot.resolve("run-456/in-flight")
    Files.createDirectories(inFlightDir)
    val conf = new Configuration()
    val expected = new IllegalArgumentException("boom")

    try {
      val thrown = intercept[IllegalArgumentException] {
        InFlightMarker.run(conf, inFlightDir.toString, "file", "/input/file.csv") {
          assert(jsonFiles(inFlightDir).size == 1)
          throw expected
        }
      }

      assert(thrown eq expected)
      assert(jsonFiles(inFlightDir).isEmpty)
    } finally {
      deleteRecursively(markerRoot)
    }
  }

  test("InFlightMarker bounds marker filenames while preserving the original identifier in JSON") {
    val markerRoot = withTempDir("privyspark-in-flight-long-id")
    val inFlightDir = markerRoot.resolve("run-789/in-flight")
    Files.createDirectories(inFlightDir)
    val conf = new Configuration()
    val longIdentifier = "/input/" + ("nested-segment-" * 40) + "file.parquet"

    try {
      val result = InFlightMarker.run(conf, inFlightDir.toString, "group", longIdentifier) {
        val markers = jsonFiles(inFlightDir)
        assert(markers.size == 1)
        assert(markers.head.getFileName.toString.length <= 128)

        val markerJson = new String(Files.readAllBytes(markers.head), StandardCharsets.UTF_8)
        assert(markerJson.contains(s""""identifier":"$longIdentifier""""))
        "done"
      }

      assert(result == "done")
      assert(jsonFiles(inFlightDir).isEmpty)
    } finally {
      deleteRecursively(markerRoot)
    }
  }

  private def withTempDir(prefix: String): Path =
    Files.createTempDirectory(prefix)

  private def jsonFiles(dir: Path): Seq[Path] = {
    val stream = Files.list(dir)
    try {
      stream.iterator().asScala.toSeq.filter(_.getFileName.toString.endsWith(".json"))
    } finally {
      stream.close()
    }
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      val stream = Files.walk(path)
      try {
        stream
          .iterator()
          .asScala
          .toSeq
          .reverse
          .foreach(Files.deleteIfExists)
      } finally {
        stream.close()
      }
    }
  }
}
