package io.github.jonggeun2001.privyspark.progress

import io.github.jonggeun2001.privyspark.PrivySparkSpecFixtures
import io.github.jonggeun2001.privyspark.model.{ProgressRun, ScanError, ScanResult}

import org.apache.hadoop.conf.Configuration
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.file.Files
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class ProgressBufferSpec extends AnyFunSuite with PrivySparkSpecFixtures {
  test("buffers progress records in memory until the group is flushed") {
    val outputDir = Files.createTempDirectory("privyspark-progress-buffer-")

    try {
      val progressRun = progressRunFor(outputDir.toString)
      Seq(progressRun.resultsPath, progressRun.errorsPath, progressRun.completionsPath).foreach { path =>
        Files.createDirectories(java.nio.file.Paths.get(path))
      }

      val buffer = new ProgressBuffer(new Configuration(), progressRun, "group", "/input/group-a")
      buffer.enqueue(
        Seq(scanResult("file-a.csv")),
        Seq.empty
      )
      buffer.enqueue(
        Seq.empty,
        Seq(ScanError("/input", "2026-05-11T00:00:00Z", "file-b.csv", "boom"))
      )

      assert(countFilesWithExtension(outputDir.resolve("run-1/results"), ".jsonl") == 0L)
      assert(countFilesWithExtension(outputDir.resolve("run-1/errors"), ".jsonl") == 0L)
      assert(countFilesWithExtension(outputDir.resolve("run-1/meta/completions"), ".jsonl") == 0L)

      buffer.flush()

      assert(countFilesWithExtension(outputDir.resolve("run-1/results"), ".jsonl") == 1L)
      assert(countFilesWithExtension(outputDir.resolve("run-1/errors"), ".jsonl") == 1L)
      assert(countFilesWithExtension(outputDir.resolve("run-1/meta/completions"), ".jsonl") == 1L)
      val completionJson = Files.walk(outputDir.resolve("run-1/meta/completions")).iterator().asScala
        .find(path => path.toString.endsWith(".jsonl"))
        .map(path => new String(Files.readAllBytes(path), java.nio.charset.StandardCharsets.UTF_8))
        .getOrElse("")
      assert(completionJson.contains("\"scope\":\"group\""))
      assert(completionJson.contains("\"identifier\":\"/input/group-a\""))
      assert(completionJson.contains("\"result_count\":1"))
      assert(completionJson.contains("\"error_count\":1"))
    } finally {
      deleteRecursively(outputDir)
    }
  }

  private def progressRunFor(outputRoot: String): ProgressRun =
    ProgressRun(
      runId = "run-1",
      rootPath = s"$outputRoot",
      runPath = s"$outputRoot/run-1",
      activeRunPath = s"$outputRoot/active-run.json",
      datasetPath = "/input",
      outputRoot = outputRoot,
      scanTimestamp = "2026-05-11T00:00:00Z",
      resultsPath = s"$outputRoot/run-1/results",
      errorsPath = s"$outputRoot/run-1/errors",
      metaPath = s"$outputRoot/run-1/meta",
      completionsPath = s"$outputRoot/run-1/meta/completions",
      inFlightPath = s"$outputRoot/run-1/in-flight"
    )

  private def scanResult(fileIdentifier: String): ScanResult =
    ScanResult(
      dataset_path = "/input",
      scan_timestamp = "2026-05-11T00:00:00Z",
      file_identifier = fileIdentifier,
      column_name = "email",
      pii_type = "email",
      match_count = 1L,
      sampled_row_count = 1L,
      match_ratio = 1.0,
      non_empty_match_ratio = 1.0,
      confidence = 1.0,
      sample_raw_value = "alice@example.com",
      sample_matched_fragment = "alice@example.com"
    )
}
