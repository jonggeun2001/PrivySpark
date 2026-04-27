package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.model.ScanResult
import org.apache.hadoop.conf.Configuration
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.charset.StandardCharsets
import java.nio.file.Files

@RunWith(classOf[JUnitRunner])
class ReviewHtmlWriterSpec extends AnyFunSuite {
  test("write creates only review.html under the scan output review directory and includes masked samples") {
    val outputRoot = Files.createTempDirectory("privyspark-review-html-")
    val result = ScanResult(
      dataset_path = "/data/project",
      scan_timestamp = "2026-04-27T10:00:00Z",
      file_identifier = "customers/part-000.parquet",
      column_name = "email",
      pii_type = "email",
      match_count = 3L,
      sampled_row_count = 10L,
      match_ratio = 0.3,
      non_empty_match_ratio = 0.3,
      confidence = 0.1,
      sample_raw_value = "owner=alice@example.com",
      sample_matched_fragment = "alice@example.com",
      file_size = 128L,
      file_mtime_epoch_ms = 1710000000000L,
      hive_table_fqn = "mart.customers"
    )

    ReviewHtmlWriter.write(
      new Configuration(),
      outputRoot.toString,
      "/data/project",
      Seq(result),
      sampleMode = "masked"
    )

    val reviewDir = outputRoot.resolve("review")
    val htmlPath = reviewDir.resolve("review.html")
    val html = new String(Files.readAllBytes(htmlPath), StandardCharsets.UTF_8)

    assert(Files.exists(htmlPath))
    assert(!Files.exists(reviewDir.resolve("responses")))
    assert(!Files.exists(reviewDir.resolve("state")))
    assert(html.contains("mart"))
    assert(html.contains("customers"))
    assert(html.contains("email"))
    assert(html.contains("finding_key"))
    assert(html.contains("responses"))
    assert(html.contains("id=\"responder\""))
    assert(html.contains("data-field=\"file_identifier_pattern\""))
    assert(html.contains("data-field=\"column_name_pattern\""))
    assert(html.contains("data-field=\"pii_type_pattern\""))
    assert(!html.contains("alice@example.com"))
    assert(html.contains("a***e@example.com"))
  }
}
