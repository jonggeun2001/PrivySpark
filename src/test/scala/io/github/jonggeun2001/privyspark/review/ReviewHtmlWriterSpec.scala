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

  test("write can place review html at a configured file path outside scan output") {
    val outputRoot = Files.createTempDirectory("privyspark-review-html-output-")
    val customRoot = Files.createTempDirectory("privyspark-review-html-custom-")
    val customHtmlPath = customRoot.resolve("owner-review.html")
    val result = ScanResult(
      dataset_path = "/data/project",
      scan_timestamp = "2026-04-27T10:00:00Z",
      file_identifier = "customers/part-000.parquet",
      column_name = "email",
      pii_type = "email",
      match_count = 1L,
      sampled_row_count = 2L,
      match_ratio = 0.5,
      non_empty_match_ratio = 0.5,
      confidence = 0.2,
      sample_raw_value = "owner=bob@example.com",
      sample_matched_fragment = "bob@example.com",
      file_size = 128L,
      file_mtime_epoch_ms = 1710000000000L
    )

    ReviewHtmlWriter.write(
      new Configuration(),
      outputRoot.toString,
      "/data/project",
      Seq(result),
      sampleMode = "none",
      reviewHtmlPath = Some(customHtmlPath.toString)
    )

    val defaultHtmlPath = outputRoot.resolve("review").resolve("review.html")
    val html = new String(Files.readAllBytes(customHtmlPath), StandardCharsets.UTF_8)

    assert(Files.exists(customHtmlPath))
    assert(!Files.exists(defaultHtmlPath))
    assert(html.contains("PrivySpark Review"))
    assert(!html.contains("bob@example.com"))
  }

  test("write renders allowlist scope in a separate column with collector-matched guidance") {
    val outputRoot = Files.createTempDirectory("privyspark-review-scope-hint-")
    val fingerprints = ReviewScopeFingerprintCodec.encode(Seq(
      RecordedFileFingerprint(
        fileIdentifier = "customers/part-000.parquet",
        fileSize = 128L,
        fileMtimeEpochMs = 1710000000000L,
        fileChecksumAlgo = "sha256",
        fileChecksum = "aaa"
      ),
      RecordedFileFingerprint(
        fileIdentifier = "customers/part-001.parquet",
        fileSize = 256L,
        fileMtimeEpochMs = 1710000001000L,
        fileChecksumAlgo = "sha256",
        fileChecksum = "bbb"
      )
    ))
    val result = ScanResult(
      dataset_path = "/data/project",
      scan_timestamp = "2026-04-27T10:00:00Z",
      file_identifier = "customers",
      column_name = "temp_driver_no",
      pii_type = "driver_license_number",
      match_count = 2L,
      sampled_row_count = 10L,
      match_ratio = 0.2,
      non_empty_match_ratio = 0.2,
      confidence = 0.1,
      sample_raw_value = "991231-1234567",
      sample_matched_fragment = "991231-1234567",
      file_size = 128L,
      file_mtime_epoch_ms = 1710000000000L,
      review_scope_file_fingerprints = fingerprints
    )

    ReviewHtmlWriter.write(
      new Configuration(),
      outputRoot.toString,
      "/data/project",
      Seq(result),
      sampleMode = "masked"
    )

    val htmlPath = outputRoot.resolve("review").resolve("review.html")
    val html = new String(Files.readAllBytes(htmlPath), StandardCharsets.UTF_8)

    assert(html.contains("<th>Allowlist Scope</th>"))
    assert(html.contains("class=\"scope-cell\""))
    assert(html.contains("data-field=\"allowlist_scope\""))
    assert(html.contains("\"fingerprint_complete\":true"))
    assert(html.contains("\"has_multiple_file_evidence\":true"))
    assert(html.contains("exact: 이 finding만 제외"))
    assert(html.contains("fingerprint가 모두 다시 일치"))
    assert(html.contains("pattern: 반복 오탐을 넓게 제외"))
    assert(html.contains("여러 파일 증거가 있어 file_identifier_pattern 필수"))
    assert(html.contains("*는 glob 와일드카드"))
    assert(html.contains("pii_type=* 금지"))
  }
}
