package io.github.jonggeun2001.privyspark.report

import io.github.jonggeun2001.privyspark.model.ScanResult
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JsonCodecSpec extends AnyFunSuite {
  test("extractJsonStringField ignores field-like tokens inside string literals") {
    val json =
      """{"reason":"contains escaped token: \"file_checksum\":\"deadbeef\"","file_checksum":"cafebabe"}"""

    assert(JsonCodec.extractJsonStringField(json, "file_checksum").contains("cafebabe"))
  }

  test("extractJsonLongField ignores numeric field-like tokens inside string literals") {
    val json =
      """{"reason":"contains escaped token: \"file_size\":123","file_size":456}"""

    assert(JsonCodec.extractJsonLongField(json, "file_size").contains(456L))
  }

  test("scanResultToJson includes hive_table_fqn") {
    val json = JsonCodec.scanResultToJson(
      ScanResult(
        dataset_path = "/data",
        scan_timestamp = "2026-04-27T00:00:00Z",
        file_identifier = "customers.csv",
        column_name = "email",
        pii_type = "email",
        match_count = 1L,
        sampled_row_count = 1L,
        match_ratio = 1.0,
        non_empty_match_ratio = 1.0,
        confidence = 0.21,
        sample_raw_value = "alice@example.com",
        sample_matched_fragment = "alice@example.com",
        hive_table_fqn = "mart.customers"
      )
    )

    assert(JsonCodec.extractJsonStringField(json, "hive_table_fqn").contains("mart.customers"))
  }
}
