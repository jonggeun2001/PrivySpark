package io.github.jonggeun2001.privyspark.report

import io.github.jonggeun2001.privyspark.PrivySparkSpecFixtures
import io.github.jonggeun2001.privyspark.model.ScanResult
import io.github.jonggeun2001.privyspark.review.{RecordedFileFingerprint, ReviewScopeFingerprintCodec}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ScanResultReportAggregatorSpec extends AnyFunSuite with PrivySparkSpecFixtures {
  test("aggregateForReport collapses Hive partition scan rows and recalculates ratios from summed denominators") {
    import spark.implicits._

    val fingerprints = Seq(
      recordedFingerprint("customers/dt=2026-05-01/part-000.parquet", 101L),
      recordedFingerprint("customers/dt=2026-05-02/part-000.parquet", 102L)
    )
    val rawResults = Seq(
      scanResult(
        fileIdentifier = "customers/dt=2026-05-01/part-000.parquet",
        matchCount = 1L,
        sampledRowCount = 10L,
        nonEmptyValueCount = 2L,
        sampleRawValue = "owner=alice@example.com",
        sampleMatchedFragment = "alice@example.com",
        fingerprint = fingerprints.head
      ),
      scanResult(
        fileIdentifier = "customers/dt=2026-05-02/part-000.parquet",
        matchCount = 3L,
        sampledRowCount = 30L,
        nonEmptyValueCount = 8L,
        sampleRawValue = "owner=bob@example.com",
        sampleMatchedFragment = "bob@example.com",
        fingerprint = fingerprints(1)
      )
    ).toDF()

    val aggregated = ScanResultReportAggregator.aggregateForReport(rawResults).as[ScanResult].collect().toSeq

    assert(aggregated.size == 1)
    val result = aggregated.head
    assert(result.file_identifier == "customers")
    assert(result.hive_table_fqn == "mart.customers")
    assert(result.match_count == 4L)
    assert(result.sampled_row_count == 40L)
    assert(result.non_empty_value_count == 10L)
    assert(result.match_ratio == 0.1)
    assert(result.non_empty_match_ratio == 0.4)
    assert(result.aggregated)
    assert(result.aggregated_file_count == 2)
    assert(result.aggregated_partition_count == 2)
    val scopeFingerprints = ReviewScopeFingerprintCodec.decode(result.review_scope_file_fingerprints).fold(
      errorMessage => fail(errorMessage),
      identity
    )
    assert(scopeFingerprints.map(_.fileIdentifier) ==
      Seq("customers/dt=2026-05-01/part-000.parquet", "customers/dt=2026-05-02/part-000.parquet"))
  }

  test("aggregateForReport keeps non-Hive scan rows separated by file identifier") {
    import spark.implicits._

    val rawResults = Seq(
      scanResult("customers/a.csv", matchCount = 1L, sampledRowCount = 10L, nonEmptyValueCount = 5L, hiveTableFqn = ""),
      scanResult("customers/b.csv", matchCount = 1L, sampledRowCount = 10L, nonEmptyValueCount = 5L, hiveTableFqn = "")
    ).toDF()

    val aggregated = ScanResultReportAggregator.aggregateForReport(rawResults).as[ScanResult].collect().toSeq

    assert(aggregated.map(_.file_identifier).toSet == Set("customers/a.csv", "customers/b.csv"))
    assert(aggregated.forall(!_.aggregated))
  }

  private def scanResult(
    fileIdentifier: String,
    matchCount: Long,
    sampledRowCount: Long,
    nonEmptyValueCount: Long,
    sampleRawValue: String = "owner=alice@example.com",
    sampleMatchedFragment: String = "alice@example.com",
    hiveTableFqn: String = "mart.customers",
    fingerprint: RecordedFileFingerprint = recordedFingerprint("customers/dt=2026-05-01/part-000.parquet", 101L)
  ): ScanResult =
    ScanResult(
      dataset_path = "/data/project",
      scan_timestamp = "2026-05-01T00:00:00Z",
      file_identifier = fileIdentifier,
      column_name = "email",
      pii_type = "email",
      match_count = matchCount,
      sampled_row_count = sampledRowCount,
      non_empty_value_count = nonEmptyValueCount,
      match_ratio = 0.0,
      non_empty_match_ratio = 0.0,
      confidence = 0.0,
      sample_raw_value = sampleRawValue,
      sample_matched_fragment = sampleMatchedFragment,
      file_size = fingerprint.fileSize,
      file_mtime_epoch_ms = fingerprint.fileMtimeEpochMs,
      hive_table_fqn = hiveTableFqn,
      review_scope_file_fingerprints = ReviewScopeFingerprintCodec.encode(Seq(fingerprint))
    )

  private def recordedFingerprint(fileIdentifier: String, size: Long): RecordedFileFingerprint =
    RecordedFileFingerprint(
      fileIdentifier = fileIdentifier,
      fileSize = size,
      fileMtimeEpochMs = 1710000000000L + size,
      fileChecksumAlgo = "crc32",
      fileChecksum = s"checksum-$size"
    )
}
