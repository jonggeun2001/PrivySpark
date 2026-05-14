package io.github.jonggeun2001.privyspark.progress

import io.github.jonggeun2001.privyspark.PrivySparkSpecFixtures
import io.github.jonggeun2001.privyspark.model.ScanResult
import io.github.jonggeun2001.privyspark.review.{RecordedFileFingerprint, ReviewScopeFingerprintCodec}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.file.Files

@RunWith(classOf[JUnitRunner])
class ProgressRunManagerSpec extends AnyFunSuite with PrivySparkSpecFixtures {
  test("mergeProgressReports writes Hive table aggregated scan result rows") {
    val outputDir = Files.createTempDirectory("privyspark-progress-merge-hive-aggregate-")
    val conf = spark.sparkContext.hadoopConfiguration

    try {
      val progressRun = ProgressRunManager.prepareProgressRun(
        conf,
        outputDir.toString,
        "/data/project",
        "2026-05-14T00:00:00Z"
      )
      ProgressIO.persistProgressRecords(
        conf,
        progressRun,
        "group",
        "customers",
        Seq(
          scanResult("customers/dt=2026-05-01/part-000.parquet", 1L, 10L, 2L, 101L),
          scanResult("customers/dt=2026-05-02/part-000.parquet", 3L, 30L, 8L, 102L)
        ),
        Seq.empty
      )

      val (resultCount, errorCount) = ProgressRunManager.mergeProgressReports(spark, outputDir.toString, progressRun)
      val results = spark.read.parquet(s"${outputDir.toString}/parquet/scan_results").collect().toSeq

      assert(resultCount == 1L)
      assert(errorCount == 0L)
      assert(results.size == 1)
      val row = results.head
      assert(row.getAs[String]("file_identifier") == "customers")
      assert(row.getAs[String]("hive_table_fqn") == "mart.customers")
      assert(row.getAs[Long]("match_count") == 4L)
      assert(row.getAs[Long]("sampled_row_count") == 40L)
      assert(row.getAs[Long]("non_empty_value_count") == 10L)
      assert(row.getAs[Double]("match_ratio") == 0.1)
      assert(row.getAs[Double]("non_empty_match_ratio") == 0.4)
      assert(row.getAs[Boolean]("aggregated"))
      assert(row.getAs[Int]("aggregated_file_count") == 2)
      assert(row.getAs[Int]("aggregated_partition_count") == 2)
    } finally {
      deleteRecursively(outputDir)
    }
  }

  private def scanResult(
    fileIdentifier: String,
    matchCount: Long,
    sampledRowCount: Long,
    nonEmptyValueCount: Long,
    fileSize: Long
  ): ScanResult = {
    val fingerprint = RecordedFileFingerprint(
      fileIdentifier = fileIdentifier,
      fileSize = fileSize,
      fileMtimeEpochMs = 1710000000000L + fileSize,
      fileChecksumAlgo = "crc32",
      fileChecksum = s"checksum-$fileSize"
    )
    ScanResult(
      dataset_path = "/data/project",
      scan_timestamp = "2026-05-14T00:00:00Z",
      file_identifier = fileIdentifier,
      column_name = "email",
      pii_type = "email",
      match_count = matchCount,
      sampled_row_count = sampledRowCount,
      non_empty_value_count = nonEmptyValueCount,
      match_ratio = 0.0,
      non_empty_match_ratio = 0.0,
      confidence = 0.0,
      sample_raw_value = "owner=alice@example.com",
      sample_matched_fragment = "alice@example.com",
      file_size = fileSize,
      file_mtime_epoch_ms = fingerprint.fileMtimeEpochMs,
      hive_table_fqn = "mart.customers",
      review_scope_file_fingerprints = ReviewScopeFingerprintCodec.encode(Seq(fingerprint))
    )
  }
}
