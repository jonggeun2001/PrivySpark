package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.model.{MatchCount, PiiRule, SampleValue, ScanResult}
import io.github.jonggeun2001.privyspark.review.{ReviewScopeFingerprintCodec, ReviewScopeIdentifierCodec}
import io.github.jonggeun2001.privyspark.util.DetectionMetricMath
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, lit, pmod, xxhash64}

import java.time.Instant

private[privyspark] object ScanResultBuilder {
  private val DeterministicSampleBuckets = 1000000L

  def effectiveRulesForFormat(format: String, rules: Seq[PiiRule]): Seq[PiiRule] = {
    rules
  }

  def buildScanResults(
    datasetPath: String,
    scanTimestamp: String,
    fileIdentifier: String,
    sampledRowCount: Long,
    nonEmptyValueCounts: Map[String, Long],
    matchCounts: Seq[MatchCount],
    sampleValues: Map[String, SampleValue] = Map.empty,
    fileSize: Long = 0L,
    fileMtimeEpochMs: Long = 0L,
    hiveTableFqn: String = "",
    reviewScopeFileIdentifiers: Seq[String] = Seq.empty,
    reviewScopeFileFingerprints: String = ""
  ): Seq[ScanResult] = {
    if (sampledRowCount <= 0L) {
      Seq.empty
    } else {
      matchCounts.map { matchCount =>
        val nonEmptyDenominator = nonEmptyValueCounts.get(matchCount.columnName).filter(_ > 0L).getOrElse(sampledRowCount)
        val sampleValue = sampleValues.get(matchCount.metricAlias)
        ScanResult(
          dataset_path = datasetPath,
          scan_timestamp = scanTimestamp,
          file_identifier = fileIdentifier,
          column_name = matchCount.columnName,
          pii_type = matchCount.piiType,
          match_count = matchCount.count,
          sampled_row_count = sampledRowCount,
          non_empty_value_count = nonEmptyDenominator,
          match_ratio = DetectionMetricMath.ratio(matchCount.count, sampledRowCount),
          non_empty_match_ratio = DetectionMetricMath.ratio(matchCount.count, nonEmptyDenominator),
          confidence = DetectionMetricMath.wilsonLowerBound(matchCount.count, nonEmptyDenominator),
          sample_raw_value = sampleValue.map(_.sampleRawValue).getOrElse(""),
          sample_matched_fragment = sampleValue.map(_.sampleMatchedFragment).getOrElse(""),
          file_size = fileSize,
          file_mtime_epoch_ms = fileMtimeEpochMs,
          hive_table_fqn = hiveTableFqn,
          review_scope_file_identifiers = ReviewScopeIdentifierCodec.encode(reviewScopeFileIdentifiers),
          review_scope_file_fingerprints = reviewScopeFileFingerprints
        )
      }
    }
  }

  def comparableResultPayloads(results: Seq[ScanResult]): Seq[(String, String, String, Long, Long, Long, Double, Double, Double)] =
    // Hive mapping depends on external metastore state and must not invalidate review snapshots.
    results
      .map(result =>
        (
          result.file_identifier,
          result.column_name,
          result.pii_type,
          result.match_count,
          result.sampled_row_count,
          result.non_empty_value_count,
          result.match_ratio,
          result.non_empty_match_ratio,
          result.confidence
        )
      )
      .sortBy(value => (value._1, value._2, value._3))

  def currentScanTimestamp(): String = Instant.now().toString

  def sampleRowsDeterministically(sourceDf: DataFrame, sampleRatio: Double): DataFrame = {
    if (sampleRatio >= 1.0) {
      sourceDf
    } else {
      val sampleThreshold = math.round(sampleRatio * DeterministicSampleBuckets.toDouble)
      if (sampleThreshold <= 0L) {
        sourceDf.limit(0)
      } else {
        val hashInputs =
          if (sourceDf.columns.isEmpty) {
            Seq(lit("__privyspark_empty__"))
          } else {
            sourceDf.columns.toSeq.map(columnName =>
              coalesce(col(columnName).cast("string"), lit("__privyspark_null__"))
            )
          }
        val bucket = pmod(xxhash64(hashInputs: _*), lit(DeterministicSampleBuckets))
        sourceDf.where(bucket < lit(sampleThreshold))
      }
    }
  }

}
