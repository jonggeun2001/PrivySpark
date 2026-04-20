package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.model.{MatchCount, PiiRule, SampleValue, ScanResult}
import io.github.jonggeun2001.privyspark.review.{ReviewScopeFingerprintCodec, ReviewScopeIdentifierCodec}
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
    reviewScopeFileIdentifiers: Seq[String] = Seq.empty,
    reviewScopeFileFingerprints: String = ""
  ): Seq[ScanResult] = {
    if (sampledRowCount <= 0L) {
      Seq.empty
    } else {
      matchCounts.map { matchCount =>
        val matchRatio = roundProbability(matchCount.count.toDouble / sampledRowCount.toDouble)
        val nonEmptyDenominator = nonEmptyValueCounts.get(matchCount.columnName).filter(_ > 0L).getOrElse(sampledRowCount)
        val nonEmptyMatchRatio = roundProbability(matchCount.count.toDouble / nonEmptyDenominator.toDouble)
        val confidenceValue = roundProbability(wilsonLowerBound(matchCount.count, nonEmptyDenominator))
        val sampleValue = sampleValues.get(matchCount.metricAlias)
        ScanResult(
          dataset_path = datasetPath,
          scan_timestamp = scanTimestamp,
          file_identifier = fileIdentifier,
          column_name = matchCount.columnName,
          pii_type = matchCount.piiType,
          match_count = matchCount.count,
          sampled_row_count = sampledRowCount,
          match_ratio = matchRatio,
          non_empty_match_ratio = nonEmptyMatchRatio,
          confidence = confidenceValue,
          sample_raw_value = sampleValue.map(_.sampleRawValue).getOrElse(""),
          sample_matched_fragment = sampleValue.map(_.sampleMatchedFragment).getOrElse(""),
          file_size = fileSize,
          file_mtime_epoch_ms = fileMtimeEpochMs,
          review_scope_file_identifiers = ReviewScopeIdentifierCodec.encode(reviewScopeFileIdentifiers),
          review_scope_file_fingerprints = reviewScopeFileFingerprints
        )
      }
    }
  }

  def comparableResultPayloads(results: Seq[ScanResult]): Seq[(String, String, String, Long, Long, Double, Double, Double)] =
    results
      .map(result =>
        (
          result.file_identifier,
          result.column_name,
          result.pii_type,
          result.match_count,
          result.sampled_row_count,
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

  private def wilsonLowerBound(successes: Long, trials: Long): Double = {
    if (trials <= 0L) {
      0.0
    } else {
      val n = trials.toDouble
      val p = successes.toDouble / n
      val z = 1.96
      val z2 = z * z
      val center = p + z2 / (2.0 * n)
      val margin = z * math.sqrt(p * (1.0 - p) / n + z2 / (4.0 * n * n))
      val denominator = 1.0 + z2 / n
      val lowerBound = (center - margin) / denominator
      math.max(0.0, math.min(1.0, lowerBound))
    }
  }

  private def roundProbability(value: Double): Double = {
    BigDecimal.decimal(value)
      .setScale(2, scala.math.BigDecimal.RoundingMode.HALF_UP)
      .toDouble
  }
}
