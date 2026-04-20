package io.github.jonggeun2001.privyspark.detect

import io.github.jonggeun2001.privyspark.config.SuppressionSet
import io.github.jonggeun2001.privyspark.model.{MatchCount, PiiRule, PiiRuleMatchType, SampleValue}
import io.github.jonggeun2001.privyspark.util.DriverLogger
import org.apache.spark.sql.{Column, DataFrame}

import java.util.regex.Pattern

object DetectionAggregator {
  final case class FileMatchCount(fileIdentifier: String, columnName: String, piiType: String, count: Long, metricAlias: String = "")
  final case class AggregationConfig(maxExpressionsPerAgg: Int = 400, legacyFallbackThreshold: Int = 50000)

  private[detect] final case class Metric(
    alias: String,
    metricKey: String,
    columnName: String,
    piiType: String,
    regex: String,
    matchType: String,
    pattern: Pattern,
    predicate: Column
  ) {
    val expressionCount: Int = 1
  }
  private[detect] final case class ExtractedMatch(fragment: String, start: Int, end: Int)
  private[detect] val LegacyFallbackBatchSize = 50
  private[detect] val SafeSampleFallbackBatchSize = 32
  @volatile private[detect] var forceDatasetBatchFailure = false
  @volatile private[detect] var forceFileBatchFailure = false
  @volatile private[detect] var forceFileSampleBatchFailure = false

  private[privyspark] def resetDebugCache(): Unit = {
    DriverLogger.resetCache()
  }

  private[privyspark] def withForcedDatasetBatchFailure[A](block: => A): A = {
    val previous = forceDatasetBatchFailure
    forceDatasetBatchFailure = true
    try {
      block
    } finally {
      forceDatasetBatchFailure = previous
    }
  }

  private[privyspark] def withForcedFileBatchFailure[A](block: => A): A = {
    val previous = forceFileBatchFailure
    forceFileBatchFailure = true
    try {
      block
    } finally {
      forceFileBatchFailure = previous
    }
  }

  private[privyspark] def withForcedFileSampleBatchFailure[A](block: => A): A = {
    val previous = forceFileSampleBatchFailure
    forceFileSampleBatchFailure = true
    try {
      block
    } finally {
      forceFileSampleBatchFailure = previous
    }
  }

  private[detect] def logDebug(event: String, fields: (String, Any)*): Unit = {
    DriverLogger.debug(event, fields: _*)
  }

  private[detect] def logFallback(scope: String, expressionCount: Int, reason: String): Unit = {
    DriverLogger.warn(
      "detection_aggregation_fallback",
      "scope" -> scope,
      "expressions" -> expressionCount,
      "reason" -> reason
    )
  }

  def aggregate(sampledDf: DataFrame, rules: Seq[PiiRule]): Seq[MatchCount] = {
    aggregate(sampledDf, rules, SuppressionSet.empty, AggregationConfig())
  }

  private[privyspark] def aggregate(
    sampledDf: DataFrame,
    rules: Seq[PiiRule],
    config: AggregationConfig
  ): Seq[MatchCount] = {
    aggregate(sampledDf, rules, SuppressionSet.empty, config)
  }

  private[privyspark] def aggregate(
    sampledDf: DataFrame,
    rules: Seq[PiiRule],
    suppressions: SuppressionSet,
    config: AggregationConfig = AggregationConfig()
  ): Seq[MatchCount] = {
    DetectionAggregationApi.aggregate(sampledDf, rules, suppressions, config)
  }

  def aggregateByFile(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    rules: Seq[PiiRule]
  ): Seq[FileMatchCount] = {
    aggregateByFile(sampledDf, fileIdentifierColumn, rules, SuppressionSet.empty, AggregationConfig())
  }

  private[privyspark] def aggregateByFile(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    rules: Seq[PiiRule],
    config: AggregationConfig
  ): Seq[FileMatchCount] = {
    aggregateByFile(sampledDf, fileIdentifierColumn, rules, SuppressionSet.empty, config)
  }

  private[privyspark] def aggregateByFile(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    rules: Seq[PiiRule],
    suppressions: SuppressionSet,
    config: AggregationConfig = AggregationConfig()
  ): Seq[FileMatchCount] = {
    DetectionAggregationApi.aggregateByFile(sampledDf, fileIdentifierColumn, rules, suppressions, config)
  }

  def countNonEmpty(
    sampledDf: DataFrame,
    columns: Seq[String]
  ): Map[String, Long] = {
    countNonEmpty(sampledDf, columns, AggregationConfig())
  }

  private[privyspark] def countNonEmpty(
    sampledDf: DataFrame,
    columns: Seq[String],
    config: AggregationConfig
  ): Map[String, Long] = {
    DetectionCounts.countNonEmpty(sampledDf, columns, config)
  }

  def countNonEmptyByFile(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    columns: Seq[String]
  ): Map[(String, String), Long] = {
    countNonEmptyByFile(sampledDf, fileIdentifierColumn, columns, AggregationConfig())
  }

  private[privyspark] def countNonEmptyByFile(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    columns: Seq[String],
    config: AggregationConfig
  ): Map[(String, String), Long] = {
    DetectionCounts.countNonEmptyByFile(sampledDf, fileIdentifierColumn, columns, config)
  }

  private[privyspark] def columnsCoveredByRules(
    columns: Seq[String],
    rules: Seq[PiiRule]
  ): Seq[String] = {
    columnsCoveredByRules(columns, rules, SuppressionSet.empty)
  }

  private[privyspark] def columnsCoveredByRules(
    columns: Seq[String],
    rules: Seq[PiiRule],
    suppressions: SuppressionSet
  ): Seq[String] = {
    DetectionCounts.columnsCoveredByRules(columns, rules, suppressions)
  }

  def sampleMatches(
    sampledDf: DataFrame,
    rules: Seq[PiiRule],
    matchCounts: Seq[MatchCount]
  ): Map[String, SampleValue] = {
    sampleMatches(sampledDf, rules, matchCounts, SuppressionSet.empty, AggregationConfig())
  }

  private[privyspark] def sampleMatches(
    sampledDf: DataFrame,
    rules: Seq[PiiRule],
    matchCounts: Seq[MatchCount],
    config: AggregationConfig
  ): Map[String, SampleValue] = {
    sampleMatches(sampledDf, rules, matchCounts, SuppressionSet.empty, config)
  }

  private[privyspark] def sampleMatches(
    sampledDf: DataFrame,
    rules: Seq[PiiRule],
    matchCounts: Seq[MatchCount],
    suppressions: SuppressionSet,
    config: AggregationConfig = AggregationConfig()
  ): Map[String, SampleValue] = {
    DetectionAggregationApi.sampleMatches(sampledDf, rules, matchCounts, suppressions, config)
  }

  def sampleMatchesByFile(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    rules: Seq[PiiRule],
    matchCounts: Seq[FileMatchCount]
  ): Map[(String, String), SampleValue] = {
    sampleMatchesByFile(sampledDf, fileIdentifierColumn, rules, matchCounts, SuppressionSet.empty, AggregationConfig())
  }

  private[privyspark] def sampleMatchesByFile(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    rules: Seq[PiiRule],
    matchCounts: Seq[FileMatchCount],
    config: AggregationConfig
  ): Map[(String, String), SampleValue] = {
    sampleMatchesByFile(sampledDf, fileIdentifierColumn, rules, matchCounts, SuppressionSet.empty, config)
  }

  private[privyspark] def sampleMatchesByFile(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    rules: Seq[PiiRule],
    matchCounts: Seq[FileMatchCount],
    suppressions: SuppressionSet,
    config: AggregationConfig = AggregationConfig()
  ): Map[(String, String), SampleValue] = {
    DetectionAggregationApi.sampleMatchesByFile(sampledDf, fileIdentifierColumn, rules, matchCounts, suppressions, config)
  }

  private def buildMetrics(
    columns: Seq[String],
    rules: Seq[PiiRule],
    suppressions: SuppressionSet
  ): Seq[Metric] = {
    DetectionMetrics.buildMetrics(columns, rules, suppressions)
  }

  private def collectSampleRawValuesSafely(
    sampledDf: DataFrame,
    metrics: Seq[Metric]
  ): Map[String, String] = {
    DetectionSampling.collectSampleRawValuesSafely(sampledDf, metrics)
  }

  private def collectSampleRawValuesByFileSafely(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    metrics: Seq[Metric]
  ): Map[(String, String), String] = {
    DetectionSampling.collectSampleRawValuesByFileSafely(sampledDf, fileIdentifierColumn, metrics)
  }

  private[privyspark] def executeThresholdFallback[T](
    scope: String,
    expressionCount: Int,
    threshold: Int,
    batchedFallback: => Seq[T],
    legacyFallback: => Seq[T]
  ): (Seq[T], String) = {
    DetectionBatches.executeThresholdFallback(scope, expressionCount, threshold, batchedFallback, legacyFallback)
  }
}
