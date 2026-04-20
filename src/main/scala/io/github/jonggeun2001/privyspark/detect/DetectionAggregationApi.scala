package io.github.jonggeun2001.privyspark.detect

import io.github.jonggeun2001.privyspark.config.SuppressionSet
import io.github.jonggeun2001.privyspark.model.{MatchCount, PiiRule, SampleValue}
import org.apache.spark.sql.DataFrame

import scala.util.control.NonFatal

private[privyspark] object DetectionAggregationApi {
  def aggregate(
    sampledDf: DataFrame,
    rules: Seq[PiiRule],
    suppressions: SuppressionSet,
    config: DetectionAggregator.AggregationConfig
  ): Seq[MatchCount] = {
    val columns = sampledDf.columns.toSeq
    DetectionAggregator.logDebug("detection_aggregation_start", "scope" -> "dataset", "columns" -> columns.size, "rules" -> rules.size)
    if (columns.isEmpty || rules.isEmpty) {
      DetectionAggregator.logDebug("detection_aggregation_complete", "scope" -> "dataset", "metrics" -> 0, "results" -> 0, "mode" -> "noop")
      return Seq.empty
    }

    require(config.maxExpressionsPerAgg > 0, "maxExpressionsPerAgg must be > 0")
    require(config.legacyFallbackThreshold > 0, "legacyFallbackThreshold must be > 0")

    val metrics = DetectionMetrics.buildMetrics(columns, rules, suppressions)
    if (metrics.isEmpty) {
      DetectionAggregator.logDebug("detection_aggregation_complete", "scope" -> "dataset", "metrics" -> 0, "results" -> 0, "mode" -> "noop")
      return Seq.empty
    }
    val expressionCount = DetectionMetrics.totalExpressionCount(metrics)
    DetectionAggregator.logDebug("detection_aggregation_metrics_built", "scope" -> "dataset", "metrics" -> metrics.size, "expressions" -> expressionCount)

    if (expressionCount > config.legacyFallbackThreshold) {
      val fallback = DetectionBatches.executeThresholdFallback(
        "dataset",
        expressionCount,
        config.legacyFallbackThreshold,
        DetectionBatches.aggregateLegacy(sampledDf, metrics),
        DetectionBatches.aggregateSafeLegacy(sampledDf, metrics)
      )
      DetectionAggregator.logDebug(
        "detection_aggregation_complete",
        "scope" -> "dataset",
        "metrics" -> metrics.size,
        "results" -> fallback._1.size,
        "mode" -> fallback._2
      )
      return fallback._1
    }

    try {
      val results = DetectionBatches.aggregateInBatches(sampledDf, metrics, config.maxExpressionsPerAgg)
      DetectionAggregator.logDebug(
        "detection_aggregation_complete",
        "scope" -> "dataset",
        "metrics" -> metrics.size,
        "results" -> results.size,
        "mode" -> "batched_agg"
      )
      results
    } catch {
      case NonFatal(e) =>
        val results = DetectionBatches.aggregateSafeLegacy(sampledDf, metrics)
        DetectionAggregator.logDebug(
          "detection_aggregation_complete",
          "scope" -> "dataset",
          "metrics" -> metrics.size,
          "results" -> results.size,
          "mode" -> "legacy_fallback"
        )
        results
    }
  }

  def aggregateByFile(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    rules: Seq[PiiRule],
    suppressions: SuppressionSet,
    config: DetectionAggregator.AggregationConfig
  ): Seq[DetectionAggregator.FileMatchCount] = {
    require(fileIdentifierColumn.nonEmpty, "fileIdentifierColumn must not be empty")

    val columns = sampledDf.columns.toSeq.filterNot(_ == fileIdentifierColumn)
    DetectionAggregator.logDebug("detection_aggregation_start", "scope" -> "file", "columns" -> columns.size, "rules" -> rules.size)
    if (columns.isEmpty || rules.isEmpty) {
      DetectionAggregator.logDebug("detection_aggregation_complete", "scope" -> "file", "metrics" -> 0, "results" -> 0, "mode" -> "noop")
      return Seq.empty
    }

    require(config.maxExpressionsPerAgg > 0, "maxExpressionsPerAgg must be > 0")
    require(config.legacyFallbackThreshold > 0, "legacyFallbackThreshold must be > 0")

    val metrics = DetectionMetrics.buildMetrics(columns, rules, suppressions)
    if (metrics.isEmpty) {
      DetectionAggregator.logDebug("detection_aggregation_complete", "scope" -> "file", "metrics" -> 0, "results" -> 0, "mode" -> "noop")
      return Seq.empty
    }
    val expressionCount = DetectionMetrics.totalExpressionCount(metrics)
    DetectionAggregator.logDebug("detection_aggregation_metrics_built", "scope" -> "file", "metrics" -> metrics.size, "expressions" -> expressionCount)

    if (expressionCount > config.legacyFallbackThreshold) {
      val fallback = DetectionBatches.executeThresholdFallback(
        "file",
        expressionCount,
        config.legacyFallbackThreshold,
        DetectionBatches.aggregateByFileLegacy(sampledDf, fileIdentifierColumn, metrics),
        DetectionBatches.aggregateByFileSafeLegacy(sampledDf, fileIdentifierColumn, metrics)
      )
      DetectionAggregator.logDebug(
        "detection_aggregation_complete",
        "scope" -> "file",
        "metrics" -> metrics.size,
        "results" -> fallback._1.size,
        "mode" -> fallback._2
      )
      return fallback._1
    }

    try {
      val results = DetectionBatches.aggregateByFileInBatches(sampledDf, fileIdentifierColumn, metrics, config.maxExpressionsPerAgg)
      DetectionAggregator.logDebug(
        "detection_aggregation_complete",
        "scope" -> "file",
        "metrics" -> metrics.size,
        "results" -> results.size,
        "mode" -> "batched_agg"
      )
      results
    } catch {
      case NonFatal(_) =>
        val results = DetectionBatches.aggregateByFileSafeLegacy(sampledDf, fileIdentifierColumn, metrics)
        DetectionAggregator.logDebug(
          "detection_aggregation_complete",
          "scope" -> "file",
          "metrics" -> metrics.size,
          "results" -> results.size,
          "mode" -> "legacy_fallback"
        )
        results
    }
  }

  def sampleMatches(
    sampledDf: DataFrame,
    rules: Seq[PiiRule],
    matchCounts: Seq[MatchCount],
    suppressions: SuppressionSet,
    config: DetectionAggregator.AggregationConfig
  ): Map[String, SampleValue] = {
    require(config.maxExpressionsPerAgg > 0, "maxExpressionsPerAgg must be > 0")
    require(config.legacyFallbackThreshold > 0, "legacyFallbackThreshold must be > 0")

    if (matchCounts.isEmpty) {
      Map.empty
    } else {
      val requestedKeys = matchCounts.map(matchCount => (matchCount.columnName, matchCount.piiType)).toSet
      val requestedAliases = matchCounts.map(_.metricAlias).filter(_.nonEmpty).toSet
      val metrics = DetectionMetrics.buildMetrics(sampledDf.columns.toSeq, rules, suppressions).filter { metric =>
        if (requestedAliases.nonEmpty) {
          requestedAliases.contains(metric.metricKey)
        } else {
          requestedKeys.contains((metric.columnName, metric.piiType))
        }
      }
      val expressionCount = DetectionMetrics.totalExpressionCount(metrics)
      val rawValues =
        if (expressionCount > config.legacyFallbackThreshold) {
          DetectionBatches.executeThresholdFallback(
            "dataset_sample",
            expressionCount,
            config.legacyFallbackThreshold,
            DetectionSampling.collectSampleRawValues(sampledDf, metrics, DetectionAggregator.LegacyFallbackBatchSize).toSeq,
            DetectionSampling.collectSampleRawValuesSafely(sampledDf, metrics).toSeq
          )._1.toMap
        } else {
          try {
            DetectionSampling.collectSampleRawValues(sampledDf, metrics, config.maxExpressionsPerAgg)
          } catch {
            case NonFatal(e) =>
              DetectionAggregator.logFallback("dataset_sample", expressionCount, Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
              DetectionSampling.collectSampleRawValuesSafely(sampledDf, metrics)
          }
        }

      DetectionSampling.buildSampleValuesByAlias(metrics, rawValues)
    }
  }

  def sampleMatchesByFile(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    rules: Seq[PiiRule],
    matchCounts: Seq[DetectionAggregator.FileMatchCount],
    suppressions: SuppressionSet,
    config: DetectionAggregator.AggregationConfig
  ): Map[(String, String), SampleValue] = {
    require(fileIdentifierColumn.nonEmpty, "fileIdentifierColumn must not be empty")
    require(config.maxExpressionsPerAgg > 0, "maxExpressionsPerAgg must be > 0")
    require(config.legacyFallbackThreshold > 0, "legacyFallbackThreshold must be > 0")

    if (matchCounts.isEmpty) {
      Map.empty
    } else {
      val requestedAliases = matchCounts
        .map(matchCount => (matchCount.fileIdentifier, matchCount.metricAlias))
        .filter(_._2.nonEmpty)
        .toSet
      val requestedMetricKeys = matchCounts.map(matchCount => (matchCount.columnName, matchCount.piiType)).toSet
      val requestedMetricAliases = requestedAliases.map(_._2)
      val metrics = DetectionMetrics.buildMetrics(sampledDf.columns.toSeq.filterNot(_ == fileIdentifierColumn), rules, suppressions)
        .filter { metric =>
          if (requestedMetricAliases.nonEmpty) {
            requestedMetricAliases.contains(metric.metricKey)
          } else {
            requestedMetricKeys.contains((metric.columnName, metric.piiType))
          }
        }
      val expressionCount = DetectionMetrics.totalExpressionCount(metrics)
      val rawValues =
        if (expressionCount > config.legacyFallbackThreshold) {
          DetectionBatches.executeThresholdFallback(
            "file_sample",
            expressionCount,
            config.legacyFallbackThreshold,
            DetectionSampling.collectSampleRawValuesByFile(sampledDf, fileIdentifierColumn, metrics, DetectionAggregator.LegacyFallbackBatchSize).toSeq,
            DetectionSampling.collectSampleRawValuesByFileSafely(sampledDf, fileIdentifierColumn, metrics).toSeq
          )._1.toMap
        } else {
          try {
            DetectionSampling.collectSampleRawValuesByFile(sampledDf, fileIdentifierColumn, metrics, config.maxExpressionsPerAgg)
          } catch {
            case NonFatal(e) =>
              DetectionAggregator.logFallback("file_sample", expressionCount, Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
              DetectionSampling.collectSampleRawValuesByFileSafely(sampledDf, fileIdentifierColumn, metrics)
          }
        }

      DetectionSampling.buildSampleValuesByFileAlias(metrics, rawValues, requestedAliases)
    }
  }
}
