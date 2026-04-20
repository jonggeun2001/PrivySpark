package io.github.jonggeun2001.privyspark.detect

import io.github.jonggeun2001.privyspark.model.MatchCount
import io.github.jonggeun2001.privyspark.util.DriverLogger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame

import scala.util.control.NonFatal

private[privyspark] object DetectionBatches {
  def executeThresholdFallback[T](
    scope: String,
    expressionCount: Int,
    threshold: Int,
    batchedFallback: => Seq[T],
    legacyFallback: => Seq[T]
  ): (Seq[T], String) = {
    logFallback(scope, expressionCount, s"metric_threshold_exceeded($threshold)")
    try {
      (batchedFallback, "threshold_fallback")
    } catch {
      case NonFatal(e) =>
        logFallback(scope, expressionCount, Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
        (legacyFallback, "legacy_fallback")
    }
  }

  def aggregateInBatches(
    sampledDf: DataFrame,
    metrics: Seq[DetectionAggregator.Metric],
    maxExpressionsPerAgg: Int
  ): Seq[MatchCount] = {
    DetectionMetrics.groupMetricsByExpressionBudget(metrics, maxExpressionsPerAgg).flatMap { batch =>
      val expressions = DetectionExpressions.buildExpressions(batch)
      val row = sampledDf.agg(expressions.head, expressions.tail: _*).head()

      batch.zipWithIndex.flatMap {
        case (metric, index) =>
          val count = if (row.isNullAt(index)) 0L else row.getLong(index)
          if (count > 0L) Some(MatchCount(metric.columnName, metric.piiType, count, metric.metricKey)) else None
      }
    }
  }

  def aggregateLegacy(sampledDf: DataFrame, metrics: Seq[DetectionAggregator.Metric]): Seq[MatchCount] = {
    aggregateInBatches(sampledDf, metrics, DetectionAggregator.LegacyFallbackBatchSize)
  }

  def aggregateSafeLegacy(sampledDf: DataFrame, metrics: Seq[DetectionAggregator.Metric]): Seq[MatchCount] = {
    metrics.flatMap { metric =>
      val count = sampledDf.filter(metric.predicate).count()
      if (count > 0L) Some(MatchCount(metric.columnName, metric.piiType, count, metric.metricKey)) else None
    }
  }

  def aggregateByFileInBatches(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    metrics: Seq[DetectionAggregator.Metric],
    maxExpressionsPerAgg: Int
  ): Seq[DetectionAggregator.FileMatchCount] = {
    DetectionMetrics.groupMetricsByExpressionBudget(metrics, maxExpressionsPerAgg).flatMap { batch =>
      val expressions = DetectionExpressions.buildExpressions(batch)
      val groupedRows = sampledDf.groupBy(col(fileIdentifierColumn)).agg(expressions.head, expressions.tail: _*).collect()

      groupedRows.flatMap { row =>
        val fileIdentifier = if (row.isNullAt(0)) null else row.getString(0)
        if (fileIdentifier == null || fileIdentifier.isEmpty) {
          Seq.empty
        } else {
          batch.zipWithIndex.flatMap {
            case (metric, batchIndex) =>
              val rowIndex = batchIndex + 1
              val count = if (row.isNullAt(rowIndex)) 0L else row.getLong(rowIndex)
              if (count > 0L) Some(DetectionAggregator.FileMatchCount(fileIdentifier, metric.columnName, metric.piiType, count, metric.metricKey)) else None
          }
        }
      }
    }
  }

  def aggregateByFileLegacy(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    metrics: Seq[DetectionAggregator.Metric]
  ): Seq[DetectionAggregator.FileMatchCount] = {
    aggregateByFileInBatches(sampledDf, fileIdentifierColumn, metrics, DetectionAggregator.LegacyFallbackBatchSize)
  }

  def aggregateByFileSafeLegacy(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    metrics: Seq[DetectionAggregator.Metric]
  ): Seq[DetectionAggregator.FileMatchCount] = {
    metrics.flatMap { metric =>
      val groupedRows = sampledDf
        .filter(metric.predicate)
        .groupBy(col(fileIdentifierColumn))
        .count()
        .collect()

      groupedRows.flatMap { row =>
        val fileIdentifier = if (row.isNullAt(0)) null else row.getString(0)
        val count = if (row.isNullAt(1)) 0L else row.getLong(1)
        if (fileIdentifier == null || fileIdentifier.isEmpty || count <= 0L) {
          None
        } else {
          Some(DetectionAggregator.FileMatchCount(fileIdentifier, metric.columnName, metric.piiType, count, metric.metricKey))
        }
      }
    }
  }

  private def logFallback(scope: String, expressionCount: Int, reason: String): Unit = {
    DriverLogger.warn(
      "detection_aggregation_fallback",
      "scope" -> scope,
      "expressions" -> expressionCount,
      "reason" -> reason
    )
  }
}
