package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.model.{PiiRule, PiiRuleMatchType}
import org.apache.spark.sql.functions.{col, lit, sum => sparkSum, trim, udf, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, Row}

import java.time.Instant
import scala.util.control.NonFatal

object DetectionAggregator {
  final case class MatchCount(columnName: String, piiType: String, count: Long)
  final case class FileMatchCount(fileIdentifier: String, columnName: String, piiType: String, count: Long)
  final case class AggregationConfig(maxExpressionsPerAgg: Int = 400, legacyFallbackThreshold: Int = 50000)

  private final case class Metric(
    alias: String,
    columnName: String,
    piiType: String,
    matchType: String,
    predicate: Column
  ) {
    val expressionCount: Int = 1
  }
  private val DebugPropertyName = "privyspark.debug"
  private val DebugEnvName = "PRIVYSPARK_DEBUG"
  private val LegacyFallbackBatchSize = 50
  private val DriverLicenseValidatorUdf = udf((value: String) => DriverLicenseNumberValidator.containsValidCandidate(value))
  @volatile private var debugLoggingEnabledCache: java.lang.Boolean = _

  private def isDebugLoggingEnabled: Boolean = {
    val cached = debugLoggingEnabledCache
    if (cached != null) {
      return cached.booleanValue()
    }

    val enabled = sys.props.get(DebugPropertyName).orElse(sys.env.get(DebugEnvName)).exists { value =>
      value.trim.toLowerCase match {
        case "1" | "true" | "yes" | "on" => true
        case _ => false
      }
    }
    debugLoggingEnabledCache = java.lang.Boolean.valueOf(enabled)
    enabled
  }

  private[privyspark] def resetDebugCache(): Unit = {
    debugLoggingEnabledCache = null
  }

  private def currentDebugTimestamp(): String = Instant.now().toString

  private def logDebug(event: String, fields: (String, Any)*): Unit = {
    if (!isDebugLoggingEnabled) {
      return
    }

    val suffix = if (fields.isEmpty) {
      ""
    } else {
      fields.map {
        case (key, value) =>
          val renderedValue = if (value == null) "null" else value.toString
          s"$key=$renderedValue"
      }.mkString(" ", " ", "")
    }

    System.err.println(s"[PrivySpark][DEBUG][${currentDebugTimestamp()}] $event$suffix")
  }

  private def logFallback(scope: String, expressionCount: Int, reason: String): Unit = {
    System.err.println(s"[PrivySpark] detection_aggregation_fallback scope=$scope expressions=$expressionCount reason=$reason")
  }

  def aggregate(sampledDf: DataFrame, rules: Seq[PiiRule]): Seq[MatchCount] = {
    aggregate(sampledDf, rules, AggregationConfig())
  }

  private[privyspark] def aggregate(
    sampledDf: DataFrame,
    rules: Seq[PiiRule],
    config: AggregationConfig
  ): Seq[MatchCount] = {
    val columns = sampledDf.columns.toSeq
    logDebug("detection_aggregation_start", "scope" -> "dataset", "columns" -> columns.size, "rules" -> rules.size)
    if (columns.isEmpty || rules.isEmpty) {
      logDebug("detection_aggregation_complete", "scope" -> "dataset", "metrics" -> 0, "results" -> 0, "mode" -> "noop")
      return Seq.empty
    }

    require(config.maxExpressionsPerAgg > 0, "maxExpressionsPerAgg must be > 0")
    require(config.legacyFallbackThreshold > 0, "legacyFallbackThreshold must be > 0")

    val metrics = buildMetrics(columns, rules)
    if (metrics.isEmpty) {
      logDebug("detection_aggregation_complete", "scope" -> "dataset", "metrics" -> 0, "results" -> 0, "mode" -> "noop")
      return Seq.empty
    }
    val expressionCount = totalExpressionCount(metrics)
    logDebug("detection_aggregation_metrics_built", "scope" -> "dataset", "metrics" -> metrics.size, "expressions" -> expressionCount)

    if (expressionCount > config.legacyFallbackThreshold) {
      val fallback = executeThresholdFallback(
        "dataset",
        expressionCount,
        config.legacyFallbackThreshold,
        aggregateLegacy(sampledDf, metrics),
        aggregateSafeLegacy(sampledDf, metrics)
      )
      logDebug(
        "detection_aggregation_complete",
        "scope" -> "dataset",
        "metrics" -> metrics.size,
        "results" -> fallback._1.size,
        "mode" -> fallback._2
      )
      return fallback._1
    }

    try {
      val results = aggregateInBatches(sampledDf, metrics, config.maxExpressionsPerAgg)
      logDebug(
        "detection_aggregation_complete",
        "scope" -> "dataset",
        "metrics" -> metrics.size,
        "results" -> results.size,
        "mode" -> "batched_agg"
      )
      results
    } catch {
      case NonFatal(e) =>
        logFallback("dataset", expressionCount, Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
        val results = aggregateSafeLegacy(sampledDf, metrics)
        logDebug(
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
    rules: Seq[PiiRule]
  ): Seq[FileMatchCount] = {
    aggregateByFile(sampledDf, fileIdentifierColumn, rules, AggregationConfig())
  }

  private[privyspark] def aggregateByFile(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    rules: Seq[PiiRule],
    config: AggregationConfig
  ): Seq[FileMatchCount] = {
    require(fileIdentifierColumn.nonEmpty, "fileIdentifierColumn must not be empty")

    val columns = sampledDf.columns.toSeq.filterNot(_ == fileIdentifierColumn)
    logDebug("detection_aggregation_start", "scope" -> "file", "columns" -> columns.size, "rules" -> rules.size)
    if (columns.isEmpty || rules.isEmpty) {
      logDebug("detection_aggregation_complete", "scope" -> "file", "metrics" -> 0, "results" -> 0, "mode" -> "noop")
      return Seq.empty
    }

    require(config.maxExpressionsPerAgg > 0, "maxExpressionsPerAgg must be > 0")
    require(config.legacyFallbackThreshold > 0, "legacyFallbackThreshold must be > 0")

    val metrics = buildMetrics(columns, rules)
    if (metrics.isEmpty) {
      logDebug("detection_aggregation_complete", "scope" -> "file", "metrics" -> 0, "results" -> 0, "mode" -> "noop")
      return Seq.empty
    }
    val expressionCount = totalExpressionCount(metrics)
    logDebug("detection_aggregation_metrics_built", "scope" -> "file", "metrics" -> metrics.size, "expressions" -> expressionCount)

    if (expressionCount > config.legacyFallbackThreshold) {
      val fallback = executeThresholdFallback(
        "file",
        expressionCount,
        config.legacyFallbackThreshold,
        aggregateByFileLegacy(sampledDf, fileIdentifierColumn, metrics),
        aggregateByFileSafeLegacy(sampledDf, fileIdentifierColumn, metrics)
      )
      logDebug(
        "detection_aggregation_complete",
        "scope" -> "file",
        "metrics" -> metrics.size,
        "results" -> fallback._1.size,
        "mode" -> fallback._2
      )
      return fallback._1
    }

    try {
      val results = aggregateByFileInBatches(sampledDf, fileIdentifierColumn, metrics, config.maxExpressionsPerAgg)
      logDebug(
        "detection_aggregation_complete",
        "scope" -> "file",
        "metrics" -> metrics.size,
        "results" -> results.size,
        "mode" -> "batched_agg"
      )
      results
    } catch {
      case NonFatal(e) =>
        logFallback("file", expressionCount, Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
        val results = aggregateByFileSafeLegacy(sampledDf, fileIdentifierColumn, metrics)
        logDebug(
          "detection_aggregation_complete",
          "scope" -> "file",
          "metrics" -> metrics.size,
          "results" -> results.size,
          "mode" -> "legacy_fallback"
        )
        results
    }
  }

  private def buildMetrics(columns: Seq[String], rules: Seq[PiiRule]): Seq[Metric] = {
    columns.zipWithIndex.flatMap {
      case (columnName, columnIndex) =>
        val normalizedColumnName = columnName.toLowerCase
        rules.zipWithIndex.flatMap {
          case (rule, ruleIndex) =>
            val shouldTestColumn =
              rule.columnHints.isEmpty || rule.columnHints.exists(hint => normalizedColumnName.contains(hint.toLowerCase))

            if (shouldTestColumn) {
              val alias = s"m_${columnIndex}_${ruleIndex}"
              val valueColumn = col(columnName).cast(StringType)
              val presentValuePredicate = valueColumn.isNotNull && trim(valueColumn) =!= ""
              val validatorPredicate = builtInValidatorPredicate(rule, valueColumn)
              val matchPredicate = rule.matchType match {
                case PiiRuleMatchType.FullColumn => valueColumn.rlike(fullMatchRegex(rule.regex)) && validatorPredicate
                case _ => valueColumn.rlike(rule.regex) && validatorPredicate
              }
              val predicate = rule.matchType match {
                case PiiRuleMatchType.FullColumn => presentValuePredicate && matchPredicate
                case _ => valueColumn.isNotNull && matchPredicate
              }
              Some(
                Metric(
                  alias = alias,
                  columnName = columnName,
                  piiType = rule.piiType,
                  matchType = rule.matchType,
                  predicate = predicate
                )
              )
            } else {
              None
            }
        }
    }
  }

  private def buildExpressions(batch: Seq[Metric]): Seq[Column] = {
    batch.map { metric =>
      sparkSum(when(metric.predicate, lit(1L)).otherwise(lit(0L))).cast("long").as(metric.alias)
    }
  }

  private def aggregateInBatches(
    sampledDf: DataFrame,
    metrics: Seq[Metric],
    maxExpressionsPerAgg: Int
  ): Seq[MatchCount] = {
    groupMetricsByExpressionBudget(metrics, maxExpressionsPerAgg).flatMap { batch =>
      val expressions = buildExpressions(batch)

      val row = sampledDf.agg(expressions.head, expressions.tail: _*).head()

      batch.zipWithIndex.flatMap {
        case (metric, index) =>
          val count = if (row.isNullAt(index)) 0L else row.getLong(index)
          if (count > 0L) Some(MatchCount(metric.columnName, metric.piiType, count)) else None
      }
    }
  }

  private def aggregateLegacy(sampledDf: DataFrame, metrics: Seq[Metric]): Seq[MatchCount] = {
    aggregateInBatches(sampledDf, metrics, LegacyFallbackBatchSize)
  }

  private[privyspark] def executeThresholdFallback[T](
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

  private def aggregateSafeLegacy(sampledDf: DataFrame, metrics: Seq[Metric]): Seq[MatchCount] = {
    metrics.flatMap { metric =>
      val count = sampledDf.filter(metric.predicate).count()
      if (count > 0L) Some(MatchCount(metric.columnName, metric.piiType, count)) else None
    }
  }

  private def aggregateByFileInBatches(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    metrics: Seq[Metric],
    maxExpressionsPerAgg: Int
  ): Seq[FileMatchCount] = {
    groupMetricsByExpressionBudget(metrics, maxExpressionsPerAgg).flatMap { batch =>
      val expressions = buildExpressions(batch)
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
              if (count > 0L) Some(FileMatchCount(fileIdentifier, metric.columnName, metric.piiType, count)) else None
          }
        }
      }
    }
  }

  private def aggregateByFileLegacy(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    metrics: Seq[Metric]
  ): Seq[FileMatchCount] = {
    aggregateByFileInBatches(sampledDf, fileIdentifierColumn, metrics, LegacyFallbackBatchSize)
  }

  private def aggregateByFileSafeLegacy(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    metrics: Seq[Metric]
  ): Seq[FileMatchCount] = {
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
          Some(FileMatchCount(fileIdentifier, metric.columnName, metric.piiType, count))
        }
      }
    }
  }

  private def totalExpressionCount(metrics: Seq[Metric]): Int = {
    metrics.map(_.expressionCount).sum
  }

  private def fullMatchRegex(regex: String): String = {
    s"\\A(?:$regex)\\z"
  }

  private def builtInValidatorPredicate(rule: PiiRule, valueColumn: Column): Column = {
    rule.piiType match {
      case "driver_license_number" => DriverLicenseValidatorUdf(valueColumn)
      case _ => lit(true)
    }
  }

  private def groupMetricsByExpressionBudget(metrics: Seq[Metric], maxExpressionsPerAgg: Int): Seq[Seq[Metric]] = {
    val batches = scala.collection.mutable.ArrayBuffer.empty[Seq[Metric]]
    val currentBatch = scala.collection.mutable.ArrayBuffer.empty[Metric]
    var currentExpressionCount = 0

    metrics.foreach { metric =>
      val metricExpressionCount = metric.expressionCount
      if (currentBatch.nonEmpty && currentExpressionCount + metricExpressionCount > maxExpressionsPerAgg) {
        batches += currentBatch.toVector
        currentBatch.clear()
        currentExpressionCount = 0
      }

      currentBatch += metric
      currentExpressionCount += metricExpressionCount
    }

    if (currentBatch.nonEmpty) {
      batches += currentBatch.toVector
    }

    batches.toSeq
  }
}
