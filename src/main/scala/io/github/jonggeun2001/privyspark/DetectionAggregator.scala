package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.model.{PiiRule, PiiRuleMatchType}
import org.apache.spark.sql.functions.{col, first, lit, sum => sparkSum, trim, udf, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame}

import java.util.regex.Pattern
import scala.util.control.NonFatal

object DetectionAggregator {
  final case class MatchCount(columnName: String, piiType: String, count: Long, metricAlias: String = "")
  final case class FileMatchCount(fileIdentifier: String, columnName: String, piiType: String, count: Long, metricAlias: String = "")
  final case class SampleValue(sampleRawValue: String, sampleMatchedFragment: String)
  final case class AggregationConfig(maxExpressionsPerAgg: Int = 400, legacyFallbackThreshold: Int = 50000)

  private final case class Metric(
    alias: String,
    columnName: String,
    piiType: String,
    regex: String,
    matchType: String,
    predicate: Column
  ) {
    val expressionCount: Int = 1
  }
  private final case class ExtractedMatch(fragment: String, start: Int, end: Int)
  private val LegacyFallbackBatchSize = 50
  private val DriverLicenseValidatorUdf = udf((value: String) => DriverLicenseNumberValidator.containsValidCandidate(value))

  private[privyspark] def resetDebugCache(): Unit = {
    DriverLogger.resetCache()
  }

  private def logDebug(event: String, fields: (String, Any)*): Unit = {
    DriverLogger.debug(event, fields: _*)
  }

  private def logFallback(scope: String, expressionCount: Int, reason: String): Unit = {
    DriverLogger.warn(
      "detection_aggregation_fallback",
      "scope" -> scope,
      "expressions" -> expressionCount,
      "reason" -> reason
    )
  }

  private def logSampleConflict(scope: String, keys: Seq[String]): Unit = {
    DriverLogger.warn(
      "detection_sample_conflict",
      "scope" -> scope,
      "keys" -> keys.mkString(",")
    )
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

  def countNonNull(
    sampledDf: DataFrame,
    columns: Seq[String]
  ): Map[String, Long] = {
    countNonNull(sampledDf, columns, AggregationConfig())
  }

  private[privyspark] def countNonNull(
    sampledDf: DataFrame,
    columns: Seq[String],
    config: AggregationConfig
  ): Map[String, Long] = {
    require(config.maxExpressionsPerAgg > 0, "maxExpressionsPerAgg must be > 0")

    val targetColumns = columns.distinct.filter(sampledDf.columns.contains)
    if (targetColumns.isEmpty) {
      Map.empty
    } else {
      targetColumns.grouped(config.maxExpressionsPerAgg).foldLeft(Map.empty[String, Long]) { (acc, batch) =>
        val expressions = batch.zipWithIndex.map {
          case (columnName, index) =>
            sparkSum(when(col(columnName).isNotNull, lit(1L)).otherwise(lit(0L))).cast("long").as(s"nn_$index")
        }
        val aggregated = sampledDf.agg(expressions.head, expressions.tail: _*).head()
        acc ++ batch.zipWithIndex.map {
          case (columnName, index) =>
            val count = if (aggregated.isNullAt(index)) 0L else aggregated.getLong(index)
            columnName -> count
        }
      }
    }
  }

  def countNonNullByFile(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    columns: Seq[String]
  ): Map[(String, String), Long] = {
    countNonNullByFile(sampledDf, fileIdentifierColumn, columns, AggregationConfig())
  }

  private[privyspark] def countNonNullByFile(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    columns: Seq[String],
    config: AggregationConfig
  ): Map[(String, String), Long] = {
    require(fileIdentifierColumn.nonEmpty, "fileIdentifierColumn must not be empty")
    require(config.maxExpressionsPerAgg > 0, "maxExpressionsPerAgg must be > 0")

    val targetColumns = columns.distinct.filter(columnName => columnName != fileIdentifierColumn && sampledDf.columns.contains(columnName))
    if (targetColumns.isEmpty) {
      Map.empty
    } else {
      targetColumns.grouped(config.maxExpressionsPerAgg).foldLeft(Map.empty[(String, String), Long]) { (acc, batch) =>
        val expressions = batch.zipWithIndex.map {
          case (columnName, index) =>
            sparkSum(when(col(columnName).isNotNull, lit(1L)).otherwise(lit(0L))).cast("long").as(s"nn_$index")
        }
        val batchCounts = sampledDf
          .groupBy(col(fileIdentifierColumn))
          .agg(expressions.head, expressions.tail: _*)
          .collect()
          .flatMap { row =>
            val fileIdentifier = if (row.isNullAt(0)) null else row.getString(0)
            if (fileIdentifier == null || fileIdentifier.isEmpty) {
              Seq.empty
            } else {
              batch.zipWithIndex.map {
                case (columnName, index) =>
                  val count = if (row.isNullAt(index + 1)) 0L else row.getLong(index + 1)
                  (fileIdentifier, columnName) -> count
              }
            }
          }

        acc ++ batchCounts
      }
    }
  }

  private[privyspark] def columnsCoveredByRules(
    columns: Seq[String],
    rules: Seq[PiiRule]
  ): Seq[String] = {
    buildMetrics(columns, rules).map(_.columnName).distinct
  }

  def sampleMatches(
    sampledDf: DataFrame,
    rules: Seq[PiiRule],
    matchCounts: Seq[MatchCount]
  ): Map[String, SampleValue] = {
    sampleMatches(sampledDf, rules, matchCounts, AggregationConfig())
  }

  private[privyspark] def sampleMatches(
    sampledDf: DataFrame,
    rules: Seq[PiiRule],
    matchCounts: Seq[MatchCount],
    config: AggregationConfig
  ): Map[String, SampleValue] = {
    require(config.maxExpressionsPerAgg > 0, "maxExpressionsPerAgg must be > 0")
    require(config.legacyFallbackThreshold > 0, "legacyFallbackThreshold must be > 0")

    if (matchCounts.isEmpty) {
      Map.empty
    } else {
      val requestedKeys = matchCounts.map(matchCount => (matchCount.columnName, matchCount.piiType)).toSet
      val requestedAliases = matchCounts.map(_.metricAlias).filter(_.nonEmpty).toSet
      val metrics = buildMetrics(sampledDf.columns.toSeq, rules).filter { metric =>
        if (requestedAliases.nonEmpty) {
          requestedAliases.contains(metric.alias)
        } else {
          requestedKeys.contains((metric.columnName, metric.piiType))
        }
      }
      val expressionCount = totalExpressionCount(metrics)
      val rawValues =
        if (expressionCount > config.legacyFallbackThreshold) {
          executeThresholdFallback(
            "dataset_sample",
            expressionCount,
            config.legacyFallbackThreshold,
            collectSampleRawValues(sampledDf, metrics, LegacyFallbackBatchSize).toSeq,
            collectSampleRawValuesSafely(sampledDf, metrics).toSeq
          )._1.toMap
        } else {
          try {
            collectSampleRawValues(sampledDf, metrics, config.maxExpressionsPerAgg)
          } catch {
            case NonFatal(e) =>
              logFallback("dataset_sample", expressionCount, Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
            collectSampleRawValuesSafely(sampledDf, metrics)
          }
        }

      buildSampleValuesByAlias(metrics, rawValues)
    }
  }

  def sampleMatchesByFile(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    rules: Seq[PiiRule],
    matchCounts: Seq[FileMatchCount]
  ): Map[(String, String), SampleValue] = {
    sampleMatchesByFile(sampledDf, fileIdentifierColumn, rules, matchCounts, AggregationConfig())
  }

  private[privyspark] def sampleMatchesByFile(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    rules: Seq[PiiRule],
    matchCounts: Seq[FileMatchCount],
    config: AggregationConfig
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
      val metrics = buildMetrics(sampledDf.columns.toSeq.filterNot(_ == fileIdentifierColumn), rules)
        .filter { metric =>
          if (requestedMetricAliases.nonEmpty) {
            requestedMetricAliases.contains(metric.alias)
          } else {
            requestedMetricKeys.contains((metric.columnName, metric.piiType))
          }
        }
      val expressionCount = totalExpressionCount(metrics)
      val rawValues =
        if (expressionCount > config.legacyFallbackThreshold) {
          executeThresholdFallback(
            "file_sample",
            expressionCount,
            config.legacyFallbackThreshold,
            collectSampleRawValuesByFile(sampledDf, fileIdentifierColumn, metrics, LegacyFallbackBatchSize).toSeq,
            collectSampleRawValuesByFileSafely(sampledDf, fileIdentifierColumn, metrics).toSeq
          )._1.toMap
        } else {
          try {
            collectSampleRawValuesByFile(sampledDf, fileIdentifierColumn, metrics, config.maxExpressionsPerAgg)
          } catch {
            case NonFatal(e) =>
              logFallback("file_sample", expressionCount, Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
            collectSampleRawValuesByFileSafely(sampledDf, fileIdentifierColumn, metrics)
          }
        }

      buildSampleValuesByFileAlias(metrics, rawValues, requestedAliases)
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
                  regex = rule.regex,
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

  private def buildSampleExpressions(batch: Seq[Metric]): Seq[Column] = {
    batch.map { metric =>
      first(when(metric.predicate, col(metric.columnName).cast(StringType)), ignoreNulls = true).as(metric.alias)
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
          if (count > 0L) Some(MatchCount(metric.columnName, metric.piiType, count, metric.alias)) else None
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
      if (count > 0L) Some(MatchCount(metric.columnName, metric.piiType, count, metric.alias)) else None
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
              if (count > 0L) Some(FileMatchCount(fileIdentifier, metric.columnName, metric.piiType, count, metric.alias)) else None
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
          Some(FileMatchCount(fileIdentifier, metric.columnName, metric.piiType, count, metric.alias))
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

  private def collectSampleRawValues(
    sampledDf: DataFrame,
    metrics: Seq[Metric],
    maxExpressionsPerAgg: Int
  ): Map[String, String] = {
    groupMetricsByExpressionBudget(metrics, maxExpressionsPerAgg).foldLeft(Map.empty[String, String]) { (acc, batch) =>
      val expressions = buildSampleExpressions(batch)
      val row = sampledDf.agg(expressions.head, expressions.tail: _*).head()
      val batchValues = batch.zipWithIndex.flatMap {
        case (metric, index) =>
          Option(row.getAs[String](index)).map(value => metric.alias -> value)
      }
      acc ++ batchValues
    }
  }

  private def collectSampleRawValuesSafely(
    sampledDf: DataFrame,
    metrics: Seq[Metric]
  ): Map[String, String] = {
    metrics.flatMap { metric =>
      sampledDf
        .filter(metric.predicate)
        .select(col(metric.columnName).cast(StringType))
        .limit(1)
        .collect()
        .headOption
        .flatMap(row => Option(row.getAs[String](0)))
        .map(value => metric.alias -> value)
    }.toMap
  }

  private def collectSampleRawValuesByFile(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    metrics: Seq[Metric],
    maxExpressionsPerAgg: Int
  ): Map[(String, String), String] = {
    groupMetricsByExpressionBudget(metrics, maxExpressionsPerAgg).foldLeft(Map.empty[(String, String), String]) { (acc, batch) =>
      val expressions = buildSampleExpressions(batch)
      val groupedRows = sampledDf.groupBy(col(fileIdentifierColumn)).agg(expressions.head, expressions.tail: _*).collect()
      val batchValues = groupedRows.flatMap { row =>
        Option(row.getAs[String](0)).filter(_.nonEmpty).toSeq.flatMap { fileIdentifier =>
          batch.zipWithIndex.flatMap {
            case (metric, batchIndex) =>
              Option(row.getAs[String](batchIndex + 1)).map(value => (fileIdentifier, metric.alias) -> value)
          }
        }
      }
      acc ++ batchValues
    }
  }

  private def collectSampleRawValuesByFileSafely(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    metrics: Seq[Metric]
  ): Map[(String, String), String] = {
    metrics.flatMap { metric =>
      sampledDf
        .filter(metric.predicate)
        .groupBy(col(fileIdentifierColumn))
        .agg(first(col(metric.columnName).cast(StringType), ignoreNulls = true).as(metric.alias))
        .collect()
        .flatMap { row =>
          Option(row.getAs[String](0)).filter(_.nonEmpty).flatMap { fileIdentifier =>
            Option(row.getAs[String](1)).map(value => (fileIdentifier, metric.alias) -> value)
          }
        }
    }.toMap
  }

  private def buildSampleValuesByAlias(
    metrics: Seq[Metric],
    rawValuesByAlias: Map[String, String]
  ): Map[String, SampleValue] = {
    metrics.flatMap { metric =>
      rawValuesByAlias
        .get(metric.alias)
        .flatMap(rawValue => sampleValue(metric, rawValue).map(metric.alias -> _))
    }.toMap
  }

  private def buildSampleValuesByFileAlias(
    metrics: Seq[Metric],
    rawValuesByFileAlias: Map[(String, String), String],
    requestedKeys: Set[(String, String)]
  ): Map[(String, String), SampleValue] = {
    val metricsByAlias = metrics.map(metric => metric.alias -> metric).toMap

    rawValuesByFileAlias.flatMap {
      case (key @ (_, alias), rawValue) if requestedKeys.isEmpty || requestedKeys.contains(key) =>
        metricsByAlias
          .get(alias)
          .flatMap(metric => sampleValue(metric, rawValue).map(key -> _))
      case _ =>
        None
    }
  }

  private def sampleValue(
    metric: Metric,
    rawValue: String
  ): Option[SampleValue] = {
    extractMatch(rawValue, metric).map { extracted =>
      SampleValue(
        sampleRawValue = buildRawSnippet(rawValue, extracted.start, extracted.end),
        sampleMatchedFragment = extracted.fragment
      )
    }
  }

  private def extractMatch(rawValue: String, metric: Metric): Option[ExtractedMatch] = {
    if (metric.piiType == "driver_license_number") {
      DriverLicenseNumberValidator
        .findFirstValidCandidate(rawValue)
        .map(candidate => ExtractedMatch(candidate.candidate, candidate.start, candidate.end))
    } else {
      val pattern = Pattern.compile(
        if (metric.matchType == PiiRuleMatchType.FullColumn) fullMatchRegex(metric.regex)
        else metric.regex
      )
      val matcher = pattern.matcher(rawValue)
      if (metric.matchType == PiiRuleMatchType.FullColumn) {
        if (matcher.matches()) Some(ExtractedMatch(matcher.group(), 0, rawValue.length)) else None
      } else if (matcher.find()) {
        Some(ExtractedMatch(matcher.group(), matcher.start(), matcher.end()))
      } else {
        None
      }
    }
  }

  private def buildRawSnippet(rawValue: String, start: Int, end: Int): String = {
    val snippetStart = math.max(0, start - 50)
    val snippetEnd = math.min(rawValue.length, end + 50)
    if (snippetStart == 0 && snippetEnd == rawValue.length && rawValue.length > 100) {
      rawValue.take(50) + "..." + rawValue.takeRight(50)
    } else {
      rawValue.substring(snippetStart, snippetEnd)
    }
  }
}
