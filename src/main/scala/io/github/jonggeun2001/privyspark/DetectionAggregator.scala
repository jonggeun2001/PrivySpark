package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.model.PiiRule
import org.apache.spark.sql.functions.{col, lit, sum => sparkSum, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame}

import scala.util.control.NonFatal

object DetectionAggregator {
  final case class MatchCount(columnName: String, piiType: String, count: Long)
  final case class FileMatchCount(fileIdentifier: String, columnName: String, piiType: String, count: Long)
  final case class AggregationConfig(maxExpressionsPerAgg: Int = 400, legacyFallbackThreshold: Int = 10000)

  private final case class Metric(alias: String, columnName: String, piiType: String, predicate: Column)

  def aggregate(sampledDf: DataFrame, rules: Seq[PiiRule]): Seq[MatchCount] = {
    aggregate(sampledDf, rules, AggregationConfig())
  }

  private[privyspark] def aggregate(
    sampledDf: DataFrame,
    rules: Seq[PiiRule],
    config: AggregationConfig
  ): Seq[MatchCount] = {
    val columns = sampledDf.columns.toSeq
    if (columns.isEmpty || rules.isEmpty) {
      return Seq.empty
    }

    require(config.maxExpressionsPerAgg > 0, "maxExpressionsPerAgg must be > 0")
    require(config.legacyFallbackThreshold > 0, "legacyFallbackThreshold must be > 0")

    val metrics = buildMetrics(columns, rules)
    if (metrics.isEmpty) {
      return Seq.empty
    }

    if (metrics.size > config.legacyFallbackThreshold) {
      return aggregateLegacy(sampledDf, metrics)
    }

    try {
      aggregateInBatches(sampledDf, metrics, config.maxExpressionsPerAgg)
    } catch {
      case NonFatal(_) =>
        aggregateLegacy(sampledDf, metrics)
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
    if (columns.isEmpty || rules.isEmpty) {
      return Seq.empty
    }

    require(config.maxExpressionsPerAgg > 0, "maxExpressionsPerAgg must be > 0")
    require(config.legacyFallbackThreshold > 0, "legacyFallbackThreshold must be > 0")

    val metrics = buildMetrics(columns, rules)
    if (metrics.isEmpty) {
      return Seq.empty
    }

    if (metrics.size > config.legacyFallbackThreshold) {
      return aggregateByFileLegacy(sampledDf, fileIdentifierColumn, metrics)
    }

    try {
      aggregateByFileInBatches(sampledDf, fileIdentifierColumn, metrics, config.maxExpressionsPerAgg)
    } catch {
      case NonFatal(_) =>
        aggregateByFileLegacy(sampledDf, fileIdentifierColumn, metrics)
    }
  }

  private def buildMetrics(columns: Seq[String], rules: Seq[PiiRule]): Seq[Metric] = {
    columns.zipWithIndex.flatMap {
      case (columnName, columnIndex) =>
        rules.zipWithIndex.map {
          case (rule, ruleIndex) =>
            val alias = s"m_${columnIndex}_${ruleIndex}"
            val valueColumn = col(columnName).cast(StringType)
            val predicate = valueColumn.isNotNull && valueColumn.rlike(rule.regex)

            Metric(alias = alias, columnName = columnName, piiType = rule.piiType, predicate = predicate)
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
    metrics.grouped(maxExpressionsPerAgg).toSeq.flatMap { batch =>
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
    metrics.grouped(maxExpressionsPerAgg).toSeq.flatMap { batch =>
      val expressions = buildExpressions(batch)
      val groupedRows = sampledDf.groupBy(col(fileIdentifierColumn)).agg(expressions.head, expressions.tail: _*).collect()

      groupedRows.flatMap { row =>
        val fileIdentifier = if (row.isNullAt(0)) null else row.getString(0)
        if (fileIdentifier == null || fileIdentifier.isEmpty) {
          Seq.empty
        } else {
          batch.zipWithIndex.flatMap {
            case (metric, index) =>
              val count = if (row.isNullAt(index + 1)) 0L else row.getLong(index + 1)
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
}
