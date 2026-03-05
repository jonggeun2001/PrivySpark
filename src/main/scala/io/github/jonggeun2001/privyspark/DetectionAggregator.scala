package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.model.PiiRule
import org.apache.spark.sql.functions.{col, lit, sum => sparkSum, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame}

import scala.util.control.NonFatal

object DetectionAggregator {
  final case class MatchCount(columnName: String, piiType: String, count: Long)
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

  private def aggregateInBatches(
    sampledDf: DataFrame,
    metrics: Seq[Metric],
    maxExpressionsPerAgg: Int
  ): Seq[MatchCount] = {
    metrics.grouped(maxExpressionsPerAgg).toSeq.flatMap { batch =>
      val expressions = batch.map { metric =>
        sparkSum(when(metric.predicate, lit(1L)).otherwise(lit(0L))).cast("long").as(metric.alias)
      }

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
}
