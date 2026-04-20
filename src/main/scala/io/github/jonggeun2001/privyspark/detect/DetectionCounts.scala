package io.github.jonggeun2001.privyspark.detect

import io.github.jonggeun2001.privyspark.config.SuppressionSet
import io.github.jonggeun2001.privyspark.model.PiiRule
import org.apache.spark.sql.functions.{col, lit, sum => sparkSum, trim, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.DataFrame

private[privyspark] object DetectionCounts {
  def countNonEmpty(
    sampledDf: DataFrame,
    columns: Seq[String],
    config: DetectionAggregator.AggregationConfig
  ): Map[String, Long] = {
    require(config.maxExpressionsPerAgg > 0, "maxExpressionsPerAgg must be > 0")

    val targetColumns = columns.distinct.filter(sampledDf.columns.contains)
    if (targetColumns.isEmpty) {
      Map.empty
    } else {
      targetColumns.grouped(config.maxExpressionsPerAgg).foldLeft(Map.empty[String, Long]) { (acc, batch) =>
        val expressions = batch.zipWithIndex.map {
          case (columnName, index) =>
            val valueColumn = col(columnName).cast(StringType)
            val presentValuePredicate = valueColumn.isNotNull && trim(valueColumn) =!= ""
            sparkSum(when(presentValuePredicate, lit(1L)).otherwise(lit(0L))).cast("long").as(s"ne_$index")
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

  def countNonEmptyByFile(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    columns: Seq[String],
    config: DetectionAggregator.AggregationConfig
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
            val valueColumn = col(columnName).cast(StringType)
            val presentValuePredicate = valueColumn.isNotNull && trim(valueColumn) =!= ""
            sparkSum(when(presentValuePredicate, lit(1L)).otherwise(lit(0L))).cast("long").as(s"ne_$index")
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

  def columnsCoveredByRules(
    columns: Seq[String],
    rules: Seq[PiiRule],
    suppressions: SuppressionSet
  ): Seq[String] = {
    DetectionMetrics.buildMetrics(columns, rules, suppressions).map(_.columnName).distinct
  }
}
