package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.model.PiiRule
import org.apache.spark.sql.functions.{col, lit, sum => sparkSum, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame}

object DetectionAggregator {
  final case class MatchCount(columnName: String, piiType: String, count: Long)

  private final case class Metric(alias: String, columnName: String, piiType: String, expr: Column)

  // Aggregate all (column, rule) match counts in a single DataFrame pass.
  def aggregate(sampledDf: DataFrame, rules: Seq[PiiRule]): Seq[MatchCount] = {
    val columns = sampledDf.columns.toSeq
    if (columns.isEmpty || rules.isEmpty) {
      return Seq.empty
    }

    val metrics = columns.zipWithIndex.flatMap {
      case (columnName, columnIndex) =>
        rules.zipWithIndex.map {
          case (rule, ruleIndex) =>
            val alias = s"m_${columnIndex}_${ruleIndex}"
            val valueColumn = col(columnName).cast(StringType)
            val matchExpr = sparkSum(
              when(valueColumn.isNotNull && valueColumn.rlike(rule.regex), lit(1L))
                .otherwise(lit(0L))
            ).cast("long").as(alias)

            Metric(alias = alias, columnName = columnName, piiType = rule.piiType, expr = matchExpr)
        }
    }

    val aggregateDf = sampledDf.agg(metrics.head.expr, metrics.tail.map(_.expr): _*)
    val row = aggregateDf.head()

    metrics.zipWithIndex.flatMap {
      case (metric, index) =>
        val count = if (row.isNullAt(index)) 0L else row.getLong(index)
        if (count > 0L) Some(MatchCount(metric.columnName, metric.piiType, count)) else None
    }
  }
}
