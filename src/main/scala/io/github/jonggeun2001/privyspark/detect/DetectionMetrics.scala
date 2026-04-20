package io.github.jonggeun2001.privyspark.detect

import io.github.jonggeun2001.privyspark.config.SuppressionSet
import io.github.jonggeun2001.privyspark.model.{PiiRule, PiiRuleMatchType}
import org.apache.spark.sql.functions.{col, trim}
import org.apache.spark.sql.types.StringType

private[privyspark] object DetectionMetrics {
  def buildMetrics(
    columns: Seq[String],
    rules: Seq[PiiRule],
    suppressions: SuppressionSet
  ): Seq[DetectionAggregator.Metric] = {
    columns.zipWithIndex.flatMap {
      case (columnName, columnIndex) =>
        val normalizedColumnName = SuppressionSet.normalizeColumnName(columnName)
        rules.zipWithIndex.flatMap {
          case (rule, ruleIndex) =>
            val passesHint =
              rule.columnHints.isEmpty || rule.columnHints.exists(hint => normalizedColumnName.contains(SuppressionSet.normalizeColumnName(hint)))
            val notSuppressed = !suppressions.isSuppressed(normalizedColumnName, rule.piiType)
            val shouldTestColumn = passesHint && notSuppressed

            if (shouldTestColumn) {
              val alias = s"m_${columnIndex}_${ruleIndex}"
              val valueColumn = col(columnName).cast(StringType)
              val presentValuePredicate = valueColumn.isNotNull && trim(valueColumn) =!= ""
              val pattern = DetectionExpressions.compiledPattern(rule.regex, rule.matchType)
              val matchPredicate = rule.matchType match {
                case PiiRuleMatchType.FullColumn => valueColumn.rlike(DetectionExpressions.fullMatchRegex(rule.regex))
                case _ => valueColumn.rlike(rule.regex)
              }
              Some(
                DetectionAggregator.Metric(
                  alias = alias,
                  metricKey = stableMetricKey(columnName, ruleIndex),
                  columnName = columnName,
                  piiType = rule.piiType,
                  regex = rule.regex,
                  matchType = rule.matchType,
                  pattern = pattern,
                  predicate = presentValuePredicate && matchPredicate
                )
              )
            } else {
              None
            }
        }
    }
  }

  def groupMetricsByExpressionBudget(
    metrics: Seq[DetectionAggregator.Metric],
    maxExpressionsPerAgg: Int
  ): Seq[Seq[DetectionAggregator.Metric]] = {
    val batches = scala.collection.mutable.ArrayBuffer.empty[Seq[DetectionAggregator.Metric]]
    val currentBatch = scala.collection.mutable.ArrayBuffer.empty[DetectionAggregator.Metric]
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

  def totalExpressionCount(metrics: Seq[DetectionAggregator.Metric]): Int = {
    metrics.map(_.expressionCount).sum
  }

  def stableMetricKey(columnName: String, ruleIndex: Int): String = {
    s"$columnName#$ruleIndex"
  }
}
