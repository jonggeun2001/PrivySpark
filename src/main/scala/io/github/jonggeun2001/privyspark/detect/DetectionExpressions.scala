package io.github.jonggeun2001.privyspark.detect

import io.github.jonggeun2001.privyspark.model.PiiRuleMatchType
import org.apache.spark.sql.functions.{col, first, lit, sum => sparkSum, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Column

import java.util.regex.Pattern

private[privyspark] object DetectionExpressions {
  def buildExpressions(batch: Seq[DetectionAggregator.Metric]): Seq[Column] = {
    batch.map { metric =>
      sparkSum(when(metric.predicate, lit(1L)).otherwise(lit(0L))).cast("long").as(metric.alias)
    }
  }

  def buildSampleExpressions(batch: Seq[DetectionAggregator.Metric]): Seq[Column] = {
    batch.map { metric =>
      first(when(metric.predicate, col(metric.columnName).cast(StringType)), ignoreNulls = true).as(metric.alias)
    }
  }

  def buildSampleProjectionExpressions(batch: Seq[DetectionAggregator.Metric]): Seq[Column] = {
    batch.map { metric =>
      when(metric.predicate, col(metric.columnName).cast(StringType)).as(metric.alias)
    }
  }

  def buildProjectedSampleFirstExpressions(batch: Seq[DetectionAggregator.Metric]): Seq[Column] = {
    batch.map { metric =>
      first(col(metric.alias), ignoreNulls = true).as(metric.alias)
    }
  }

  def fullMatchRegex(regex: String): String = {
    s"\\A(?:$regex)\\z"
  }

  def compiledPattern(regex: String, matchType: String): Pattern = {
    Pattern.compile(
      if (matchType == PiiRuleMatchType.FullColumn) fullMatchRegex(regex)
      else regex
    )
  }
}
