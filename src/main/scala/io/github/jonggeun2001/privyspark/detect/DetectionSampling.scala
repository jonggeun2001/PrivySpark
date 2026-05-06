package io.github.jonggeun2001.privyspark.detect

import io.github.jonggeun2001.privyspark.model.{PiiRuleMatchType, SampleValue}
import io.github.jonggeun2001.privyspark.util.DriverLogger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame

private[privyspark] object DetectionSampling {
  def collectSampleRawValues(
    sampledDf: DataFrame,
    metrics: Seq[DetectionAggregator.Metric],
    maxExpressionsPerAgg: Int
  ): Map[String, String] = {
    DetectionMetrics.groupMetricsByExpressionBudget(metrics, maxExpressionsPerAgg).foldLeft(Map.empty[String, String]) { (acc, batch) =>
      val expressions = DetectionExpressions.buildSampleExpressions(batch)
      val row = sampledDf.agg(expressions.head, expressions.tail: _*).head()
      val batchValues = batch.zipWithIndex.flatMap {
        case (metric, index) =>
          Option(row.getAs[String](index)).map(value => metric.alias -> value)
      }
      acc ++ batchValues
    }
  }

  def collectSampleRawValuesSafely(
    sampledDf: DataFrame,
    metrics: Seq[DetectionAggregator.Metric]
  ): Map[String, String] = {
    DetectionMetrics.groupMetricsByExpressionBudget(metrics, DetectionAggregator.SafeSampleFallbackBatchSize)
      .foldLeft(Map.empty[String, String]) { (acc, batch) =>
        val expressions = DetectionExpressions.buildSampleProjectionExpressions(batch)
        val aliases = batch.map(_.alias).toArray
        val batchValues = sampledDf
          .select(expressions: _*)
          .rdd
          .mapPartitions { rows =>
            val partitionValues = scala.collection.mutable.LinkedHashMap.empty[String, String]

            while (rows.hasNext && partitionValues.size < aliases.length) {
              val row = rows.next()
              var index = 0
              while (index < aliases.length && partitionValues.size < aliases.length) {
                val alias = aliases(index)
                if (!partitionValues.contains(alias) && !row.isNullAt(index)) {
                  partitionValues += alias -> row.getString(index)
                }
                index += 1
              }
            }

            Iterator.single(partitionValues.toMap)
          }
          .collect()

        acc ++ mergeFirstSeenValues(batchValues)
      }
  }

  def collectSampleRawValuesByFile(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    metrics: Seq[DetectionAggregator.Metric],
    maxExpressionsPerAgg: Int
  ): Map[(String, String), String] = {
    DetectionAggregator.faultInjector.beforeFileSampleCollection()

    DetectionMetrics.groupMetricsByExpressionBudget(metrics, maxExpressionsPerAgg)
      .foldLeft(Map.empty[(String, String), String]) { (acc, batch) =>
        val expressions = DetectionExpressions.buildSampleExpressions(batch)
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

  def collectSampleRawValuesByFileSafely(
    sampledDf: DataFrame,
    fileIdentifierColumn: String,
    metrics: Seq[DetectionAggregator.Metric]
  ): Map[(String, String), String] = {
    DetectionMetrics.groupMetricsByExpressionBudget(metrics, DetectionAggregator.SafeSampleFallbackBatchSize)
      .foldLeft(Map.empty[(String, String), String]) { (acc, batch) =>
        val projectedExpressions = DetectionExpressions.buildSampleProjectionExpressions(batch)
        val firstExpressions = DetectionExpressions.buildProjectedSampleFirstExpressions(batch)
        val groupedRows = sampledDf
          .select((col(fileIdentifierColumn) +: projectedExpressions): _*)
          .groupBy(col(fileIdentifierColumn))
          .agg(firstExpressions.head, firstExpressions.tail: _*)
          .collect()
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

  def buildSampleValuesByAlias(
    metrics: Seq[DetectionAggregator.Metric],
    rawValuesByAlias: Map[String, String]
  ): Map[String, SampleValue] = {
    metrics.flatMap { metric =>
      rawValuesByAlias
        .get(metric.alias)
        .flatMap(rawValue => sampleValue(metric, rawValue).map(metric.metricKey -> _))
    }.toMap
  }

  def buildSampleValuesByFileAlias(
    metrics: Seq[DetectionAggregator.Metric],
    rawValuesByFileAlias: Map[(String, String), String],
    requestedKeys: Set[(String, String)]
  ): Map[(String, String), SampleValue] = {
    val metricsByAlias = metrics.map(metric => metric.alias -> metric).toMap

    rawValuesByFileAlias.flatMap {
      case ((fileIdentifier, alias), rawValue) =>
        metricsByAlias
          .get(alias)
          .flatMap { metric =>
            val key = (fileIdentifier, metric.metricKey)
            if (requestedKeys.isEmpty || requestedKeys.contains(key)) {
              sampleValue(metric, rawValue).map(key -> _)
            } else {
              None
            }
          }
    }
  }

  def mergeFirstSeenValues[K](batchValues: Seq[Map[K, String]]): Map[K, String] = {
    batchValues.foldLeft(Map.empty[K, String]) { (acc, current) =>
      current.foldLeft(acc) {
        case (innerAcc, (key, value)) =>
          if (innerAcc.contains(key)) innerAcc else innerAcc + (key -> value)
      }
    }
  }

  def sampleValue(
    metric: DetectionAggregator.Metric,
    rawValue: String
  ): Option[SampleValue] = {
    extractMatch(rawValue, metric).map { extracted =>
      SampleValue(
        sampleRawValue = buildRawSnippet(rawValue, extracted.start, extracted.end),
        sampleMatchedFragment = extracted.fragment
      )
    }
  }

  def extractMatch(
    rawValue: String,
    metric: DetectionAggregator.Metric
  ): Option[DetectionAggregator.ExtractedMatch] = {
    val matcher = metric.pattern.matcher(rawValue)
    if (metric.matchType == PiiRuleMatchType.FullColumn) {
      if (matcher.matches()) Some(DetectionAggregator.ExtractedMatch(matcher.group(), 0, rawValue.length)) else None
    } else if (matcher.find()) {
      Some(DetectionAggregator.ExtractedMatch(matcher.group(), matcher.start(), matcher.end()))
    } else {
      None
    }
  }

  def buildRawSnippet(rawValue: String, start: Int, end: Int): String = {
    val snippetStart = math.max(0, start - 50)
    val snippetEnd = math.min(rawValue.length, end + 50)
    if (snippetStart == 0 && snippetEnd == rawValue.length && rawValue.length > 100) {
      rawValue.take(50) + "..." + rawValue.takeRight(50)
    } else {
      rawValue.substring(snippetStart, snippetEnd)
    }
  }

  private def logSampleConflict(scope: String, keys: Seq[String]): Unit = {
    DriverLogger.warn(
      "detection_sample_conflict",
      "scope" -> scope,
      "keys" -> keys.mkString(",")
    )
  }
}
