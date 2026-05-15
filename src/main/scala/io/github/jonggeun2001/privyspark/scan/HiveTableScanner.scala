package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.config.SuppressionSet
import io.github.jonggeun2001.privyspark.detect.DetectionAggregator
import io.github.jonggeun2001.privyspark.model.{PiiRule, ScanError, ScanGroup, ScanResult}
import io.github.jonggeun2001.privyspark.review.AllowlistMatcher
import io.github.jonggeun2001.privyspark.util.DriverLogger
import io.github.jonggeun2001.privyspark.util.PathIdentifiers.{resolveDirectoryIdentifier, resolveRelativeIdentifier}
import org.apache.spark.sql.SparkSession

import scala.util.control.NonFatal

private[privyspark] object HiveTableScanner {
  private def reviewScopeFileIdentifiers(group: ScanGroup, datasetPath: String): Seq[String] =
    group.filePaths
      .map { sourceKey =>
        group.logicalIdentifiersByKey.getOrElse(
          sourceKey,
          resolveRelativeIdentifier(datasetPath, group.physicalPathsByKey.getOrElse(sourceKey, sourceKey))
        )
      }
      .map(value => Option(value).map(_.trim).getOrElse(""))
      .filter(_.nonEmpty)
      .distinct
      .sorted

  def scanHiveTable(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    suppressions: SuppressionSet = SuppressionSet.empty,
    allowlistMatcher: AllowlistMatcher = AllowlistMatcher.empty,
    allowlistInputRoot: Option[String] = None
  ): (Seq[ScanResult], Seq[ScanError]) = {
    val tableFqn = Option(group.hiveTableFqn).map(_.trim).getOrElse("")
    val fileIdentifier = resolveDirectoryIdentifier(datasetPath, group.directoryPath)
    if (tableFqn.isEmpty) {
      return (Seq.empty, Seq(ScanError(datasetPath, timestamp, fileIdentifier, "Hive table scan requires hive_table_fqn")))
    }

    DriverLogger.debug(
      "hive_table_scan_start",
      "directory" -> group.directoryPath,
      "hive_table_fqn" -> tableFqn,
      "sample_ratio" -> sampleRatio,
      "files" -> group.filePaths.size
    )

    try {
      val effectiveRules = ScanResultBuilder.effectiveRulesForFormat(group.format, rules)
      val baseDf = spark.table(tableFqn)
      val sampledDf = ScanResultBuilder.sampleRowsDeterministically(baseDf, sampleRatio)
      val sampledRowCount = sampledDf.count()

      val provisionalResults =
        if (sampledRowCount == 0L) {
          Seq.empty
        } else {
          val matchCounts = DetectionAggregator.aggregate(sampledDf, effectiveRules, suppressions = suppressions)
          val nonEmptyValueCounts = DetectionAggregator.countNonEmpty(
            sampledDf,
            DetectionAggregator.columnsCoveredByRules(sampledDf.columns.toSeq, effectiveRules, suppressions)
          )
          val sampleValues = DetectionAggregator.sampleMatches(sampledDf, effectiveRules, matchCounts, suppressions = suppressions)
          val fileSize = group.fileSizesByKey.values.sum
          val fileMtimeEpochMs = if (group.fileMtimesByKey.isEmpty) 0L else group.fileMtimesByKey.values.max

          ScanResultBuilder.buildScanResults(
            datasetPath,
            ScanResultBuilder.currentScanTimestamp(),
            fileIdentifier,
            sampledRowCount,
            nonEmptyValueCounts,
            matchCounts,
            sampleValues,
            fileSize,
            fileMtimeEpochMs,
            hiveTableFqn = tableFqn,
            reviewScopeFileIdentifiers = reviewScopeFileIdentifiers(group, datasetPath)
          )
        }

      val filteredResults = AllowlistApplier.applyAllowlist(
        spark.sparkContext.hadoopConfiguration,
        datasetPath,
        allowlistMatcher,
        allowlistInputRoot,
        provisionalResults
      )
      DriverLogger.debug(
        "hive_table_scan_complete",
        "directory" -> group.directoryPath,
        "hive_table_fqn" -> tableFqn,
        "sampled_rows" -> sampledRowCount,
        "result_rows" -> filteredResults.size
      )
      (filteredResults, Seq.empty)
    } catch {
      case NonFatal(e) =>
        val message = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        DriverLogger.warn(
          "hive_table_scan_error",
          "directory" -> group.directoryPath,
          "hive_table_fqn" -> tableFqn,
          "reason" -> message
        )
        (Seq.empty, Seq(ScanError(datasetPath, timestamp, fileIdentifier, message)))
    }
  }
}
