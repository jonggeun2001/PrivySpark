package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.config.SuppressionSet
import io.github.jonggeun2001.privyspark.detect.DetectionAggregator
import io.github.jonggeun2001.privyspark.format.CsvInference.{readSource, resolveFileIdentifierColumn}
import io.github.jonggeun2001.privyspark.fsio.RetryIO.withFileReadRetry
import io.github.jonggeun2001.privyspark.model.{MatchCount, PiiRule, ScanGroup, ScanResult}
import io.github.jonggeun2001.privyspark.review.AllowlistMatcher
import io.github.jonggeun2001.privyspark.util.DriverLogger
import io.github.jonggeun2001.privyspark.util.PathIdentifiers._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, input_file_name}

private[privyspark] object GroupBatchScanner {
  def scanGroupBatch(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    fileSampleRatio: Option[Double] = None,
    fileSampleMinFiles: Int = 10,
    suppressions: SuppressionSet = SuppressionSet.empty,
    allowlistMatcher: AllowlistMatcher = AllowlistMatcher.empty,
    allowlistInputRoot: Option[String] = None,
    selectedSourceKeys: Option[Seq[String]] = None
  ): Seq[ScanResult] = {
    require(!group.useDirectoryIdentifier, "scanGroupBatch does not support directory identifiers; use scanGroupByFile")
    DriverLogger.debug(
      "group_scan_batch_start",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "files" -> group.filePaths.size,
      "sample_ratio" -> sampleRatio,
      "file_sample_ratio" -> fileSampleRatio.getOrElse("none"),
      "file_sample_min_files" -> fileSampleMinFiles,
      "use_directory_identifier" -> group.useDirectoryIdentifier
    )
    val effectiveSelectedSourceKeys =
      selectedSourceKeys.getOrElse(FileMetricsScanner.resolveSelectedFileKeys(group, sampleRatio, fileSampleRatio, fileSampleMinFiles))
    val fileSamplingApplied = effectiveSelectedSourceKeys.size < group.filePaths.size
    val hasDuplicateSelectedSourceKeys = effectiveSelectedSourceKeys.distinct.size != effectiveSelectedSourceKeys.size
    val physicalPaths = effectiveSelectedSourceKeys.map(sourceKey => resolvePhysicalPath(group, sourceKey))
    val effectiveSampleRatio = if (fileSamplingApplied) 1.0 else sampleRatio
    withFileReadRetry(spark, physicalPaths, "group_batch_scan") {
      val effectiveRules = ScanResultBuilder.effectiveRulesForFormat(group.format, rules)
      val baseDf = readSource(spark, group.format, physicalPaths, group.csvHasHeader)
      val fileIdentifierColumn = Some(resolveFileIdentifierColumn(baseDf.columns.toSeq))
      val sourceDf = baseDf.withColumn(fileIdentifierColumn.get, input_file_name())
      DriverLogger.debug(
        "group_scan_batch_source_ready",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "columns" -> sourceDf.columns.length,
        "file_identifier_mode" -> fileIdentifierColumn.get
      )

      val sampledDf = if (fileSamplingApplied) sourceDf else ScanResultBuilder.sampleRowsDeterministically(sourceDf, sampleRatio)
      val columnName = fileIdentifierColumn.get
      val sampledRowsByFile = sampledDf
        .groupBy(col(columnName))
        .count()
        .collect()
        .flatMap { row =>
          val fileIdentifier = if (row.isNullAt(0)) null else row.getString(0)
          val rowCount = if (row.isNullAt(1)) 0L else row.getLong(1)
          if (fileIdentifier == null || fileIdentifier.isEmpty || rowCount <= 0L) {
            None
          } else {
            Some(fileIdentifier -> rowCount)
          }
        }
        .toMap
      DriverLogger.debug(
        "group_scan_batch_sampled_file_rows",
        "directory" -> group.directoryPath,
        "files_with_rows" -> sampledRowsByFile.size,
        "mode" -> "file_identifier"
      )

      if (sampledRowsByFile.isEmpty) {
        DriverLogger.debug(
          "group_scan_batch_complete",
          "directory" -> group.directoryPath,
          "result_rows" -> 0,
          "mode" -> "file_identifier"
        )
        Seq.empty
      } else {
        val matchCountsByFile = DetectionAggregator.aggregateByFile(sampledDf, columnName, effectiveRules, suppressions = suppressions)
        val sampleValuesByFile = DetectionAggregator.sampleMatchesByFile(
          sampledDf,
          columnName,
          effectiveRules,
          matchCountsByFile,
          suppressions = suppressions
        )
        val nonEmptyCountsByFile = DetectionAggregator.countNonEmptyByFile(sampledDf, columnName, matchCountsByFile.map(_.columnName).distinct)
        val matchedSourceKeys = matchCountsByFile
          .map { matchCount =>
            resolveSourceKeyForPhysicalPath(group, matchCount.fileIdentifier).getOrElse {
              throw new IllegalStateException(s"Missing source key for batch match file: ${matchCount.fileIdentifier}")
            }
          }
          .distinct
        val batchFileIdentifierValuesBySourceKey = matchCountsByFile
          .flatMap { matchCount =>
            resolveSourceKeyForPhysicalPath(group, matchCount.fileIdentifier).map(_ -> matchCount.fileIdentifier)
          }
          .toMap
        val groupScanTimestamp = ScanResultBuilder.currentScanTimestamp()
        val provisionalResults = matchCountsByFile.flatMap { matchCount =>
          sampledRowsByFile.get(matchCount.fileIdentifier).flatMap { sampledRowCount =>
            val sourceKey = resolveSourceKeyForPhysicalPath(group, matchCount.fileIdentifier)
            ScanResultBuilder.buildScanResults(
              datasetPath,
              groupScanTimestamp,
              resolveLogicalIdentifierForPhysicalPath(group, datasetPath, matchCount.fileIdentifier),
              sampledRowCount,
              Map(matchCount.columnName -> nonEmptyCountsByFile.getOrElse((matchCount.fileIdentifier, matchCount.columnName), sampledRowCount)),
              Seq(MatchCount(matchCount.columnName, matchCount.piiType, matchCount.count, matchCount.metricAlias)),
              sampleValuesByFile
                .get((matchCount.fileIdentifier, matchCount.metricAlias))
                .map(value => Map(matchCount.metricAlias -> value))
                .getOrElse(Map.empty),
              sourceKey.flatMap(group.fileSizesByKey.get).getOrElse(0L),
              sourceKey.flatMap(group.fileMtimesByKey.get).getOrElse(0L)
            ).headOption
          }
        }
        val snapshotResults = AllowlistApplier.rescanBatchMatchedFilesWithSnapshots(
          spark,
          datasetPath,
          group,
          rules,
          effectiveSampleRatio,
          timestamp,
          suppressions,
          matchedSourceKeys,
          batchFileIdentifierValuesBySourceKey,
          columnName,
          selectedFileCount = effectiveSelectedSourceKeys.size
        )
        if (!hasDuplicateSelectedSourceKeys && ScanResultBuilder.comparableResultPayloads(provisionalResults) != ScanResultBuilder.comparableResultPayloads(snapshotResults)) {
          throw new IllegalStateException(s"Review snapshot changed during batch rescan: ${group.directoryPath}")
        }
        val filteredResults = AllowlistApplier.applyAllowlist(
          spark.sparkContext.hadoopConfiguration,
          datasetPath,
          allowlistMatcher,
          allowlistInputRoot,
          snapshotResults
        )
        DriverLogger.debug(
          "group_scan_batch_complete",
          "directory" -> group.directoryPath,
          "result_rows" -> filteredResults.size,
          "mode" -> "file_identifier"
        )
        filteredResults
      }
    }
  }
}
