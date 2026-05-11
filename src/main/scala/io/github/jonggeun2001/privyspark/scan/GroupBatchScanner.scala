package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.config.SuppressionSet
import io.github.jonggeun2001.privyspark.detect.DetectionAggregator
import io.github.jonggeun2001.privyspark.hive.{HiveTableFqnResolver, HiveTableLookupIndex}
import io.github.jonggeun2001.privyspark.format.CsvInference.{readSource, resolveFileIdentifierColumn}
import io.github.jonggeun2001.privyspark.fsio.RetryIO.withFileReadRetry
import io.github.jonggeun2001.privyspark.model.{MatchCount, PiiRule, ProgressRun, ScanGroup, ScanResult}
import io.github.jonggeun2001.privyspark.review.AllowlistMatcher
import io.github.jonggeun2001.privyspark.util.{DriverLogger, DriverTcpConnectionLogger}
import io.github.jonggeun2001.privyspark.util.PathIdentifiers._
import org.apache.spark.broadcast.Broadcast
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
    selectedSourceKeys: Option[Seq[String]] = None,
    progressRun: Option[ProgressRun] = None,
    hiveLookup: Option[Broadcast[HiveTableLookupIndex]] = None
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
    def logTcpSnapshot(phase: String, fields: (String, Any)*): Unit = {
      DriverTcpConnectionLogger.debugSnapshot(
        "group_scan_tcp_snapshot",
        (Seq(
          "phase" -> phase,
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "files" -> group.filePaths.size
        ) ++ fields): _*
      )
    }
    def elapsedMs(startNanos: Long): Long =
      (System.nanoTime() - startNanos) / 1000000L

    val effectiveSelectedSourceKeys =
      selectedSourceKeys.getOrElse(FileMetricsScanner.resolveSelectedFileKeys(group, sampleRatio, fileSampleRatio, fileSampleMinFiles))
    val fileSamplingApplied = effectiveSelectedSourceKeys.size < group.filePaths.size
    val hasDuplicateSelectedSourceKeys = effectiveSelectedSourceKeys.distinct.size != effectiveSelectedSourceKeys.size
    val physicalPaths = effectiveSelectedSourceKeys.map(sourceKey => resolvePhysicalPath(group, sourceKey))
    val effectiveSampleRatio = if (fileSamplingApplied) 1.0 else sampleRatio
    logTcpSnapshot(
      "batch_selected_sources",
      "selected_files" -> effectiveSelectedSourceKeys.size,
      "file_sampling_applied" -> fileSamplingApplied,
      "effective_sample_ratio" -> effectiveSampleRatio
    )
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
      val sampledRowsStartNanos = System.nanoTime()
      logTcpSnapshot(
        "batch_action_start",
        "action" -> "sampled_rows_by_file",
        "selected_files" -> effectiveSelectedSourceKeys.size,
        "dataframe_cached" -> false
      )
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
      logTcpSnapshot(
        "batch_action_complete",
        "action" -> "sampled_rows_by_file",
        "selected_files" -> effectiveSelectedSourceKeys.size,
        "files_with_rows" -> sampledRowsByFile.size,
        "duration_ms" -> elapsedMs(sampledRowsStartNanos),
        "dataframe_cached" -> false
      )
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
        val aggregateStartNanos = System.nanoTime()
        logTcpSnapshot(
          "batch_action_start",
          "action" -> "aggregate_matches",
          "selected_files" -> effectiveSelectedSourceKeys.size,
          "dataframe_cached" -> false
        )
        val matchCountsByFile = DetectionAggregator.aggregateByFile(sampledDf, columnName, effectiveRules, suppressions = suppressions)
        logTcpSnapshot(
          "batch_action_complete",
          "action" -> "aggregate_matches",
          "selected_files" -> effectiveSelectedSourceKeys.size,
          "matches" -> matchCountsByFile.size,
          "duration_ms" -> elapsedMs(aggregateStartNanos),
          "dataframe_cached" -> false
        )
        val sampleMatchesStartNanos = System.nanoTime()
        logTcpSnapshot(
          "batch_action_start",
          "action" -> "sample_matches",
          "selected_files" -> effectiveSelectedSourceKeys.size,
          "dataframe_cached" -> false
        )
        val sampleValuesByFile = DetectionAggregator.sampleMatchesByFile(
          sampledDf,
          columnName,
          effectiveRules,
          matchCountsByFile,
          suppressions = suppressions
        )
        logTcpSnapshot(
          "batch_action_complete",
          "action" -> "sample_matches",
          "selected_files" -> effectiveSelectedSourceKeys.size,
          "sample_values" -> sampleValuesByFile.size,
          "duration_ms" -> elapsedMs(sampleMatchesStartNanos),
          "dataframe_cached" -> false
        )
        val nonEmptyStartNanos = System.nanoTime()
        logTcpSnapshot(
          "batch_action_start",
          "action" -> "count_non_empty",
          "selected_files" -> effectiveSelectedSourceKeys.size,
          "dataframe_cached" -> false
        )
        val nonEmptyCountsByFile = DetectionAggregator.countNonEmptyByFile(sampledDf, columnName, matchCountsByFile.map(_.columnName).distinct)
        logTcpSnapshot(
          "batch_action_complete",
          "action" -> "count_non_empty",
          "selected_files" -> effectiveSelectedSourceKeys.size,
          "non_empty_counts" -> nonEmptyCountsByFile.size,
          "duration_ms" -> elapsedMs(nonEmptyStartNanos),
          "dataframe_cached" -> false
        )
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
            val fqn = HiveTableFqnResolver.resolve(hiveLookup, matchCount.fileIdentifier)
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
              sourceKey.flatMap(group.fileMtimesByKey.get).getOrElse(0L),
              hiveTableFqn = fqn
            ).headOption
          }
        }
        val snapshotRescanStartNanos = System.nanoTime()
        logTcpSnapshot(
          "batch_snapshot_rescan_start",
          "selected_files" -> effectiveSelectedSourceKeys.size,
          "matched_files" -> matchedSourceKeys.size
        )
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
          selectedFileCount = effectiveSelectedSourceKeys.size,
          progressRun = progressRun,
          hiveLookup = hiveLookup
        )
        logTcpSnapshot(
          "batch_snapshot_rescan_complete",
          "selected_files" -> effectiveSelectedSourceKeys.size,
          "matched_files" -> matchedSourceKeys.size,
          "duration_ms" -> elapsedMs(snapshotRescanStartNanos)
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
