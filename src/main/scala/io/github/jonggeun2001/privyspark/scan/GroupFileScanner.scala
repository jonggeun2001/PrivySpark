package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.config.SuppressionSet
import io.github.jonggeun2001.privyspark.hive.{HiveTableFqnResolver, HiveTableLookupIndex}
import io.github.jonggeun2001.privyspark.model.{FileScanMetrics, MatchCount, PiiRule, ProgressRun, ScanError, ScanGroup, ScanResult}
import io.github.jonggeun2001.privyspark.progress.InFlightMarker
import io.github.jonggeun2001.privyspark.progress.{ProgressBuffer, ProgressIO}
import io.github.jonggeun2001.privyspark.progress.ProgressIO.{ProgressFlushMode, persistProgressRecords}
import io.github.jonggeun2001.privyspark.review.{AllowlistMatcher, ReviewScopeFingerprintCodec}
import io.github.jonggeun2001.privyspark.util.{DriverLogger, DriverTcpConnectionLogger, RpcGate}
import io.github.jonggeun2001.privyspark.util.ParallelismConfig.{executeInParallel, resolveFileParallelism, resolveParallelism}
import io.github.jonggeun2001.privyspark.util.PathIdentifiers.{resolveDirectoryIdentifier, resolveLogicalIdentifier, resolvePhysicalPath}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

private[privyspark] object GroupFileScanner {
  val FileInFlightMarkerEnabledConfKey = "spark.privyspark.progress.fileMarker.enabled"

  def fileInFlightMarkersEnabled(conf: SparkConf): Boolean =
    conf.getBoolean(FileInFlightMarkerEnabledConfKey, false)

  def scanGroupByFile(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    fileParallelism: Int = -1,
    suppressions: SuppressionSet = SuppressionSet.empty,
    allowlistMatcher: AllowlistMatcher = AllowlistMatcher.empty,
    allowlistInputRoot: Option[String] = None,
    progressRun: Option[ProgressRun] = None,
    csvHeadCache: CsvHeadCache = new CsvHeadCache(),
    fileSampleRatio: Option[Double] = None,
    fileSampleMinFiles: Int = 10,
    selectedSourceKeys: Option[Seq[String]] = None,
    hiveLookup: Option[Broadcast[HiveTableLookupIndex]] = None
  ): (Seq[ScanResult], Seq[ScanError]) = {
    DriverLogger.warn(
      "group_scan_fallback_execute",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "files" -> group.filePaths.size,
      "file_sample_ratio" -> fileSampleRatio.getOrElse("none"),
      "file_sample_min_files" -> fileSampleMinFiles,
      "mode" -> "file_scan"
    )
    val effectiveSelectedSourceKeys =
      selectedSourceKeys.getOrElse(FileMetricsScanner.resolveSelectedFileKeys(group, sampleRatio, fileSampleRatio, fileSampleMinFiles))
    val effectiveSampleRatio = if (effectiveSelectedSourceKeys.size < group.filePaths.size) 1.0 else sampleRatio
    val parallelism = if (fileParallelism > 0) {
      resolveParallelism(effectiveSelectedSourceKeys.size, fileParallelism)
    } else {
      resolveFileParallelism(spark, effectiveSelectedSourceKeys.size)
    }
    DriverLogger.debug(
      "group_scan_fallback_execute",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "files" -> group.filePaths.size,
      "selected_files" -> effectiveSelectedSourceKeys.size,
      "file_sample_ratio" -> fileSampleRatio.getOrElse("none"),
      "file_sample_min_files" -> fileSampleMinFiles,
      "use_directory_identifier" -> group.useDirectoryIdentifier,
      "parallelism" -> parallelism
    )
    DriverTcpConnectionLogger.debugSnapshot(
      "group_scan_tcp_snapshot",
      "phase" -> "file_scan_parallelism",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "files" -> group.filePaths.size,
      "selected_files" -> effectiveSelectedSourceKeys.size,
      "use_directory_identifier" -> group.useDirectoryIdentifier,
      "parallelism" -> parallelism
    )
    val successfulFileMetrics = ArrayBuffer.empty[FileScanMetrics]
    val fallbackErrors = ArrayBuffer.empty[ScanError]
    val rpcGate = RpcGate.driverGate(spark)
    val fileMarkersEnabled = fileInFlightMarkersEnabled(spark.sparkContext.getConf)
    val progressFlushMode = ProgressIO.resolveFlushMode(spark)
    val groupProgressBuffer =
      if (!group.useDirectoryIdentifier && progressFlushMode == ProgressFlushMode.Group) {
        progressRun.map(run => new ProgressBuffer(spark.sparkContext.hadoopConfiguration, run, "group", group.directoryPath))
      } else {
        None
      }

    def persistFileProgress(identifier: String, results: Seq[ScanResult], errors: Seq[ScanError]): Unit = {
      progressRun.foreach { run =>
        groupProgressBuffer match {
          case Some(buffer) =>
            buffer.enqueue(results, errors)
          case None =>
            persistProgressRecords(
              spark.sparkContext.hadoopConfiguration,
              run,
              "file",
              identifier,
              results,
              errors
            )
        }
      }
    }

    executeInParallel(parallelism, effectiveSelectedSourceKeys.map { sourceKey =>
      () => {
        val physicalPath = resolvePhysicalPath(group, sourceKey)
        val logicalIdentifier = resolveLogicalIdentifier(group, datasetPath, sourceKey)
        val fileHiveTableFqn = HiveTableFqnResolver.resolve(hiveLookup, physicalPath)
        DriverLogger.debug("group_scan_fallback_file_start", "file" -> physicalPath, "directory" -> group.directoryPath)
        DriverTcpConnectionLogger.debugSnapshot(
          "group_scan_tcp_snapshot",
          "phase" -> "file_scan_source_start",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "file" -> physicalPath,
          "file_identifier" -> logicalIdentifier,
          "use_directory_identifier" -> group.useDirectoryIdentifier,
          "effective_sample_ratio" -> effectiveSampleRatio
        )
        def scanFileMetrics(): Either[ScanError, FileScanMetrics] =
          if (group.useDirectoryIdentifier) {
            SourceKeyMetrics.scanSourceKeyUsingSnapshot(
              spark,
              datasetPath,
              group,
              sourceKey,
              rules,
              effectiveSampleRatio,
              timestamp,
              suppressions,
              csvHeadCache
            )
          } else {
            SourceKeyMetrics.scanSourceKeyMetrics(
              spark,
              datasetPath,
              group,
              sourceKey,
              rules,
              effectiveSampleRatio,
              timestamp,
              suppressions = suppressions,
              csvHeadCache = csvHeadCache,
              captureRecordedFingerprintWhenMissing = false
            ).flatMap { provisionalMetrics =>
              if (provisionalMetrics.matchCounts.isEmpty) {
                ReviewSnapshotLog.logReviewSnapshotSkipped(
                  "file",
                  matchedFiles = 0,
                  selectedFiles = 1,
                  physicalPath = Some(physicalPath),
                  fileIdentifier = Some(logicalIdentifier)
                )
                Right(provisionalMetrics)
              } else {
                ReviewSnapshotLog.logReviewSnapshotStart("file", matchedFiles = 1, selectedFiles = 1)
                ReviewSnapshotLog.logReviewSnapshotFile("file", physicalPath, logicalIdentifier)
                val provisionalResults = ScanResultBuilder.buildScanResults(
                  datasetPath,
                  provisionalMetrics.scanTimestamp,
                  provisionalMetrics.fileIdentifier,
                  provisionalMetrics.sampledRowCount,
                  provisionalMetrics.nonEmptyValueCounts,
                  provisionalMetrics.matchCounts,
                  provisionalMetrics.sampleValues,
                  provisionalMetrics.fileSize,
                  provisionalMetrics.fileMtimeEpochMs,
                  hiveTableFqn = fileHiveTableFqn
                )
                SourceKeyMetrics.scanSourceKeyUsingSnapshot(
                  spark,
                  datasetPath,
                  group,
                  sourceKey,
                  rules,
                  effectiveSampleRatio,
                  timestamp,
                  suppressions,
                  csvHeadCache
                ).flatMap { snapshotMetrics =>
                  val snapshotResults = ScanResultBuilder.buildScanResults(
                    datasetPath,
                    snapshotMetrics.scanTimestamp,
                    snapshotMetrics.fileIdentifier,
                    snapshotMetrics.sampledRowCount,
                    snapshotMetrics.nonEmptyValueCounts,
                    snapshotMetrics.matchCounts,
                    snapshotMetrics.sampleValues,
                    snapshotMetrics.fileSize,
                    snapshotMetrics.fileMtimeEpochMs,
                    hiveTableFqn = fileHiveTableFqn
                  )
                  if (ScanResultBuilder.comparableResultPayloads(provisionalResults) == ScanResultBuilder.comparableResultPayloads(snapshotResults)) {
                    Right(snapshotMetrics)
                  } else {
                    Left(ScanError(datasetPath, timestamp, logicalIdentifier, "Review snapshot changed during rescan"))
                  }
                }
              }
            }
          }
        val fileMetrics = progressRun match {
          case Some(run) if fileMarkersEnabled =>
            InFlightMarker.run(
              spark.sparkContext.hadoopConfiguration,
              run.inFlightPath,
              "file",
              logicalIdentifier,
              Map("format" -> group.format, "schemaSignature" -> group.schemaSignature),
              preserveOnFailure = true
            ) {
              scanFileMetrics()
            }
          case None =>
            scanFileMetrics()
          case Some(_) =>
            scanFileMetrics()
        }
        sourceKey -> fileMetrics
          .fold(
            error => {
              if (!group.useDirectoryIdentifier) {
                persistFileProgress(error.file_identifier, Seq.empty, Seq(error))
              }
              Left(error)
            },
            fileMetrics => {
              if (!group.useDirectoryIdentifier) {
                val fileResults = AllowlistApplier.applyAllowlist(
                  spark.sparkContext.hadoopConfiguration,
                  datasetPath,
                  allowlistMatcher,
                  allowlistInputRoot,
                  ScanResultBuilder.buildScanResults(
                    datasetPath,
                    fileMetrics.scanTimestamp,
                    fileMetrics.fileIdentifier,
                    fileMetrics.sampledRowCount,
                    fileMetrics.nonEmptyValueCounts,
                    fileMetrics.matchCounts,
                    fileMetrics.sampleValues,
                    fileMetrics.fileSize,
                    fileMetrics.fileMtimeEpochMs,
                    hiveTableFqn = fileHiveTableFqn,
                    reviewScopeFileFingerprints = ReviewSnapshotLog.encodeRecordedFingerprint(fileMetrics.recordedFingerprint)
                  )
                )
                persistFileProgress(fileMetrics.fileIdentifier, fileResults, Seq.empty)
              }
              Right(fileMetrics)
            }
          )
      }
    }, gate = rpcGate).foreach {
      case (sourceKey, fileResult) =>
        val physicalPath = resolvePhysicalPath(group, sourceKey)
        fileResult match {
          case Right(fileMetrics) =>
            successfulFileMetrics += fileMetrics
            DriverLogger.debug(
              "group_scan_fallback_file_success",
              "file" -> physicalPath,
              "file_identifier" -> fileMetrics.fileIdentifier,
              "sampled_rows" -> fileMetrics.sampledRowCount,
              "matches" -> fileMetrics.matchCounts.size
            )
            DriverTcpConnectionLogger.debugSnapshot(
              "group_scan_tcp_snapshot",
              "phase" -> "file_scan_source_complete",
              "directory" -> group.directoryPath,
              "format" -> group.format,
              "schema" -> group.schemaSignature,
              "file" -> physicalPath,
              "file_identifier" -> fileMetrics.fileIdentifier,
              "sampled_rows" -> fileMetrics.sampledRowCount,
              "matches" -> fileMetrics.matchCounts.size
            )
          case Left(error) =>
            fallbackErrors += error
            DriverLogger.debug(
              "group_scan_fallback_file_error",
              "file" -> physicalPath,
              "file_identifier" -> error.file_identifier,
              "reason" -> error.error_message
            )
            DriverTcpConnectionLogger.debugSnapshot(
              "group_scan_tcp_snapshot",
              "phase" -> "file_scan_source_error",
              "directory" -> group.directoryPath,
              "format" -> group.format,
              "schema" -> group.schemaSignature,
              "file" -> physicalPath,
              "file_identifier" -> error.file_identifier,
              "reason" -> error.error_message
            )
        }
    }
    groupProgressBuffer.foreach(_.flush())

    val fallbackResults = if (group.useDirectoryIdentifier && fallbackErrors.isEmpty) {
      val directoryHiveTableFqn = HiveTableFqnResolver.resolve(hiveLookup, group.directoryPath)
      val reviewScopeFileIdentifiers = successfulFileMetrics.map(_.fileIdentifier)
      val reviewScopeFileFingerprints =
        if (successfulFileMetrics.forall(_.recordedFingerprint.nonEmpty)) {
          ReviewScopeFingerprintCodec.encode(successfulFileMetrics.flatMap(_.recordedFingerprint))
        } else {
          ""
        }
      val sampledRowCount = successfulFileMetrics.map(_.sampledRowCount).sum
      val aggregatedMatchCounts = successfulFileMetrics
        .flatMap(_.matchCounts)
        .groupBy(matchCount => (matchCount.metricAlias, matchCount.columnName, matchCount.piiType))
        .toSeq
        .sortBy { case ((metricAlias, columnName, piiType), _) => (columnName, piiType, metricAlias) }
        .map {
          case ((metricAlias, columnName, piiType), matchCounts) =>
            MatchCount(columnName, piiType, matchCounts.map(_.count).sum, metricAlias)
        }

      ScanResultBuilder.buildScanResults(
        datasetPath,
        ScanResultBuilder.currentScanTimestamp(),
        resolveDirectoryIdentifier(datasetPath, group.directoryPath),
        sampledRowCount,
        successfulFileMetrics
          .flatMap(_.nonEmptyValueCounts.toSeq)
          .groupBy(_._1)
          .map {
            case (columnName, counts) => columnName -> counts.map(_._2).sum
          }
          .toMap,
        aggregatedMatchCounts,
        successfulFileMetrics
          .flatMap(_.sampleValues.toSeq)
          .groupBy(_._1)
          .map {
            case (metricAlias, values) => metricAlias -> values.head._2
          }
          .toMap,
        successfulFileMetrics.map(_.fileSize).sum,
        successfulFileMetrics.map(_.fileMtimeEpochMs).foldLeft(0L)(math.max),
        hiveTableFqn = directoryHiveTableFqn,
        reviewScopeFileIdentifiers = reviewScopeFileIdentifiers,
        reviewScopeFileFingerprints = reviewScopeFileFingerprints
      )
    } else {
      if (group.useDirectoryIdentifier && fallbackErrors.nonEmpty) {
        DriverLogger.warn(
          "group_scan_partial_results",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "failed_files" -> fallbackErrors.size,
          "mode" -> "file_identifier_preserved"
        )
        DriverLogger.debug(
          "group_scan_partial_results",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "failed_files" -> fallbackErrors.size
        )
      }
      successfulFileMetrics.flatMap { fileMetrics =>
        val fileHiveTableFqn = HiveTableFqnResolver.resolve(hiveLookup, physicalPathForFileIdentifier(group, datasetPath, fileMetrics.fileIdentifier))
        ScanResultBuilder.buildScanResults(
          datasetPath,
          fileMetrics.scanTimestamp,
          fileMetrics.fileIdentifier,
          fileMetrics.sampledRowCount,
          fileMetrics.nonEmptyValueCounts,
          fileMetrics.matchCounts,
          fileMetrics.sampleValues,
          fileMetrics.fileSize,
          fileMetrics.fileMtimeEpochMs,
          hiveTableFqn = fileHiveTableFqn,
          reviewScopeFileFingerprints = ReviewSnapshotLog.encodeRecordedFingerprint(fileMetrics.recordedFingerprint)
        )
      }
    }
    val filteredFallbackResults = AllowlistApplier.applyAllowlist(
      spark.sparkContext.hadoopConfiguration,
      datasetPath,
      allowlistMatcher,
      allowlistInputRoot,
      fallbackResults
    )
    progressRun.foreach { run =>
      if (group.useDirectoryIdentifier) {
        persistProgressRecords(
          spark.sparkContext.hadoopConfiguration,
          run,
          "group",
          group.directoryPath,
          filteredFallbackResults,
          fallbackErrors.toSeq
        )
      }
    }

    DriverLogger.debug(
      "group_scan_fallback_complete",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "successful_files" -> successfulFileMetrics.size,
      "failed_files" -> fallbackErrors.size,
      "result_rows" -> filteredFallbackResults.size
    )
    (filteredFallbackResults.toSeq, fallbackErrors.toSeq)
  }

  private def physicalPathForFileIdentifier(group: ScanGroup, datasetPath: String, fileIdentifier: String): String =
    group.filePaths.find(sourceKey => resolveLogicalIdentifier(group, datasetPath, sourceKey) == fileIdentifier) match {
      case Some(sourceKey) => resolvePhysicalPath(group, sourceKey)
      case None => fileIdentifier
    }
}
