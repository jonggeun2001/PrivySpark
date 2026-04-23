package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.config.SuppressionSet
import io.github.jonggeun2001.privyspark.model.{FileScanMetrics, MatchCount, PiiRule, ProgressRun, ScanError, ScanGroup, ScanResult}
import io.github.jonggeun2001.privyspark.progress.InFlightMarker
import io.github.jonggeun2001.privyspark.progress.ProgressIO.persistProgressRecords
import io.github.jonggeun2001.privyspark.review.{AllowlistMatcher, ReviewScopeFingerprintCodec}
import io.github.jonggeun2001.privyspark.util.DriverLogger
import io.github.jonggeun2001.privyspark.util.ParallelismConfig.{executeInParallel, resolveFileParallelism, resolveParallelism}
import io.github.jonggeun2001.privyspark.util.PathIdentifiers.{resolveDirectoryIdentifier, resolveLogicalIdentifier, resolvePhysicalPath}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

private[privyspark] object GroupFileScanner {
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
    selectedSourceKeys: Option[Seq[String]] = None
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
    val successfulFileMetrics = ArrayBuffer.empty[FileScanMetrics]
    val fallbackErrors = ArrayBuffer.empty[ScanError]
    executeInParallel(parallelism, effectiveSelectedSourceKeys.map { sourceKey =>
      () => {
        val physicalPath = resolvePhysicalPath(group, sourceKey)
        val logicalIdentifier = resolveLogicalIdentifier(group, datasetPath, sourceKey)
        DriverLogger.debug("group_scan_fallback_file_start", "file" -> physicalPath, "directory" -> group.directoryPath)
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
                  provisionalMetrics.fileMtimeEpochMs
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
                    snapshotMetrics.fileMtimeEpochMs
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
          case Some(run) =>
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
        }
        sourceKey -> fileMetrics
          .fold(
            error => {
              if (!group.useDirectoryIdentifier) {
                progressRun.foreach { run =>
                  persistProgressRecords(
                    spark.sparkContext.hadoopConfiguration,
                    run,
                    "file",
                    error.file_identifier,
                    Seq.empty,
                    Seq(error)
                  )
                }
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
                    reviewScopeFileFingerprints = ReviewSnapshotLog.encodeRecordedFingerprint(fileMetrics.recordedFingerprint)
                  )
                )
                progressRun.foreach { run =>
                  persistProgressRecords(
                    spark.sparkContext.hadoopConfiguration,
                    run,
                    "file",
                    fileMetrics.fileIdentifier,
                    fileResults,
                    Seq.empty
                  )
                }
              }
              Right(fileMetrics)
            }
          )
      }
    }).foreach {
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
          case Left(error) =>
            fallbackErrors += error
            DriverLogger.debug(
              "group_scan_fallback_file_error",
              "file" -> physicalPath,
              "file_identifier" -> error.file_identifier,
              "reason" -> error.error_message
            )
        }
    }

    val fallbackResults = if (group.useDirectoryIdentifier && fallbackErrors.isEmpty) {
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
        reviewScopeFileIdentifiers,
        reviewScopeFileFingerprints
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
}
