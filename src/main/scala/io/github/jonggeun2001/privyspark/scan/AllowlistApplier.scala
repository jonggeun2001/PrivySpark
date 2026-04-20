package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.config.SuppressionSet
import io.github.jonggeun2001.privyspark.model.{MatchCount, PiiRule, ProgressRun, ScanError, ScanGroup, ScanResult}
import io.github.jonggeun2001.privyspark.progress.ProgressIO.persistProgressRecords
import io.github.jonggeun2001.privyspark.review.{AllowlistEvaluation, AllowlistMatcher, FileIdentifierResolver, ReviewScopeFingerprintCodec}
import io.github.jonggeun2001.privyspark.util.DriverLogger
import io.github.jonggeun2001.privyspark.util.ParallelismConfig.{executeInParallel, resolveFileParallelism}
import io.github.jonggeun2001.privyspark.util.PathIdentifiers.{resolveDirectoryIdentifier, resolveLogicalIdentifier, resolvePhysicalPath}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

private[privyspark] object AllowlistApplier {
  def rescanBatchMatchedFilesWithSnapshots(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    suppressions: SuppressionSet,
    matchedSourceKeys: Seq[String],
    batchFileIdentifierValuesBySourceKey: Map[String, String],
    batchFileIdentifierColumnName: String,
    selectedFileCount: Int,
    csvHeadCache: CsvHeadCache = new CsvHeadCache()
  ): Seq[ScanResult] = {
    if (matchedSourceKeys.isEmpty) {
      ReviewSnapshotLog.logReviewSnapshotSkipped("batch", matchedFiles = 0, selectedFiles = selectedFileCount)
      Seq.empty
    } else {
      ReviewSnapshotLog.logReviewSnapshotStart("batch", matchedSourceKeys.size, selectedFileCount)
      val parallelism = resolveFileParallelism(spark, matchedSourceKeys.size)
      val rescannedMetrics = executeInParallel(parallelism, matchedSourceKeys.map { sourceKey =>
        () => {
          val physicalPath = resolvePhysicalPath(group, sourceKey)
          val logicalIdentifier = resolveLogicalIdentifier(group, datasetPath, sourceKey)
          val batchFileIdentifierValue = batchFileIdentifierValuesBySourceKey.getOrElse(sourceKey, physicalPath)
          ReviewSnapshotLog.logReviewSnapshotFile("batch", physicalPath, logicalIdentifier)
          sourceKey -> SourceKeyMetrics.scanSourceKeyUsingSnapshot(
            spark,
            datasetPath,
            group,
            sourceKey,
            rules,
            sampleRatio,
            timestamp,
            suppressions,
            csvHeadCache,
            injectedFileIdentifierColumn = Some(batchFileIdentifierColumnName -> batchFileIdentifierValue)
          )
        }
      })

      val metricMap = rescannedMetrics.map {
        case (_, Right(fileMetrics)) =>
          fileMetrics.fileIdentifier -> fileMetrics
        case (sourceKey, Left(error)) =>
          throw new IllegalStateException(
            s"Batch review snapshot rescan failed for ${resolveLogicalIdentifier(group, datasetPath, sourceKey)}: ${error.error_message}"
          )
      }.toMap

      matchedSourceKeys.flatMap { sourceKey =>
        val fileIdentifier = resolveLogicalIdentifier(group, datasetPath, sourceKey)
        metricMap.get(fileIdentifier).toSeq.flatMap { fileMetrics =>
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
    }
  }

  def applyAllowlist(
    conf: org.apache.hadoop.conf.Configuration,
    datasetPath: String,
    allowlistMatcher: AllowlistMatcher,
    allowlistInputRoot: Option[String],
    results: Seq[ScanResult]
  ): Seq[ScanResult] = {
    if (results.isEmpty || allowlistMatcher.isEmpty || allowlistInputRoot.isEmpty) {
      results
    } else {
      val inputRoot = allowlistInputRoot.get
      results.flatMap { result =>
        val reviewScopeIdentifiers = ReviewSnapshotLog.parseReviewScopeIdentifiers(result.review_scope_file_identifiers)
        val hasCandidate =
          allowlistMatcher.hasDirectoryCandidate(result.dataset_path, result.file_identifier, result.column_name, result.pii_type) ||
            (if (reviewScopeIdentifiers.nonEmpty) {
              reviewScopeIdentifiers.exists(identifier =>
                allowlistMatcher.hasExactCandidate(result.dataset_path, identifier, result.column_name, result.pii_type)
              )
            } else {
              allowlistMatcher.hasExactCandidate(result.dataset_path, result.file_identifier, result.column_name, result.pii_type)
            })

        if (!hasCandidate) {
          Some(result)
        } else {
          val identifiersToResolve = if (reviewScopeIdentifiers.nonEmpty) reviewScopeIdentifiers else Seq(result.file_identifier)
          val resolvedFingerprints = identifiersToResolve.foldLeft[Either[String, Vector[io.github.jonggeun2001.privyspark.review.ResolvedFileFingerprint]]](Right(Vector.empty)) {
            case (Right(fingerprints), identifier) =>
              FileIdentifierResolver.resolveFingerprints(conf, inputRoot, identifier) match {
                case Right(resolved) =>
                  Right(fingerprints ++ resolved)
                case Left(errorMessage) =>
                  Left(s"$identifier: $errorMessage")
              }
            case (left, _) =>
              left
          }
          resolvedFingerprints match {
            case Right(fingerprints) =>
              val evaluation = allowlistMatcher.evaluate(result.dataset_path, result.column_name, result.pii_type, fingerprints)
              applyAllowlistEvaluation(result, evaluation)
            case Left(errorMessage) =>
              DriverLogger.warn(
                "allowlist_resolution_failed",
                "file_identifier" -> result.file_identifier,
                "column" -> result.column_name,
                "pii_type" -> result.pii_type,
                "reason" -> errorMessage
              )
              Some(result)
          }
        }
      }
    }
  }

  def applyAllowlistEvaluation(
    result: ScanResult,
    evaluation: AllowlistEvaluation
  ): Option[ScanResult] = {
    if (evaluation.shouldSuppress) {
      None
    } else if (
      evaluation.reviewStatus != result.review_status ||
        evaluation.reviewReason != result.review_reason ||
        evaluation.reviewInvalidated != result.review_invalidated
    ) {
      Some(result.copy(
        review_status = evaluation.reviewStatus,
        review_reason = evaluation.reviewReason,
        review_invalidated = evaluation.reviewInvalidated
      ))
    } else {
      Some(result)
    }
  }

  def rescanSampledGroupWithExactSplit(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    mode: String,
    fileParallelism: Int,
    fileSampleRatio: Option[Double],
    fileSampleMinFiles: Int,
    suppressions: SuppressionSet,
    allowlistMatcher: AllowlistMatcher,
    allowlistInputRoot: Option[String],
    progressRun: Option[ProgressRun],
    csvHeadCache: CsvHeadCache
  ): (Seq[ScanResult], Seq[ScanError]) = {
    val (splitGroups, splitErrors) = DirectoryScanner.splitGroupBySchema(
      spark,
      datasetPath,
      timestamp,
      group.copy(schemaSampled = false),
      csvHeadCache
    )
    val exactSplitCanUseDirectoryIdentifier =
      group.directoryIdentifierEligible &&
      splitGroups.size == 1 &&
      splitErrors.isEmpty
    val rescannedGroups = splitGroups.map(_.copy(
      useDirectoryIdentifier = exactSplitCanUseDirectoryIdentifier,
      directoryIdentifierEligible = group.directoryIdentifierEligible,
      schemaSampled = false
    ))

    val rescannedResults = ArrayBuffer.empty[ScanResult]
    val rescannedErrors = ArrayBuffer.empty[ScanError] ++ splitErrors
    if (splitErrors.nonEmpty) {
      progressRun.foreach { run =>
        persistProgressRecords(
          spark.sparkContext.hadoopConfiguration,
          run,
          "schema-split",
          group.directoryPath,
          Seq.empty,
          splitErrors
        )
      }
    }
    rescannedGroups.foreach { rescannedGroup =>
      val (groupResults, groupErrors) = GroupScanCoordinator.scanGroup(
        spark,
        datasetPath,
        rescannedGroup,
        rules,
        sampleRatio,
        timestamp,
        fileParallelism,
        fileSampleRatio,
        fileSampleMinFiles,
        suppressions,
        allowlistMatcher,
        allowlistInputRoot,
        progressRun,
        csvHeadCache
      )
      rescannedResults ++= groupResults
      rescannedErrors ++= groupErrors
    }

    DriverLogger.debug(
      "group_scan_exact_split_complete",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "split_groups" -> splitGroups.size,
      "split_errors" -> splitErrors.size,
      "use_directory_identifier" -> exactSplitCanUseDirectoryIdentifier,
      "mode" -> mode
    )
    (rescannedResults.toSeq, rescannedErrors.toSeq)
  }
}
