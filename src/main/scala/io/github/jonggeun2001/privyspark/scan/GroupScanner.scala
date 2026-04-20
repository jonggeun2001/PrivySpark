package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.detect.DetectionAggregator
import io.github.jonggeun2001.privyspark.format.ByteProbe.detectPhysicalFormat
import io.github.jonggeun2001.privyspark.format.CsvInference._
import io.github.jonggeun2001.privyspark.scan.DirectoryScanner.splitGroupBySchema
import io.github.jonggeun2001.privyspark.util.ParallelismConfig._
import io.github.jonggeun2001.privyspark.util.PathIdentifiers._
import io.github.jonggeun2001.privyspark.util.DriverLogger
import io.github.jonggeun2001.privyspark.fsio.RetryIO.withFileReadRetry
import io.github.jonggeun2001.privyspark.scan.SourceExpansion.supportsBatchScan
import io.github.jonggeun2001.privyspark.model.{FileScanMetrics, MatchCount, PiiRule, ProgressRun, SampleValue, ScanError, ScanGroup, ScanReadOptions, ScanResult}
import io.github.jonggeun2001.privyspark.progress.ProgressIO.persistProgressRecords
import io.github.jonggeun2001.privyspark.review.{AllowlistEvaluation, AllowlistMatcher, FileIdentifierResolver, RecordedFileFingerprint, ReviewScopeFingerprintCodec}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, input_file_name}

import java.time.Instant
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.NonFatal

private[privyspark] object GroupScanner {
  private def effectiveRulesForFormat(format: String, rules: Seq[PiiRule]): Seq[PiiRule] = {
    rules
  }

  private def buildScanResults(
    datasetPath: String,
    scanTimestamp: String,
    fileIdentifier: String,
    sampledRowCount: Long,
    nonEmptyValueCounts: Map[String, Long],
    matchCounts: Seq[MatchCount],
    sampleValues: Map[String, SampleValue] = Map.empty,
    fileSize: Long = 0L,
    fileMtimeEpochMs: Long = 0L,
    reviewScopeFileIdentifiers: Seq[String] = Seq.empty,
    reviewScopeFileFingerprints: String = ""
  ): Seq[ScanResult] = {
    if (sampledRowCount <= 0L) {
      Seq.empty
    } else {
      matchCounts.map { matchCount =>
        val matchRatio = roundProbability(matchCount.count.toDouble / sampledRowCount.toDouble)
        val nonEmptyDenominator = nonEmptyValueCounts.get(matchCount.columnName).filter(_ > 0L).getOrElse(sampledRowCount)
        val nonEmptyMatchRatio = roundProbability(matchCount.count.toDouble / nonEmptyDenominator.toDouble)
        val confidenceValue = roundProbability(wilsonLowerBound(matchCount.count, nonEmptyDenominator))
        val sampleValue = sampleValues.get(matchCount.metricAlias)
        ScanResult(
          dataset_path = datasetPath,
          scan_timestamp = scanTimestamp,
          file_identifier = fileIdentifier,
          column_name = matchCount.columnName,
          pii_type = matchCount.piiType,
          match_count = matchCount.count,
          sampled_row_count = sampledRowCount,
          match_ratio = matchRatio,
          non_empty_match_ratio = nonEmptyMatchRatio,
          confidence = confidenceValue,
          sample_raw_value = sampleValue.map(_.sampleRawValue).getOrElse(""),
          sample_matched_fragment = sampleValue.map(_.sampleMatchedFragment).getOrElse(""),
          file_size = fileSize,
          file_mtime_epoch_ms = fileMtimeEpochMs,
          review_scope_file_identifiers = reviewScopeFileIdentifiers.mkString("|"),
          review_scope_file_fingerprints = reviewScopeFileFingerprints
        )
      }
    }
  }

  private def buildReviewScopeFileFingerprints(
    conf: org.apache.hadoop.conf.Configuration,
    datasetPath: String,
    fileIdentifiers: Seq[String],
    expectedMetadataByFileIdentifier: Map[String, (Long, Long)]
  ): String = {
    val recordedFingerprints = ArrayBuffer.empty[RecordedFileFingerprint]
    fileIdentifiers.distinct.foreach { fileIdentifier =>
      val expectedMetadata = expectedMetadataByFileIdentifier.get(fileIdentifier)
      if (expectedMetadata.isEmpty) {
        DriverLogger.warn(
          "review_scope_fingerprint_metadata_missing",
          "file_identifier" -> fileIdentifier
        )
        return ""
      }
      FileIdentifierResolver.resolveFingerprints(conf, datasetPath, fileIdentifier) match {
        case Right(resolvedFingerprints) =>
          val (expectedFileSize, expectedFileMtimeEpochMs) = expectedMetadata.get
          val snapshotMatches = resolvedFingerprints.forall { fingerprint =>
            fingerprint.fileIdentifier == fileIdentifier &&
              fingerprint.fileSize == expectedFileSize &&
              fingerprint.fileMtimeEpochMs == expectedFileMtimeEpochMs
          }
          if (!snapshotMatches) {
            DriverLogger.warn(
              "review_scope_fingerprint_snapshot_mismatch",
              "file_identifier" -> fileIdentifier,
              "expected_file_size" -> expectedFileSize,
              "expected_file_mtime_epoch_ms" -> expectedFileMtimeEpochMs
            )
            return ""
          }
          recordedFingerprints ++= resolvedFingerprints.map(RecordedFileFingerprint.fromResolved)
        case Left(errorMessage) =>
          DriverLogger.warn(
            "review_scope_fingerprint_resolution_failed",
            "file_identifier" -> fileIdentifier,
            "reason" -> errorMessage
          )
          return ""
      }
    }
    ReviewScopeFingerprintCodec.encode(recordedFingerprints.toSeq)
  }

  private def applyAllowlist(
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
        val hasCandidate =
          allowlistMatcher.hasExactCandidate(result.dataset_path, result.file_identifier, result.column_name, result.pii_type) ||
            allowlistMatcher.hasDirectoryCandidate(result.dataset_path, result.file_identifier, result.column_name, result.pii_type)

        if (!hasCandidate) {
          Some(result)
        } else {
          FileIdentifierResolver.resolveFingerprints(conf, inputRoot, result.file_identifier) match {
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

  private def applyAllowlistEvaluation(
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

  private def currentScanTimestamp(): String = Instant.now().toString

  private def wilsonLowerBound(successes: Long, trials: Long): Double = {
    if (trials <= 0L) {
      0.0
    } else {
      val n = trials.toDouble
      val p = successes.toDouble / n
      val z = 1.96
      val z2 = z * z
      val center = p + z2 / (2.0 * n)
      val margin = z * math.sqrt(p * (1.0 - p) / n + z2 / (4.0 * n * n))
      val denominator = 1.0 + z2 / n
      val lowerBound = (center - margin) / denominator
      math.max(0.0, math.min(1.0, lowerBound))
    }
  }

  private def roundProbability(value: Double): Double = {
    BigDecimal.decimal(value)
      .setScale(2, scala.math.BigDecimal.RoundingMode.HALF_UP)
      .toDouble
  }

  def scanGroups(
    spark: SparkSession,
    datasetPath: String,
    groups: Seq[ScanGroup],
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    groupParallelism: Int = -1,
    fileParallelism: Int = -1,
    fileSampleRatio: Option[Double] = None,
    fileSampleMinFiles: Int = 10,
    allowlistMatcher: AllowlistMatcher = AllowlistMatcher.empty,
    allowlistInputRoot: Option[String] = None,
    progressRun: Option[ProgressRun] = None,
    retainPayloads: Boolean = true,
    csvHeadCache: CsvHeadCache = new CsvHeadCache()
  ): Seq[(ScanGroup, Seq[ScanResult], Seq[ScanError])] = {
    if (groups.isEmpty) {
      return Seq.empty
    }

    val parallelism = if (groupParallelism > 0) {
      resolveParallelism(groups.size, groupParallelism)
    } else {
      resolveGroupParallelism(spark, groups.size)
    }
    DriverLogger.debug("group_scan_parallelism", "groups" -> groups.size, "parallelism" -> parallelism)

    executeInParallel(parallelism, groups.map { group =>
      () => {
        DriverLogger.debug(
          "group_scan_dispatch",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "files" -> group.filePaths.size,
          "use_directory_identifier" -> group.useDirectoryIdentifier,
          "parallelism" -> parallelism
        )
        val (groupResults, groupErrors) =
          scanGroup(
            spark,
            datasetPath,
            group,
            rules,
            sampleRatio,
            timestamp,
            fileParallelism,
            fileSampleRatio,
            fileSampleMinFiles,
            allowlistMatcher,
            allowlistInputRoot,
            progressRun,
            csvHeadCache
          )
        DriverLogger.debug(
          "group_scan_recorded",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "result_rows" -> groupResults.size,
          "error_rows" -> groupErrors.size
        )
        if (retainPayloads) {
          (group, groupResults, groupErrors)
        } else {
          (group, Seq.empty, Seq.empty)
        }
      }
    })
  }

  def scanGroup(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    fileParallelism: Int = -1,
    fileSampleRatio: Option[Double] = None,
    fileSampleMinFiles: Int = 10,
    allowlistMatcher: AllowlistMatcher = AllowlistMatcher.empty,
    allowlistInputRoot: Option[String] = None,
    progressRun: Option[ProgressRun] = None,
    csvHeadCache: CsvHeadCache = new CsvHeadCache(),
    selectedSourceKeys: Option[Seq[String]] = None
  ): (Seq[ScanResult], Seq[ScanError]) = {
    DriverLogger.debug(
      "group_scan_start",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "files" -> group.filePaths.size,
      "sample_ratio" -> sampleRatio,
      "file_sample_ratio" -> fileSampleRatio.getOrElse("none"),
      "file_sample_min_files" -> fileSampleMinFiles,
      "use_directory_identifier" -> group.useDirectoryIdentifier,
      "schema_sampled" -> group.schemaSampled,
      "csv_has_header" -> group.csvHasHeader
    )
    if (group.schemaSampled && group.filePaths.size > 1) {
      val exactSplitResult = rescanSampledGroupWithExactSplit(
        spark,
        datasetPath,
        group,
        rules,
        sampleRatio,
        timestamp,
        "sampled_exact_split",
        fileParallelism,
        fileSampleRatio,
        fileSampleMinFiles,
        allowlistMatcher,
        allowlistInputRoot,
        progressRun,
        csvHeadCache
      )
      DriverLogger.debug(
        "group_scan_complete",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "schema" -> group.schemaSignature,
        "result_rows" -> exactSplitResult._1.size,
        "error_rows" -> exactSplitResult._2.size,
        "mode" -> "sampled_exact_split"
      )
      return exactSplitResult
    }

    if (!supportsBatchScan(group)) {
      val effectiveSelectedSourceKeys =
        selectedSourceKeys.getOrElse(resolveSelectedFileKeys(group, sampleRatio, fileSampleRatio, fileSampleMinFiles))
      val fallbackResult = scanGroupByFile(
        spark,
        datasetPath,
        group,
        rules,
        sampleRatio,
        timestamp,
        fileParallelism,
        allowlistMatcher,
        allowlistInputRoot,
        progressRun,
        csvHeadCache,
        fileSampleRatio,
        fileSampleMinFiles,
        selectedSourceKeys = Some(effectiveSelectedSourceKeys)
      )
      DriverLogger.debug(
        "group_scan_complete",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "schema" -> group.schemaSignature,
        "result_rows" -> fallbackResult._1.size,
        "error_rows" -> fallbackResult._2.size,
        "mode" -> "direct_file_scan"
      )
      return fallbackResult
    }

    val effectiveSelectedSourceKeys =
      selectedSourceKeys.getOrElse(resolveSelectedFileKeys(group, sampleRatio, fileSampleRatio, fileSampleMinFiles))

    try {
      val results = scanGroupBatch(
        spark,
        datasetPath,
        group,
        rules,
        sampleRatio,
        timestamp,
        fileSampleRatio,
        fileSampleMinFiles,
        allowlistMatcher,
        allowlistInputRoot,
        selectedSourceKeys = Some(effectiveSelectedSourceKeys)
      )
      progressRun.foreach { run =>
        persistProgressRecords(
          spark.sparkContext.hadoopConfiguration,
          run,
          "group",
          group.directoryPath,
          results,
          Seq.empty
        )
      }
      DriverLogger.debug(
        "group_scan_complete",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "schema" -> group.schemaSignature,
        "result_rows" -> results.size,
        "error_rows" -> 0,
        "mode" -> "group_batch_scan"
      )
      (results, Seq.empty)
    } catch {
      case NonFatal(e) =>
        DriverLogger.warn(
          "group_scan_fallback",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "files" -> group.filePaths.size,
          "reason" -> Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        )
        val errorMessage = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        DriverLogger.debug(
          "group_scan_fallback_requested",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "files" -> group.filePaths.size,
          "reason" -> errorMessage
        )
        if (group.schemaSampled) {
          DriverLogger.warn(
            "group_scan_fallback_execute",
            "directory" -> group.directoryPath,
            "format" -> group.format,
            "schema" -> group.schemaSignature,
            "files" -> group.filePaths.size,
            "mode" -> "schema_resplit"
          )
          val exactSplitResult = rescanSampledGroupWithExactSplit(
            spark,
            datasetPath,
            group,
            rules,
            sampleRatio,
            timestamp,
            "fallback_schema_resplit",
            fileParallelism,
            fileSampleRatio,
            fileSampleMinFiles,
            allowlistMatcher,
            allowlistInputRoot,
            progressRun,
            csvHeadCache
          )

          DriverLogger.debug(
            "group_scan_complete",
            "directory" -> group.directoryPath,
            "format" -> group.format,
            "schema" -> group.schemaSignature,
            "result_rows" -> exactSplitResult._1.size,
            "error_rows" -> exactSplitResult._2.size,
            "mode" -> "fallback_schema_resplit"
          )
          exactSplitResult
        } else {
          val fallbackResult = scanGroupByFile(
            spark,
            datasetPath,
            group,
            rules,
            sampleRatio,
            timestamp,
            fileParallelism,
            allowlistMatcher,
            allowlistInputRoot,
            progressRun,
            csvHeadCache,
            fileSampleRatio,
            fileSampleMinFiles,
            selectedSourceKeys = Some(effectiveSelectedSourceKeys)
          )
          DriverLogger.debug(
            "group_scan_complete",
            "directory" -> group.directoryPath,
            "format" -> group.format,
            "schema" -> group.schemaSignature,
            "result_rows" -> fallbackResult._1.size,
            "error_rows" -> fallbackResult._2.size,
            "mode" -> "fallback_file_scan"
          )
          fallbackResult
        }
    }
  }

  def scanGroupByFile(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    fileParallelism: Int = -1,
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
      selectedSourceKeys.getOrElse(resolveSelectedFileKeys(group, sampleRatio, fileSampleRatio, fileSampleMinFiles))
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
        val readOptions = resolveReadOptions(group, sourceKey)
        val logicalIdentifier = resolveLogicalIdentifier(group, datasetPath, sourceKey)
        DriverLogger.debug("group_scan_fallback_file_start", "file" -> physicalPath, "directory" -> group.directoryPath)
        val csvHasHeaderOverride =
          if (group.format == "csv" && group.schemaSampled) None else Some(group.csvHasHeader)
        sourceKey -> scanFileMetrics(
          spark,
          datasetPath,
          sourceKey,
          rules,
          effectiveSampleRatio,
          timestamp,
          csvHasHeaderOverride,
          formatOverride = Some(group.format),
          logicalIdentifierOverride = Some(logicalIdentifier),
          physicalPathOverride = Some(physicalPath),
          readOptions = readOptions,
          csvHeadCache = csvHeadCache
        )
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
                val fileResults = applyAllowlist(
                  spark.sparkContext.hadoopConfiguration,
                  datasetPath,
                  allowlistMatcher,
                  allowlistInputRoot,
                  buildScanResults(
                    datasetPath,
                    fileMetrics.scanTimestamp,
                    fileMetrics.fileIdentifier,
                    fileMetrics.sampledRowCount,
                    fileMetrics.nonEmptyValueCounts,
                    fileMetrics.matchCounts,
                    fileMetrics.sampleValues,
                    fileMetrics.fileSize,
                    fileMetrics.fileMtimeEpochMs
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
      val expectedScopeMetadata = successfulFileMetrics.map { fileMetrics =>
        fileMetrics.fileIdentifier -> (fileMetrics.fileSize -> fileMetrics.fileMtimeEpochMs)
      }.toMap
      val reviewScopeFileFingerprints = buildReviewScopeFileFingerprints(
        spark.sparkContext.hadoopConfiguration,
        datasetPath,
        reviewScopeFileIdentifiers,
        expectedScopeMetadata
      )
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

      buildScanResults(
        datasetPath,
        currentScanTimestamp(),
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
        buildScanResults(
          datasetPath,
          fileMetrics.scanTimestamp,
          fileMetrics.fileIdentifier,
          fileMetrics.sampledRowCount,
          fileMetrics.nonEmptyValueCounts,
          fileMetrics.matchCounts,
          fileMetrics.sampleValues,
          fileMetrics.fileSize,
          fileMetrics.fileMtimeEpochMs
        )
      }
    }
    val filteredFallbackResults = applyAllowlist(
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

  def scanGroupBatch(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    fileSampleRatio: Option[Double] = None,
    fileSampleMinFiles: Int = 10,
    allowlistMatcher: AllowlistMatcher = AllowlistMatcher.empty,
    allowlistInputRoot: Option[String] = None,
    selectedSourceKeys: Option[Seq[String]] = None
  ): Seq[ScanResult] = {
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
      selectedSourceKeys.getOrElse(resolveSelectedFileKeys(group, sampleRatio, fileSampleRatio, fileSampleMinFiles))
    val fileSamplingApplied = effectiveSelectedSourceKeys.size < group.filePaths.size
    val physicalPaths = effectiveSelectedSourceKeys.map(sourceKey => resolvePhysicalPath(group, sourceKey))
    withFileReadRetry(spark, physicalPaths, "group_batch_scan") {
      val effectiveRules = effectiveRulesForFormat(group.format, rules)
      val baseDf = readSource(spark, group.format, physicalPaths, group.csvHasHeader)
      val fileIdentifierColumn = if (group.useDirectoryIdentifier) {
        None
      } else {
        Some(resolveFileIdentifierColumn(baseDf.columns.toSeq))
      }
      val sourceDf = fileIdentifierColumn match {
        case Some(columnName) => baseDf.withColumn(columnName, input_file_name())
        case None => baseDf
      }
      DriverLogger.debug(
        "group_scan_batch_source_ready",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "columns" -> sourceDf.columns.length,
        "file_identifier_mode" -> fileIdentifierColumn.fold("directory")(identity)
      )

      val sampledDf = if (fileSamplingApplied || sampleRatio >= 1.0) {
        sourceDf
      } else {
        sourceDf.sample(withReplacement = false, sampleRatio)
      }

      fileIdentifierColumn match {
        case None =>
          val sampledRowCount = sampledDf.count()
          DriverLogger.debug(
            "group_scan_batch_sampled_rows",
            "directory" -> group.directoryPath,
            "sampled_rows" -> sampledRowCount,
            "mode" -> "directory_identifier"
          )
          if (sampledRowCount == 0L) {
            DriverLogger.debug(
              "group_scan_batch_complete",
              "directory" -> group.directoryPath,
              "result_rows" -> 0,
              "mode" -> "directory_identifier"
            )
            Seq.empty
          } else {
            val matchCounts = DetectionAggregator.aggregate(sampledDf, effectiveRules)
            val sampleValues = DetectionAggregator.sampleMatches(sampledDf, effectiveRules, matchCounts)
            val groupScanTimestamp = currentScanTimestamp()
            val reviewScopeFileIdentifiers = effectiveSelectedSourceKeys.map(sourceKey => resolveLogicalIdentifier(group, datasetPath, sourceKey))
            val expectedScopeMetadata = effectiveSelectedSourceKeys.map { sourceKey =>
              resolveLogicalIdentifier(group, datasetPath, sourceKey) ->
                (group.fileSizesByKey.getOrElse(sourceKey, 0L) -> group.fileMtimesByKey.getOrElse(sourceKey, 0L))
            }.toMap
            val reviewScopeFileFingerprints = buildReviewScopeFileFingerprints(
              spark.sparkContext.hadoopConfiguration,
              datasetPath,
              reviewScopeFileIdentifiers,
              expectedScopeMetadata
            )
            val results = buildScanResults(
              datasetPath,
              groupScanTimestamp,
              resolveDirectoryIdentifier(datasetPath, group.directoryPath),
              sampledRowCount,
              DetectionAggregator.countNonEmpty(sampledDf, matchCounts.map(_.columnName).distinct),
              matchCounts,
              sampleValues,
              effectiveSelectedSourceKeys.flatMap(group.fileSizesByKey.get).sum,
              effectiveSelectedSourceKeys.flatMap(group.fileMtimesByKey.get).foldLeft(0L)(math.max),
              reviewScopeFileIdentifiers,
              reviewScopeFileFingerprints
            )
            val filteredResults = applyAllowlist(
              spark.sparkContext.hadoopConfiguration,
              datasetPath,
              allowlistMatcher,
              allowlistInputRoot,
              results
            )
            DriverLogger.debug(
              "group_scan_batch_complete",
              "directory" -> group.directoryPath,
              "result_rows" -> filteredResults.size,
              "mode" -> "directory_identifier"
            )
            filteredResults
          }
        case Some(columnName) =>
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
            val matchCountsByFile = DetectionAggregator.aggregateByFile(sampledDf, columnName, effectiveRules)
            val sampleValuesByFile = DetectionAggregator.sampleMatchesByFile(sampledDf, columnName, effectiveRules, matchCountsByFile)
            val nonEmptyCountsByFile = DetectionAggregator.countNonEmptyByFile(sampledDf, columnName, matchCountsByFile.map(_.columnName).distinct)
            val groupScanTimestamp = currentScanTimestamp()
            val results = matchCountsByFile.flatMap { matchCount =>
              sampledRowsByFile.get(matchCount.fileIdentifier).flatMap { sampledRowCount =>
                val sourceKey = resolveSourceKeyForPhysicalPath(group, matchCount.fileIdentifier)
                buildScanResults(
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
            val filteredResults = applyAllowlist(
              spark.sparkContext.hadoopConfiguration,
              datasetPath,
              allowlistMatcher,
              allowlistInputRoot,
              results
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

  private def scanFileMetrics(
    spark: SparkSession,
    datasetPath: String,
    filePath: String,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    csvHasHeaderOverride: Option[Boolean] = None,
    formatOverride: Option[String] = None,
    logicalIdentifierOverride: Option[String] = None,
    physicalPathOverride: Option[String] = None,
    readOptions: ScanReadOptions = ScanReadOptions(),
    csvHeadCache: CsvHeadCache = new CsvHeadCache()
  ): Either[ScanError, FileScanMetrics] = {
    val physicalPath = physicalPathOverride.getOrElse(filePath)
    val fileIdentifier = logicalIdentifierOverride.getOrElse(resolveRelativeIdentifier(datasetPath, physicalPath))
    DriverLogger.debug("scan_file_start", "file" -> physicalPath, "file_identifier" -> fileIdentifier, "sample_ratio" -> sampleRatio)

    try {
      withFileReadRetry(spark, Seq(physicalPath), "file_scan") {
        val fileStatus = new org.apache.hadoop.fs.Path(physicalPath).getFileSystem(spark.sparkContext.hadoopConfiguration)
          .getFileStatus(new org.apache.hadoop.fs.Path(physicalPath))
        val format = formatOverride.orElse(detectPhysicalFormat(spark.sparkContext.hadoopConfiguration, physicalPath)).getOrElse {
          DriverLogger.debug("scan_file_error", "file" -> physicalPath, "file_identifier" -> fileIdentifier, "reason" -> "Unsupported file format")
          return Left(ScanError(datasetPath, timestamp, fileIdentifier, s"Unsupported file format: $fileIdentifier"))
        }
        val effectiveRules = effectiveRulesForFormat(format, rules)

        val csvHasHeader = if (format == "csv") {
          csvHasHeaderOverride.getOrElse(detectCsvHasHeader(spark, physicalPath, csvHeadCache))
        } else {
          true
        }
        val sourceDf = readSource(spark, format, Seq(physicalPath), csvHasHeader, readOptions)
        val sampledDf = if (sampleRatio >= 1.0) sourceDf else sourceDf.sample(withReplacement = false, sampleRatio)

        val sampledRowCount = sampledDf.count()
        DriverLogger.debug(
          "scan_file_sampled_rows",
          "file" -> physicalPath,
          "file_identifier" -> fileIdentifier,
          "sampled_rows" -> sampledRowCount
        )

        if (sampledRowCount == 0L) {
          DriverLogger.debug("scan_file_complete", "file" -> physicalPath, "file_identifier" -> fileIdentifier, "matches" -> 0)
          Right(FileScanMetrics(
            fileIdentifier,
            sampledRowCount,
            Map.empty,
            Seq.empty,
            Map.empty,
            fileStatus.getLen,
            fileStatus.getModificationTime,
            currentScanTimestamp()
          ))
        } else {
          val matchCounts = DetectionAggregator.aggregate(sampledDf, effectiveRules)
          val nonEmptyValueCounts = DetectionAggregator.countNonEmpty(
            sampledDf,
            DetectionAggregator.columnsCoveredByRules(sampledDf.columns.toSeq, effectiveRules)
          )
          val sampleValues = DetectionAggregator.sampleMatches(sampledDf, effectiveRules, matchCounts)
          DriverLogger.debug(
            "scan_file_complete",
            "file" -> physicalPath,
            "file_identifier" -> fileIdentifier,
            "matches" -> matchCounts.size
          )
          Right(FileScanMetrics(
            fileIdentifier,
            sampledRowCount,
            nonEmptyValueCounts,
            matchCounts,
            sampleValues,
            fileStatus.getLen,
            fileStatus.getModificationTime,
            currentScanTimestamp()
          ))
        }
      }
    } catch {
      case NonFatal(e) =>
        val errorMessage = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        DriverLogger.debug("scan_file_error", "file" -> physicalPath, "file_identifier" -> fileIdentifier, "reason" -> errorMessage)
        Left(ScanError(datasetPath, timestamp, fileIdentifier, errorMessage))
    }
  }

  private def scanFile(
    spark: SparkSession,
    datasetPath: String,
    filePath: String,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String
  ): Either[ScanError, Seq[ScanResult]] = {
    scanFileMetrics(spark, datasetPath, filePath, rules, sampleRatio, timestamp).map { fileMetrics =>
      buildScanResults(
        datasetPath,
        fileMetrics.scanTimestamp,
        fileMetrics.fileIdentifier,
        fileMetrics.sampledRowCount,
        fileMetrics.nonEmptyValueCounts,
        fileMetrics.matchCounts,
        fileMetrics.sampleValues,
        fileMetrics.fileSize,
        fileMetrics.fileMtimeEpochMs
      )
    }
  }

  private def resolveSelectedFileKeys(
    group: ScanGroup,
    sampleRatio: Double,
    fileSampleRatio: Option[Double],
    fileSampleMinFiles: Int
  ): Seq[String] = {
    fileSampleRatio match {
      case Some(ratio) if group.filePaths.size > fileSampleMinFiles =>
        val sampledKeys = selectSampledFileKeys(group.filePaths, ratio)
        if (sampledKeys.size < group.filePaths.size) {
          if (sampleRatio < 1.0) {
            DriverLogger.warn(
              "group_scan_row_sampling_ignored",
              "directory" -> group.directoryPath,
              "format" -> group.format,
              "schema" -> group.schemaSignature,
              "sample_ratio" -> sampleRatio,
              "file_sample_ratio" -> ratio,
              "file_sample_min_files" -> fileSampleMinFiles,
              "selected_files" -> sampledKeys.size,
              "total_files" -> group.filePaths.size
            )
          }
          DriverLogger.debug(
            "group_scan_file_sampling_applied",
            "directory" -> group.directoryPath,
            "format" -> group.format,
            "schema" -> group.schemaSignature,
            "file_sample_ratio" -> ratio,
            "file_sample_min_files" -> fileSampleMinFiles,
            "selected_files" -> sampledKeys.size,
            "total_files" -> group.filePaths.size
          )
        } else {
          DriverLogger.debug(
            "group_scan_file_sampling_skipped",
            "directory" -> group.directoryPath,
            "format" -> group.format,
            "schema" -> group.schemaSignature,
            "file_sample_ratio" -> ratio,
            "file_sample_min_files" -> fileSampleMinFiles,
            "selected_files" -> sampledKeys.size,
            "total_files" -> group.filePaths.size,
            "file_sample_skipped_reason" -> "no_reduction"
          )
        }
        sampledKeys
      case Some(ratio) =>
        DriverLogger.debug(
          "group_scan_file_sampling_skipped",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "file_sample_ratio" -> ratio,
          "file_sample_min_files" -> fileSampleMinFiles,
          "total_files" -> group.filePaths.size,
          "file_sample_skipped_reason" -> "below_threshold"
        )
        group.filePaths
      case None =>
        group.filePaths
    }
  }

  def selectSampledFileKeys(fileKeys: Seq[String], fileSampleRatio: Double): Seq[String] = {
    require(fileKeys.nonEmpty, "fileKeys must not be empty")
    require(fileSampleRatio > 0.0 && fileSampleRatio <= 1.0, "fileSampleRatio must be > 0.0 and <= 1.0")

    val sampleSize = math.max(1, math.min(fileKeys.size, math.ceil(fileKeys.size * fileSampleRatio).toInt))
    val selectedKeySet = Random.shuffle(fileKeys.indices.toVector).take(sampleSize).map(fileKeys).toSet
    fileKeys.filter(selectedKeySet.contains)
  }

  private def rescanSampledGroupWithExactSplit(
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
    allowlistMatcher: AllowlistMatcher,
    allowlistInputRoot: Option[String],
    progressRun: Option[ProgressRun],
    csvHeadCache: CsvHeadCache
  ): (Seq[ScanResult], Seq[ScanError]) = {
    val (splitGroups, splitErrors) = splitGroupBySchema(
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
      val (groupResults, groupErrors) = scanGroup(
        spark,
        datasetPath,
        rescannedGroup,
        rules,
        sampleRatio,
        timestamp,
        fileParallelism,
        fileSampleRatio,
        fileSampleMinFiles,
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
