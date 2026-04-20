package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.detect.DetectionAggregator
import io.github.jonggeun2001.privyspark.format.ByteProbe.detectPhysicalFormat
import io.github.jonggeun2001.privyspark.format.CsvInference._
import io.github.jonggeun2001.privyspark.config.SuppressionSet
import io.github.jonggeun2001.privyspark.fsio.ManagedPaths.deleteStagingPath
import io.github.jonggeun2001.privyspark.scan.DirectoryScanner.splitGroupBySchema
import io.github.jonggeun2001.privyspark.util.ParallelismConfig._
import io.github.jonggeun2001.privyspark.util.PathIdentifiers._
import io.github.jonggeun2001.privyspark.util.DriverLogger
import io.github.jonggeun2001.privyspark.fsio.RetryIO.withFileReadRetry
import io.github.jonggeun2001.privyspark.scan.SourceExpansion.supportsBatchScan
import io.github.jonggeun2001.privyspark.model.{FileScanMetrics, MatchCount, PiiRule, ProgressRun, SampleValue, ScanError, ScanGroup, ScanReadOptions, ScanResult}
import io.github.jonggeun2001.privyspark.progress.ProgressIO.persistProgressRecords
import io.github.jonggeun2001.privyspark.review.{AllowlistEvaluation, AllowlistMatcher, FileIdentifierResolver, RecordedFileFingerprint, ReviewScopeFingerprintCodec, ReviewScopeIdentifierCodec}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, input_file_name, lit, pmod, xxhash64}
import org.apache.hadoop.fs.Path

import java.io.{InputStream, OutputStream}
import java.time.Instant
import java.util.UUID
import java.util.zip.CRC32
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.NonFatal

private[privyspark] object GroupScanner {
  private final case class StagedFileSnapshot(
    stagedRoot: String,
    stagedPath: String,
    recordedFingerprint: RecordedFileFingerprint
  )

  private val CopyBufferSize = 8192
  private val DeterministicSampleBuckets = 1000000L

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
          review_scope_file_identifiers = ReviewScopeIdentifierCodec.encode(reviewScopeFileIdentifiers),
          review_scope_file_fingerprints = reviewScopeFileFingerprints
        )
      }
    }
  }

  private def comparableResultPayloads(results: Seq[ScanResult]): Seq[(String, String, String, Long, Long, Double, Double, Double)] =
    results
      .map(result =>
        (
          result.file_identifier,
          result.column_name,
          result.pii_type,
          result.match_count,
          result.sampled_row_count,
          result.match_ratio,
          result.non_empty_match_ratio,
          result.confidence
        )
      )
      .sortBy(value => (value._1, value._2, value._3))

  private def stageFileSnapshot(
    conf: org.apache.hadoop.conf.Configuration,
    datasetPath: String,
    group: ScanGroup,
    sourceKey: String
  ): Either[String, StagedFileSnapshot] = {
    val sourcePath = new Path(resolvePhysicalPath(group, sourceKey))
    val sourceFs = sourcePath.getFileSystem(conf)
    val logicalIdentifier = resolveLogicalIdentifier(group, datasetPath, sourceKey)
    val stagingBase = new Path(sourceFs.getHomeDirectory, ".privyspark-scan-staging")
    val stagingRoot = new Path(stagingBase, UUID.randomUUID().toString)
    val stagedPath = new Path(stagingRoot, sourcePath.getName)
    var inputStream: InputStream = null
    var outputStream: OutputStream = null

    try {
      val sourceStatus = sourceFs.getFileStatus(sourcePath)
      val expectedFileSize = group.fileSizesByKey.getOrElse(sourceKey, sourceStatus.getLen)
      val expectedFileMtimeEpochMs = group.fileMtimesByKey.getOrElse(sourceKey, sourceStatus.getModificationTime)
      if (!sourceFs.exists(stagingBase) && !sourceFs.mkdirs(stagingBase)) {
        return Left(s"Scan staging base creation failed: ${stagingBase.toString}")
      }
      if (!sourceFs.mkdirs(stagingRoot) && !sourceFs.exists(stagingRoot)) {
        return Left(s"Scan staging directory creation failed: ${stagingRoot.toString}")
      }

      inputStream = sourceFs.open(sourcePath)
      outputStream = sourceFs.create(stagedPath, true)
      val crc32 = new CRC32()
      val buffer = new Array[Byte](CopyBufferSize)
      var bytesRead = inputStream.read(buffer)
      while (bytesRead >= 0) {
        if (bytesRead > 0) {
          outputStream.write(buffer, 0, bytesRead)
          crc32.update(buffer, 0, bytesRead)
        }
        bytesRead = inputStream.read(buffer)
      }

      Right(StagedFileSnapshot(
        stagedRoot = stagingRoot.toString,
        stagedPath = stagedPath.toString,
        recordedFingerprint = RecordedFileFingerprint(
          fileIdentifier = logicalIdentifier,
          fileSize = expectedFileSize,
          fileMtimeEpochMs = expectedFileMtimeEpochMs,
          fileChecksumAlgo = FileIdentifierResolver.DefaultChecksumAlgo,
          fileChecksum = f"${crc32.getValue}%08x"
        )
      ))
    } catch {
      case NonFatal(e) =>
        deleteStagingPath(conf, stagingRoot.toString)
        Left(s"Scan staging failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}")
    } finally {
      if (outputStream != null) {
        try outputStream.close() catch {
          case NonFatal(_) => ()
        }
      }
      if (inputStream != null) {
        try inputStream.close() catch {
          case NonFatal(_) => ()
        }
      }
    }
  }

  private def captureRecordedFingerprint(
    conf: org.apache.hadoop.conf.Configuration,
    fileIdentifier: String,
    physicalPath: String,
    fileSize: Long,
    fileMtimeEpochMs: Long
  ): RecordedFileFingerprint = {
    val checksum = crc32Hex(conf, physicalPath)
    recordedFingerprint(fileIdentifier, fileSize, fileMtimeEpochMs, checksum)
  }

  private def recordedFingerprint(
    fileIdentifier: String,
    fileSize: Long,
    fileMtimeEpochMs: Long,
    checksum: String
  ): RecordedFileFingerprint = {
    RecordedFileFingerprint(
      fileIdentifier = fileIdentifier,
      fileSize = fileSize,
      fileMtimeEpochMs = fileMtimeEpochMs,
      fileChecksumAlgo = FileIdentifierResolver.DefaultChecksumAlgo,
      fileChecksum = checksum
    )
  }

  private def crc32Hex(
    conf: org.apache.hadoop.conf.Configuration,
    physicalPath: String
  ): String = {
    val sourcePath = new Path(physicalPath)
    val fs = sourcePath.getFileSystem(conf)
    var inputStream: InputStream = null

    try {
      inputStream = fs.open(sourcePath)
      val crc32 = new CRC32()
      val buffer = new Array[Byte](CopyBufferSize)
      var bytesRead = inputStream.read(buffer)
      while (bytesRead >= 0) {
        if (bytesRead > 0) {
          crc32.update(buffer, 0, bytesRead)
        }
        bytesRead = inputStream.read(buffer)
      }
      f"${crc32.getValue}%08x"
    } finally {
      if (inputStream != null) {
        try inputStream.close() catch {
          case NonFatal(_) => ()
        }
      }
    }
  }

  private def parseReviewScopeIdentifiers(rawValue: String): Seq[String] = {
    ReviewScopeIdentifierCodec.decode(rawValue) match {
      case Right(identifiers) =>
        identifiers
      case Left(errorMessage) =>
        throw new IllegalArgumentException(errorMessage)
    }
  }

  private def encodeRecordedFingerprint(recordedFingerprint: Option[RecordedFileFingerprint]): String =
    recordedFingerprint.map(fingerprint => ReviewScopeFingerprintCodec.encode(Seq(fingerprint))).getOrElse("")

  private def scanSourceKeyMetrics(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    sourceKey: String,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    suppressions: SuppressionSet,
    csvHeadCache: CsvHeadCache,
    captureRecordedFingerprintWhenMissing: Boolean,
    stagedSnapshot: Option[StagedFileSnapshot] = None,
    injectedFileIdentifierColumn: Option[(String, String)] = None
  ): Either[ScanError, FileScanMetrics] = {
    val physicalPath = resolvePhysicalPath(group, sourceKey)
    val logicalIdentifier = resolveLogicalIdentifier(group, datasetPath, sourceKey)
    val readOptions = resolveReadOptions(group, sourceKey)
    val csvHasHeaderOverride =
      if (group.format == "csv" && group.schemaSampled) None else Some(group.csvHasHeader)

    scanFileMetrics(
      spark,
      datasetPath,
      sourceKey,
      rules,
      sampleRatio,
      timestamp,
      csvHasHeaderOverride,
      formatOverride = Some(group.format),
      logicalIdentifierOverride = Some(logicalIdentifier),
      physicalPathOverride = Some(stagedSnapshot.map(_.stagedPath).getOrElse(physicalPath)),
      fileSizeOverride = stagedSnapshot.map(_.recordedFingerprint.fileSize).orElse(group.fileSizesByKey.get(sourceKey)),
      fileMtimeEpochMsOverride = stagedSnapshot.map(_.recordedFingerprint.fileMtimeEpochMs).orElse(group.fileMtimesByKey.get(sourceKey)),
      recordedFingerprint = stagedSnapshot.map(_.recordedFingerprint),
      readOptions = readOptions,
      suppressions = suppressions,
      csvHeadCache = csvHeadCache,
      captureRecordedFingerprintWhenMissing = captureRecordedFingerprintWhenMissing,
      injectedFileIdentifierColumn = injectedFileIdentifierColumn
    )
  }

  private def scanSourceKeyUsingSnapshot(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    sourceKey: String,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    suppressions: SuppressionSet,
    csvHeadCache: CsvHeadCache,
    injectedFileIdentifierColumn: Option[(String, String)] = None
  ): Either[ScanError, FileScanMetrics] = {
    val conf = spark.sparkContext.hadoopConfiguration
    val logicalIdentifier = resolveLogicalIdentifier(group, datasetPath, sourceKey)

    stageFileSnapshot(conf, datasetPath, group, sourceKey) match {
      case Right(stagedSnapshot) =>
        try {
          scanSourceKeyMetrics(
            spark,
            datasetPath,
            group,
            sourceKey,
            rules,
            sampleRatio,
            timestamp,
            suppressions,
            csvHeadCache,
            captureRecordedFingerprintWhenMissing = false,
            stagedSnapshot = Some(stagedSnapshot),
            injectedFileIdentifierColumn = injectedFileIdentifierColumn
          )
        } finally {
          deleteStagingPath(conf, stagedSnapshot.stagedRoot)
        }
      case Left(errorMessage) =>
        Left(ScanError(datasetPath, timestamp, logicalIdentifier, errorMessage))
    }
  }

  private def logReviewSnapshotStart(
    mode: String,
    matchedFiles: Int,
    selectedFiles: Int
  ): Unit = {
    DriverLogger.debug(
      "group_scan_review_snapshot_start",
      "mode" -> mode,
      "matched_files" -> matchedFiles,
      "selected_files" -> selectedFiles
    )
  }

  private def logReviewSnapshotFile(
    mode: String,
    physicalPath: String,
    fileIdentifier: String
  ): Unit = {
    DriverLogger.debug(
      "group_scan_review_snapshot_file",
      "mode" -> mode,
      "file" -> physicalPath,
      "file_identifier" -> fileIdentifier
    )
  }

  private def logReviewSnapshotSkipped(
    mode: String,
    matchedFiles: Int,
    selectedFiles: Int,
    physicalPath: Option[String] = None,
    fileIdentifier: Option[String] = None
  ): Unit = {
    val baseFields = Seq(
      "mode" -> mode,
      "matched_files" -> matchedFiles,
      "selected_files" -> selectedFiles
    )
    val optionalFields = Seq(
      physicalPath.map("file" -> _),
      fileIdentifier.map("file_identifier" -> _)
    ).flatten
    DriverLogger.debug("group_scan_review_snapshot_skipped", (baseFields ++ optionalFields): _*)
  }

  private def rescanBatchMatchedFilesWithSnapshots(
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
      logReviewSnapshotSkipped("batch", matchedFiles = 0, selectedFiles = selectedFileCount)
      Seq.empty
    } else {
      logReviewSnapshotStart("batch", matchedSourceKeys.size, selectedFileCount)
      val parallelism = resolveFileParallelism(spark, matchedSourceKeys.size)
      val rescannedMetrics = executeInParallel(parallelism, matchedSourceKeys.map { sourceKey =>
        () => {
          val physicalPath = resolvePhysicalPath(group, sourceKey)
          val logicalIdentifier = resolveLogicalIdentifier(group, datasetPath, sourceKey)
          val batchFileIdentifierValue = batchFileIdentifierValuesBySourceKey.getOrElse(sourceKey, physicalPath)
          logReviewSnapshotFile("batch", physicalPath, logicalIdentifier)
          sourceKey -> scanSourceKeyUsingSnapshot(
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

      val results = matchedSourceKeys.flatMap { sourceKey =>
        val fileIdentifier = resolveLogicalIdentifier(group, datasetPath, sourceKey)
        metricMap.get(fileIdentifier).toSeq.flatMap { fileMetrics =>
          buildScanResults(
            datasetPath,
            fileMetrics.scanTimestamp,
            fileMetrics.fileIdentifier,
            fileMetrics.sampledRowCount,
            fileMetrics.nonEmptyValueCounts,
            fileMetrics.matchCounts,
            fileMetrics.sampleValues,
            fileMetrics.fileSize,
            fileMetrics.fileMtimeEpochMs,
            reviewScopeFileFingerprints = encodeRecordedFingerprint(fileMetrics.recordedFingerprint)
          )
        }
      }
      results
    }
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
        val reviewScopeIdentifiers = parseReviewScopeIdentifiers(result.review_scope_file_identifiers)
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

  private def sampleRowsDeterministically(sourceDf: org.apache.spark.sql.DataFrame, sampleRatio: Double): org.apache.spark.sql.DataFrame = {
    if (sampleRatio >= 1.0) {
      sourceDf
    } else {
      val sampleThreshold = math.round(sampleRatio * DeterministicSampleBuckets.toDouble)
      if (sampleThreshold <= 0L) {
        sourceDf.limit(0)
      } else {
        val hashInputs =
          if (sourceDf.columns.isEmpty) {
            Seq(lit("__privyspark_empty__"))
          } else {
            sourceDf.columns.toSeq.map(columnName =>
              coalesce(col(columnName).cast("string"), lit("__privyspark_null__"))
            )
          }
        val bucket = pmod(xxhash64(hashInputs: _*), lit(DeterministicSampleBuckets))
        sourceDf.where(bucket < lit(sampleThreshold))
      }
    }
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
    suppressions: SuppressionSet = SuppressionSet.empty,
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
            suppressions,
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
    suppressions: SuppressionSet = SuppressionSet.empty,
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
        suppressions,
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

    if (!supportsBatchScan(group) || group.useDirectoryIdentifier) {
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
        suppressions,
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
        "mode" -> (if (group.useDirectoryIdentifier) "directory_file_scan" else "direct_file_scan")
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
        suppressions,
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
            suppressions,
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
            suppressions,
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
        val logicalIdentifier = resolveLogicalIdentifier(group, datasetPath, sourceKey)
        DriverLogger.debug("group_scan_fallback_file_start", "file" -> physicalPath, "directory" -> group.directoryPath)
        val fileMetrics = if (group.useDirectoryIdentifier) {
          scanSourceKeyUsingSnapshot(
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
          scanSourceKeyMetrics(
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
              logReviewSnapshotSkipped(
                "file",
                matchedFiles = 0,
                selectedFiles = 1,
                physicalPath = Some(physicalPath),
                fileIdentifier = Some(logicalIdentifier)
              )
              Right(provisionalMetrics)
            } else {
              logReviewSnapshotStart("file", matchedFiles = 1, selectedFiles = 1)
              logReviewSnapshotFile("file", physicalPath, logicalIdentifier)
              val provisionalResults = buildScanResults(
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
              scanSourceKeyUsingSnapshot(
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
                val snapshotResults = buildScanResults(
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
                if (comparableResultPayloads(provisionalResults) == comparableResultPayloads(snapshotResults)) {
                  Right(snapshotMetrics)
                } else {
                  Left(ScanError(datasetPath, timestamp, logicalIdentifier, "Review snapshot changed during rescan"))
                }
              }
            }
          }
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
                    fileMetrics.fileMtimeEpochMs,
                    reviewScopeFileFingerprints = encodeRecordedFingerprint(fileMetrics.recordedFingerprint)
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
          fileMetrics.fileMtimeEpochMs,
          reviewScopeFileFingerprints = encodeRecordedFingerprint(fileMetrics.recordedFingerprint)
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
      selectedSourceKeys.getOrElse(resolveSelectedFileKeys(group, sampleRatio, fileSampleRatio, fileSampleMinFiles))
    val fileSamplingApplied = effectiveSelectedSourceKeys.size < group.filePaths.size
    val hasDuplicateSelectedSourceKeys = effectiveSelectedSourceKeys.distinct.size != effectiveSelectedSourceKeys.size
    val physicalPaths = effectiveSelectedSourceKeys.map(sourceKey => resolvePhysicalPath(group, sourceKey))
    val effectiveSampleRatio = if (fileSamplingApplied) 1.0 else sampleRatio
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

      val sampledDf = if (fileSamplingApplied) sourceDf else sampleRowsDeterministically(sourceDf, sampleRatio)

      fileIdentifierColumn match {
        case None =>
          throw new IllegalStateException("scanGroupBatch does not support directory identifiers; use scanGroupByFile")
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
            val groupScanTimestamp = currentScanTimestamp()
            val provisionalResults = matchCountsByFile.flatMap { matchCount =>
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
            val snapshotResults = rescanBatchMatchedFilesWithSnapshots(
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
            if (!hasDuplicateSelectedSourceKeys && comparableResultPayloads(provisionalResults) != comparableResultPayloads(snapshotResults)) {
              throw new IllegalStateException(s"Review snapshot changed during batch rescan: ${group.directoryPath}")
            }
            val filteredResults = applyAllowlist(
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
    fileSizeOverride: Option[Long] = None,
    fileMtimeEpochMsOverride: Option[Long] = None,
    recordedFingerprint: Option[RecordedFileFingerprint] = None,
    readOptions: ScanReadOptions = ScanReadOptions(),
    suppressions: SuppressionSet = SuppressionSet.empty,
    csvHeadCache: CsvHeadCache = new CsvHeadCache(),
    captureRecordedFingerprintWhenMissing: Boolean = true,
    injectedFileIdentifierColumn: Option[(String, String)] = None
  ): Either[ScanError, FileScanMetrics] = {
    val physicalPath = physicalPathOverride.getOrElse(filePath)
    val fileIdentifier = logicalIdentifierOverride.getOrElse(resolveRelativeIdentifier(datasetPath, physicalPath))
    DriverLogger.debug("scan_file_start", "file" -> physicalPath, "file_identifier" -> fileIdentifier, "sample_ratio" -> sampleRatio)

    try {
      withFileReadRetry(spark, Seq(physicalPath), "file_scan") {
        val fileStatus = new org.apache.hadoop.fs.Path(physicalPath).getFileSystem(spark.sparkContext.hadoopConfiguration)
          .getFileStatus(new org.apache.hadoop.fs.Path(physicalPath))
        val effectiveFileSize = fileSizeOverride.getOrElse(fileStatus.getLen)
        val effectiveFileMtimeEpochMs = fileMtimeEpochMsOverride.getOrElse(fileStatus.getModificationTime)
        val effectiveRecordedFingerprint = recordedFingerprint.orElse {
          if (captureRecordedFingerprintWhenMissing) {
            Some(captureRecordedFingerprint(
              spark.sparkContext.hadoopConfiguration,
              fileIdentifier,
              physicalPath,
              effectiveFileSize,
              effectiveFileMtimeEpochMs
            ))
          } else {
            None
          }
        }
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
        val baseDf = readSource(spark, format, Seq(physicalPath), csvHasHeader, readOptions)
        val sourceDf = injectedFileIdentifierColumn match {
          case Some((columnName, value)) => baseDf.withColumn(columnName, lit(value))
          case None => baseDf
        }
        val sampledDf = sampleRowsDeterministically(sourceDf, sampleRatio)

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
            effectiveFileSize,
            effectiveFileMtimeEpochMs,
            currentScanTimestamp(),
            effectiveRecordedFingerprint
          ))
        } else {
          val matchCounts = DetectionAggregator.aggregate(sampledDf, effectiveRules, suppressions = suppressions)
          val nonEmptyValueCounts = DetectionAggregator.countNonEmpty(
            sampledDf,
            DetectionAggregator.columnsCoveredByRules(sampledDf.columns.toSeq, effectiveRules, suppressions)
          )
          val sampleValues = DetectionAggregator.sampleMatches(sampledDf, effectiveRules, matchCounts, suppressions = suppressions)
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
            effectiveFileSize,
            effectiveFileMtimeEpochMs,
            currentScanTimestamp(),
            effectiveRecordedFingerprint
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
    timestamp: String,
    suppressions: SuppressionSet = SuppressionSet.empty
  ): Either[ScanError, Seq[ScanResult]] = {
    scanFileMetrics(spark, datasetPath, filePath, rules, sampleRatio, timestamp, suppressions = suppressions).map { fileMetrics =>
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
    suppressions: SuppressionSet,
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
