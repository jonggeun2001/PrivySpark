package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.cli.ReviewApplyCliConfig
import io.github.jonggeun2001.privyspark.format.ByteProbe.detectPhysicalFormat
import io.github.jonggeun2001.privyspark.format.CsvInference.{XlsxFormat, readSource}
import io.github.jonggeun2001.privyspark.model.ScanReadOptions
import io.github.jonggeun2001.privyspark.util.DriverLogger
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID
import scala.collection.JavaConverters._

object ReviewApplyCommand {
  private final case class ReviewDecision(
    datasetPath: String,
    fileIdentifier: String,
    reviewStatus: String,
    columnName: String,
    piiType: String,
    reviewReason: String,
    sourceRunId: String,
    reviewScopeFileIdentifiers: Seq[String],
    reviewScopeFileFingerprints: Seq[RecordedFileFingerprint],
    scanFileSize: Long,
    scanFileMtimeEpochMs: Long
  )

  def run(spark: SparkSession, config: ReviewApplyCliConfig): Unit = {
    DriverLogger.info(
      "review_apply_start",
      "scan_results" -> config.scanResultsPath,
      "input_root" -> config.inputRoot,
      "allowlist" -> config.allowlistPath,
      "reviewer" -> config.reviewer,
      "dry_run" -> config.dryRun
    )
    val conf = spark.sparkContext.hadoopConfiguration
    val reviewDecisions = loadReviewDecisions(spark, config)
    val reviewedAt = Instant.now().toString
    val resolvedDecisions = reviewDecisions.map { decision =>
      decision -> concreteScopeIdentifiers(conf, config, decision)
    }
    val affectedKeys = resolvedDecisions.flatMap {
      case (decision, concreteIdentifiers) =>
        concreteIdentifiers.map(identifier =>
          AllowlistKey(decision.datasetPath, identifier, decision.columnName, decision.piiType)
        )
    }.toSet
    val stagedEntries = resolvedDecisions.flatMap {
      case (decision, concreteIdentifiers) if decision.reviewStatus == ReviewStatus.FalsePositive =>
        val fingerprints = concreteIdentifiers.flatMap { concreteIdentifier =>
          FileIdentifierResolver.resolveFingerprints(conf, config.inputRoot, concreteIdentifier) match {
            case Right(resolvedFingerprints) =>
              resolvedFingerprints
            case Left(errorMessage) =>
              throw new IllegalArgumentException(s"Failed to resolve $concreteIdentifier: $errorMessage")
          }
        }
        validateScanMetadata(decision, fingerprints)
        fingerprints.map { fingerprint =>
          AllowlistEntry(
            datasetPath = decision.datasetPath,
            fileIdentifier = fingerprint.fileIdentifier,
            columnName = decision.columnName,
            piiType = decision.piiType,
            reason = decision.reviewReason,
            reviewer = config.reviewer,
            reviewedAt = reviewedAt,
            sourceRunId = decision.sourceRunId,
            fileSize = fingerprint.fileSize,
            fileMtimeEpochMs = fingerprint.fileMtimeEpochMs,
            fileChecksumAlgo = fingerprint.fileChecksumAlgo,
            fileChecksum = fingerprint.fileChecksum
          )
        }
      case _ =>
        Seq.empty
    }

    val existingEntries = loadExistingEntries(conf, config.allowlistPath)
    val mergedEntries = (existingEntries.filterNot(entry => affectedKeys.contains(entry.key)) ++ stagedEntries)
      .groupBy(_.key)
      .map {
        case (_, groupedEntries) => groupedEntries.last
      }
      .toSeq
      .sortBy(entry => (entry.datasetPath, entry.fileIdentifier, entry.columnName, entry.piiType))

    DriverLogger.info(
      "review_apply_ready",
      "review_rows" -> reviewDecisions.size,
      "affected_keys" -> affectedKeys.size,
      "staged_entries" -> stagedEntries.size,
      "final_entries" -> mergedEntries.size,
      "dry_run" -> config.dryRun
    )

    if (!config.dryRun) {
      writeAllowlist(conf, config.allowlistPath, mergedEntries)
    }
  }

  private def loadReviewDecisions(
    spark: SparkSession,
    config: ReviewApplyCliConfig
  ): Seq[ReviewDecision] = {
    val df = readScanResults(spark, config.scanResultsPath)
    val normalizedColumns = df.columns.map(columnName => columnName.toLowerCase -> columnName).toMap
    val requiredColumns = Seq(
      "dataset_path",
      "file_identifier",
      "column_name",
      "pii_type",
      "review_status",
      "review_reason",
      "file_size",
      "file_mtime_epoch_ms"
    )
    requiredColumns.foreach { columnName =>
      require(normalizedColumns.contains(columnName), s"scan_results is missing required column: $columnName")
    }
    val selectedColumnNames =
      (requiredColumns ++ Seq("source_run_id", "review_scope_file_identifiers", "review_scope_file_fingerprints"))
        .flatMap(normalizedColumns.get)
        .distinct
    val projectedDf = df.select(selectedColumnNames.map(df.col): _*)

    projectedDf.toLocalIterator().asScala.flatMap { row =>
      val reviewStatus = valueOf(row, normalizedColumns("review_status"))
      ReviewStatus.normalize(reviewStatus) match {
        case Some(normalizedReviewStatus) =>
          val reviewReason = valueOf(row, normalizedColumns("review_reason")).trim
          if (normalizedReviewStatus == ReviewStatus.FalsePositive) {
            require(reviewReason.nonEmpty, s"review_reason is required for false_positive: ${valueOf(row, normalizedColumns("file_identifier"))}")
          }
          Some(ReviewDecision(
            datasetPath = valueOf(row, normalizedColumns("dataset_path")),
            fileIdentifier = valueOf(row, normalizedColumns("file_identifier")),
            reviewStatus = normalizedReviewStatus,
            columnName = valueOf(row, normalizedColumns("column_name")),
            piiType = valueOf(row, normalizedColumns("pii_type")),
            reviewReason = reviewReason,
            sourceRunId = normalizedColumns.get("source_run_id").map(columnName => valueOf(row, columnName)).getOrElse(""),
            reviewScopeFileIdentifiers = normalizedColumns.get("review_scope_file_identifiers")
              .map(columnName => parseScopeIdentifiers(valueOf(row, columnName)))
              .getOrElse(Seq.empty),
            reviewScopeFileFingerprints = normalizedColumns.get("review_scope_file_fingerprints")
              .map(columnName => parseScopeFingerprints(valueOf(row, columnName)))
              .getOrElse(Seq.empty),
            scanFileSize = valueOf(row, normalizedColumns("file_size")).toLong,
            scanFileMtimeEpochMs = valueOf(row, normalizedColumns("file_mtime_epoch_ms")).toLong
          ))
        case None =>
          throw new IllegalArgumentException(
            s"Unsupported review_status for ${valueOf(row, normalizedColumns("file_identifier"))}: $reviewStatus"
          )
      }
    }.toSeq
  }

  private def readScanResults(spark: SparkSession, scanResultsPath: String) = {
    val conf = spark.sparkContext.hadoopConfiguration
    resolveScanResultsFormat(conf, scanResultsPath) match {
      case XlsxFormat =>
        readSource(
          spark,
          XlsxFormat,
          Seq(scanResultsPath),
          readOptions = ScanReadOptions(sheetName = Some("scan_results"))
        )
      case format =>
        readSource(spark, format, Seq(scanResultsPath), csvHasHeader = true)
    }
  }

  private def resolveScanResultsFormat(
    conf: org.apache.hadoop.conf.Configuration,
    scanResultsPath: String
  ): String = {
    val path = new Path(scanResultsPath)
    val fs = path.getFileSystem(conf)

    if (fs.exists(path) && fs.getFileStatus(path).isDirectory) {
      Option(path.getParent)
        .map(_.getName.toLowerCase)
        .collect {
          case "csv" => "csv"
          case "parquet" => "parquet"
        }
        .getOrElse(throw new IllegalArgumentException(s"Unsupported scan_results directory format: $scanResultsPath"))
    } else {
      detectPhysicalFormat(conf, scanResultsPath)
        .getOrElse(throw new IllegalArgumentException(s"Unsupported scan_results format: $scanResultsPath"))
    }
  }

  private def valueOf(row: org.apache.spark.sql.Row, columnName: String): String = {
    if (row.isNullAt(row.fieldIndex(columnName))) "" else Option(row.get(row.fieldIndex(columnName))).map(_.toString).getOrElse("")
  }

  private def loadExistingEntries(
    conf: org.apache.hadoop.conf.Configuration,
    allowlistPath: String
  ): Seq[AllowlistEntry] = {
    val path = new Path(allowlistPath)
    val fs = path.getFileSystem(conf)
    val backupPath = new Path(s"${allowlistPath}.bak")
    if (fs.exists(path) || fs.exists(backupPath)) AllowlistMatcher.loadEntries(conf, allowlistPath) else Seq.empty
  }

  private def writeAllowlist(
    conf: org.apache.hadoop.conf.Configuration,
    allowlistPath: String,
    entries: Seq[AllowlistEntry]
  ): Unit = {
    val path = new Path(allowlistPath)
    val fs = path.getFileSystem(conf)
    val tempPath = new Path(s"${allowlistPath}.tmp-${UUID.randomUUID().toString}")
    val backupPath = new Path(s"${allowlistPath}.bak")
    val writer = new BufferedWriter(new OutputStreamWriter(fs.create(tempPath, true), StandardCharsets.UTF_8))

    try {
      entries.foreach { entry =>
        writer.write(AllowlistJson.exactEntryToJson(entry))
        writer.newLine()
      }
    } finally {
      writer.close()
    }

    if (!fs.exists(path) && fs.exists(backupPath) && !fs.rename(backupPath, path)) {
      fs.delete(tempPath, false)
      throw new IllegalStateException(s"Allowlist backup restore failed: $allowlistPath")
    }

    if (fs.exists(backupPath) && !fs.delete(backupPath, false)) {
      fs.delete(tempPath, false)
      throw new IllegalStateException(s"Stale allowlist backup cleanup failed: ${backupPath.toString}")
    }

    if (fs.exists(path) && !fs.rename(path, backupPath)) {
      fs.delete(tempPath, false)
      throw new IllegalStateException(s"Existing allowlist backup failed: $allowlistPath")
    }

    if (fs.rename(tempPath, path)) {
      if (fs.exists(backupPath) && !fs.delete(backupPath, false)) {
        DriverLogger.warn("allowlist_backup_cleanup_failed", "allowlist" -> allowlistPath, "backup" -> backupPath.toString)
      }
    } else {
      fs.delete(tempPath, false)
      if (fs.exists(backupPath) && !fs.rename(backupPath, path)) {
        throw new IllegalStateException(s"Allowlist replace failed and backup restore failed: $allowlistPath")
      }
      throw new IllegalStateException(s"Allowlist rename failed: $allowlistPath")
    }
  }

  private def concreteScopeIdentifiers(
    conf: org.apache.hadoop.conf.Configuration,
    config: ReviewApplyCliConfig,
    decision: ReviewDecision
  ): Seq[String] = {
    if (decision.reviewScopeFileIdentifiers.nonEmpty) {
      decision.reviewScopeFileIdentifiers
    } else if (isDirectoryIdentifier(conf, config.inputRoot, decision.fileIdentifier)) {
      throw new IllegalArgumentException(
        s"Directory review rows require review_scope_file_identifiers: ${decision.fileIdentifier}"
      )
    } else {
      Seq(decision.fileIdentifier)
    }
  }

  private def validateScanMetadata(
    decision: ReviewDecision,
    fingerprints: Seq[ResolvedFileFingerprint]
  ): Unit = {
    require(
      decision.reviewScopeFileFingerprints.nonEmpty,
      s"False positive review rows require review_scope_file_fingerprints: ${decision.fileIdentifier}"
    )
    if (decision.reviewScopeFileIdentifiers.nonEmpty) {
      require(
        decision.reviewScopeFileFingerprints.nonEmpty,
        s"Directory review rows require review_scope_file_fingerprints: ${decision.fileIdentifier}"
      )
    }

    if (decision.reviewScopeFileFingerprints.nonEmpty) {
      validateScopeFingerprints(decision, fingerprints)
    } else {
      val currentFileSize = fingerprints.map(_.fileSize).sum
      val currentFileMtimeEpochMs = fingerprints.map(_.fileMtimeEpochMs).foldLeft(0L)(math.max)

      require(
        decision.scanFileSize == currentFileSize && decision.scanFileMtimeEpochMs == currentFileMtimeEpochMs,
        s"Scan result metadata is stale for ${decision.fileIdentifier}; rerun scan before review apply"
      )
    }
  }

  private def isDirectoryIdentifier(
    conf: org.apache.hadoop.conf.Configuration,
    inputRoot: String,
    fileIdentifier: String
  ): Boolean = {
    val resolvedPath = if (fileIdentifier == "." || fileIdentifier.isEmpty) {
      inputRoot
    } else {
      new Path(new Path(inputRoot), fileIdentifier).toString
    }
    val path = new Path(resolvedPath)
    val fs = path.getFileSystem(conf)
    fs.exists(path) && fs.getFileStatus(path).isDirectory
  }

  private def parseScopeIdentifiers(rawValue: String): Seq[String] = {
    ReviewScopeIdentifierCodec.decode(rawValue) match {
      case Right(identifiers) =>
        identifiers
      case Left(errorMessage) =>
        throw new IllegalArgumentException(errorMessage)
    }
  }

  private def parseScopeFingerprints(rawValue: String): Seq[RecordedFileFingerprint] = {
    ReviewScopeFingerprintCodec.decode(rawValue) match {
      case Right(fingerprints) =>
        fingerprints
      case Left(errorMessage) =>
        throw new IllegalArgumentException(errorMessage)
    }
  }

  private def validateScopeFingerprints(
    decision: ReviewDecision,
    fingerprints: Seq[ResolvedFileFingerprint]
  ): Unit = {
    val expectedFingerprints = decision.reviewScopeFileFingerprints.sortBy(_.fileIdentifier)
    val currentFingerprints = fingerprints.map(RecordedFileFingerprint.fromResolved).sortBy(_.fileIdentifier)

    require(
      expectedFingerprints == currentFingerprints,
      s"Scan result metadata is stale for ${decision.fileIdentifier}; rerun scan before review apply"
    )
  }
}
