package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.cli.ReviewApplyCliConfig
import io.github.jonggeun2001.privyspark.format.ByteProbe.detectPhysicalFormat
import io.github.jonggeun2001.privyspark.format.CsvInference.{XlsxFormat, readSource}
import io.github.jonggeun2001.privyspark.model.ScanReadOptions
import io.github.jonggeun2001.privyspark.report.JsonCodec.jsonString
import io.github.jonggeun2001.privyspark.util.DriverLogger
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID
import scala.collection.mutable.ArrayBuffer

object ReviewApplyCommand {
  private final case class ReviewCandidate(
    datasetPath: String,
    fileIdentifier: String,
    columnName: String,
    piiType: String,
    reviewReason: String,
    sourceRunId: String
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
    val reviewCandidates = loadReviewCandidates(spark, config)
    val reviewedAt = Instant.now().toString
    val stagedEntries = reviewCandidates.flatMap { candidate =>
      FileIdentifierResolver.resolveFingerprints(conf, config.inputRoot, candidate.fileIdentifier) match {
        case Right(fingerprints) =>
          fingerprints.map { fingerprint =>
            AllowlistEntry(
              datasetPath = candidate.datasetPath,
              fileIdentifier = fingerprint.fileIdentifier,
              columnName = candidate.columnName,
              piiType = candidate.piiType,
              reason = candidate.reviewReason,
              reviewer = config.reviewer,
              reviewedAt = reviewedAt,
              sourceRunId = candidate.sourceRunId,
              fileSize = fingerprint.fileSize,
              fileMtimeEpochMs = fingerprint.fileMtimeEpochMs,
              fileChecksumAlgo = fingerprint.fileChecksumAlgo,
              fileChecksum = fingerprint.fileChecksum
            )
          }
        case Left(errorMessage) =>
          throw new IllegalArgumentException(s"Failed to resolve ${candidate.fileIdentifier}: $errorMessage")
      }
    }

    val existingEntries = loadExistingEntries(conf, config.allowlistPath)
    val mergedEntries = (existingEntries ++ stagedEntries)
      .groupBy(_.key)
      .map {
        case (_, groupedEntries) => groupedEntries.last
      }
      .toSeq
      .sortBy(entry => (entry.datasetPath, entry.fileIdentifier, entry.columnName, entry.piiType))

    DriverLogger.info(
      "review_apply_ready",
      "review_rows" -> reviewCandidates.size,
      "staged_entries" -> stagedEntries.size,
      "final_entries" -> mergedEntries.size,
      "dry_run" -> config.dryRun
    )

    if (!config.dryRun) {
      writeAllowlist(conf, config.allowlistPath, mergedEntries)
    }
  }

  private def loadReviewCandidates(
    spark: SparkSession,
    config: ReviewApplyCliConfig
  ): Seq[ReviewCandidate] = {
    val df = readScanResults(spark, config.scanResultsPath)
    val normalizedColumns = df.columns.map(columnName => columnName.toLowerCase -> columnName).toMap
    val requiredColumns = Seq("dataset_path", "file_identifier", "column_name", "pii_type", "review_status", "review_reason")
    requiredColumns.foreach { columnName =>
      require(normalizedColumns.contains(columnName), s"scan_results is missing required column: $columnName")
    }

    df.collect().flatMap { row =>
      val reviewStatus = valueOf(row, normalizedColumns("review_status"))
      ReviewStatus.normalize(reviewStatus) match {
        case Some(ReviewStatus.FalsePositive) =>
          val reviewReason = valueOf(row, normalizedColumns("review_reason")).trim
          require(reviewReason.nonEmpty, s"review_reason is required for false_positive: ${valueOf(row, normalizedColumns("file_identifier"))}")
          Some(ReviewCandidate(
            datasetPath = valueOf(row, normalizedColumns("dataset_path")),
            fileIdentifier = valueOf(row, normalizedColumns("file_identifier")),
            columnName = valueOf(row, normalizedColumns("column_name")),
            piiType = valueOf(row, normalizedColumns("pii_type")),
            reviewReason = reviewReason,
            sourceRunId = normalizedColumns.get("source_run_id").map(columnName => valueOf(row, columnName)).getOrElse("")
          ))
        case _ =>
          None
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
    if (fs.exists(path)) AllowlistMatcher.loadEntries(conf, allowlistPath) else Seq.empty
  }

  private def writeAllowlist(
    conf: org.apache.hadoop.conf.Configuration,
    allowlistPath: String,
    entries: Seq[AllowlistEntry]
  ): Unit = {
    val path = new Path(allowlistPath)
    val fs = path.getFileSystem(conf)
    val tempPath = new Path(s"${allowlistPath}.tmp-${UUID.randomUUID().toString}")
    val writer = new BufferedWriter(new OutputStreamWriter(fs.create(tempPath, true), StandardCharsets.UTF_8))

    try {
      entries.foreach { entry =>
        writer.write(allowlistEntryToJson(entry))
        writer.newLine()
      }
    } finally {
      writer.close()
    }

    if (fs.exists(path) && !fs.delete(path, false)) {
      fs.delete(tempPath, false)
      throw new IllegalStateException(s"Existing allowlist replace failed: $allowlistPath")
    }
    if (!fs.rename(tempPath, path)) {
      fs.delete(tempPath, false)
      throw new IllegalStateException(s"Allowlist rename failed: $allowlistPath")
    }
  }

  private def allowlistEntryToJson(entry: AllowlistEntry): String =
    s"""{"dataset_path":${jsonString(entry.datasetPath)},"file_identifier":${jsonString(entry.fileIdentifier)},"column_name":${jsonString(entry.columnName)},"pii_type":${jsonString(entry.piiType)},"reason":${jsonString(entry.reason)},"reviewer":${jsonString(entry.reviewer)},"reviewed_at":${jsonString(entry.reviewedAt)},"source_run_id":${jsonString(entry.sourceRunId)},"file_size":${entry.fileSize},"file_mtime_epoch_ms":${entry.fileMtimeEpochMs},"file_checksum_algo":${jsonString(entry.fileChecksumAlgo)},"file_checksum":${jsonString(entry.fileChecksum)}}"""
}
