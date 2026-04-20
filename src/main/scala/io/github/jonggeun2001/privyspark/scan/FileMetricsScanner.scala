package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.config.SuppressionSet
import io.github.jonggeun2001.privyspark.detect.DetectionAggregator
import io.github.jonggeun2001.privyspark.format.ByteProbe.detectPhysicalFormat
import io.github.jonggeun2001.privyspark.format.CsvInference.{detectCsvHasHeader, readSource}
import io.github.jonggeun2001.privyspark.fsio.RetryIO.withFileReadRetry
import io.github.jonggeun2001.privyspark.model.{FileScanMetrics, PiiRule, ScanError, ScanGroup, ScanReadOptions, ScanResult}
import io.github.jonggeun2001.privyspark.util.DriverLogger
import io.github.jonggeun2001.privyspark.util.PathIdentifiers.resolveRelativeIdentifier
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

import scala.util.control.NonFatal

private[privyspark] object FileMetricsScanner {
  def scanFileMetrics(
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
    recordedFingerprint: Option[io.github.jonggeun2001.privyspark.review.RecordedFileFingerprint] = None,
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
        val fileStatus = new Path(physicalPath).getFileSystem(spark.sparkContext.hadoopConfiguration)
          .getFileStatus(new Path(physicalPath))
        val effectiveFileSize = fileSizeOverride.getOrElse(fileStatus.getLen)
        val effectiveFileMtimeEpochMs = fileMtimeEpochMsOverride.getOrElse(fileStatus.getModificationTime)
        val effectiveRecordedFingerprint = recordedFingerprint.orElse {
          if (captureRecordedFingerprintWhenMissing) {
            Some(ReviewSnapshotLog.captureRecordedFingerprint(
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
        val effectiveRules = ScanResultBuilder.effectiveRulesForFormat(format, rules)

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
        val sampledDf = ScanResultBuilder.sampleRowsDeterministically(sourceDf, sampleRatio)

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
            ScanResultBuilder.currentScanTimestamp(),
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
            ScanResultBuilder.currentScanTimestamp(),
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

  def scanFile(
    spark: SparkSession,
    datasetPath: String,
    filePath: String,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    suppressions: SuppressionSet = SuppressionSet.empty
  ): Either[ScanError, Seq[ScanResult]] = {
    scanFileMetrics(spark, datasetPath, filePath, rules, sampleRatio, timestamp, suppressions = suppressions).map { fileMetrics =>
      ScanResultBuilder.buildScanResults(
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

  def resolveSelectedFileKeys(
    group: ScanGroup,
    sampleRatio: Double,
    fileSampleRatio: Option[Double],
    fileSampleMinFiles: Int
  ): Seq[String] = {
    fileSampleRatio match {
      case Some(ratio) if group.filePaths.size > fileSampleMinFiles =>
        val sampledKeys = GroupScanner.selectSampledFileKeys(group.filePaths, ratio)
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
}
