package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.config.SuppressionSet
import io.github.jonggeun2001.privyspark.model.{FileScanMetrics, PiiRule, ScanError, ScanGroup}
import io.github.jonggeun2001.privyspark.scan.ReviewSnapshotLog.StagedFileSnapshot
import io.github.jonggeun2001.privyspark.util.PathIdentifiers.{resolveLogicalIdentifier, resolvePhysicalPath, resolveReadOptions}
import org.apache.spark.sql.SparkSession

private[privyspark] object SourceKeyMetrics {
  def scanSourceKeyMetrics(
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

    FileMetricsScanner.scanFileMetrics(
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

  def scanSourceKeyUsingSnapshot(
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

    ReviewSnapshotLog.stageFileSnapshot(conf, datasetPath, group, sourceKey) match {
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
          io.github.jonggeun2001.privyspark.fsio.ManagedPaths.deleteStagingPath(conf, stagedSnapshot.stagedRoot)
        }
      case Left(errorMessage) =>
        Left(ScanError(datasetPath, timestamp, logicalIdentifier, errorMessage))
    }
  }
}
