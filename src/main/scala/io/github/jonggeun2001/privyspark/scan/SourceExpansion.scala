package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.scan.ArchiveStaging._
import io.github.jonggeun2001.privyspark.format.ByteProbe._
import io.github.jonggeun2001.privyspark.format.FormatDetector
import io.github.jonggeun2001.privyspark.format.CsvInference.XlsxFormat
import io.github.jonggeun2001.privyspark.format.WorkbookHelpers.listVisibleWorkbookSheets
import io.github.jonggeun2001.privyspark.config.IgnoreMatcher
import io.github.jonggeun2001.privyspark.model.{PiiRule, ScanError, ScanFileEntry, ScanGroup, ScanReadOptions}
import io.github.jonggeun2001.privyspark.util.{DriverLogger, PathIdentifiers}
import org.apache.hadoop.fs.Path

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

private[privyspark] object SourceExpansion {
  private val NonDirectoryIdentifierFormats = Set(TextFormat, XlsxFormat)

  def workbookSheetReadOptions(readOptions: ScanReadOptions, sheetName: String): ScanReadOptions = {
    readOptions.copy(sheetName = Some(sheetName))
  }

  def supportsBatchScan(group: ScanGroup): Boolean = {
    group.format != XlsxFormat && group.readOptionsByKey.isEmpty
  }

  def expandPhysicalSource(
    conf: org.apache.hadoop.conf.Configuration,
    datasetPath: String,
    timestamp: String,
    physicalPath: String,
    logicalIdentifier: String,
    groupingDirectoryPath: String,
    stagingPaths: ArrayBuffer[String],
    fileSize: Long = 0L,
    fileMtimeEpochMs: Long = 0L,
    readOptions: ScanReadOptions = ScanReadOptions(),
    ignoreMatcher: IgnoreMatcher = IgnoreMatcher.empty,
    archiveExpansionDepth: Int = 0,
    forceDisableDirectoryIdentifier: Boolean = false
  ): (Seq[ScanFileEntry], Seq[ScanError], Int) = {
    try {
      if (isZeroBytePhysicalFile(conf, physicalPath)) {
        return (Seq.empty, Seq.empty, 0)
      }
    } catch {
      case NonFatal(e) =>
        return (
          Seq.empty,
          Seq(ScanError(datasetPath, timestamp, logicalIdentifier, Option(e.getMessage).getOrElse(e.getClass.getSimpleName))),
          0
        )
    }

    val detectedFormat =
      try {
        detectPhysicalFormatWithReadOptions(conf, physicalPath)
      } catch {
        case NonFatal(e) =>
          return (
            Seq.empty,
            Seq(ScanError(datasetPath, timestamp, logicalIdentifier, Option(e.getMessage).getOrElse(e.getClass.getSimpleName))),
            0
          )
      }
    detectedFormat match {
      case Some((format, _)) if ArchiveFormats.contains(format) && archiveExpansionDepth < MaxArchiveExpansionDepth =>
        expandArchiveSource(
          conf,
          datasetPath,
          timestamp,
          physicalPath,
          logicalIdentifier,
          stagingPaths,
          ignoreMatcher,
          archiveExpansionDepth + 1
        )
      case Some((format, _)) if ArchiveFormats.contains(format) =>
        (
          Seq.empty,
          Seq(ScanError(datasetPath, timestamp, logicalIdentifier, s"Nested archive expansion is not supported: $logicalIdentifier")),
          0
        )
      case Some((XlsxFormat, _)) =>
        val (entries, errors) =
          expandWorkbookSource(conf, datasetPath, timestamp, physicalPath, logicalIdentifier, fileSize, fileMtimeEpochMs, readOptions)
        (entries, errors, 0)
      case Some((format, readOptions)) =>
        (
          Seq(
            ScanFileEntry(
              sourceKey = physicalPath,
              physicalPath = physicalPath,
              directoryPath = groupingDirectoryPath,
              format = format,
              logicalIdentifier = logicalIdentifier,
              fileSize = fileSize,
              fileMtimeEpochMs = fileMtimeEpochMs,
              readOptions = readOptions,
              allowDirectoryIdentifier = !forceDisableDirectoryIdentifier && !NonDirectoryIdentifierFormats.contains(format)
            )
          ),
          Seq.empty,
          0
        )
      case None =>
        (
          Seq.empty,
          Seq(ScanError(datasetPath, timestamp, logicalIdentifier, s"Unsupported file format: $logicalIdentifier")),
          0
        )
    }
  }

  def expandWorkbookSource(
    conf: org.apache.hadoop.conf.Configuration,
    datasetPath: String,
    timestamp: String,
    physicalPath: String,
    logicalIdentifier: String,
    fileSize: Long = 0L,
    fileMtimeEpochMs: Long = 0L,
    readOptions: ScanReadOptions = ScanReadOptions()
  ): (Seq[ScanFileEntry], Seq[ScanError]) = {
    listVisibleWorkbookSheets(conf, physicalPath) match {
      case Right(sheetNames) =>
        (
          sheetNames.map { sheetName =>
            ScanFileEntry(
              sourceKey = s"$physicalPath#$sheetName",
              physicalPath = physicalPath,
              directoryPath = logicalIdentifier,
              format = XlsxFormat,
              logicalIdentifier = s"$logicalIdentifier#$sheetName",
              fileSize = fileSize,
              fileMtimeEpochMs = fileMtimeEpochMs,
              readOptions = workbookSheetReadOptions(readOptions, sheetName),
              allowDirectoryIdentifier = false
            )
          },
          Seq.empty
        )
      case Left(errorMessage) =>
        (
          Seq.empty,
          Seq(ScanError(datasetPath, timestamp, logicalIdentifier, s"Workbook read failed: $errorMessage"))
        )
    }
  }

  def expandArchiveSource(
    conf: org.apache.hadoop.conf.Configuration,
    datasetPath: String,
    timestamp: String,
    archivePath: String,
    logicalIdentifier: String,
    stagingPaths: ArrayBuffer[String],
    ignoreMatcher: IgnoreMatcher,
    archiveExpansionDepth: Int
  ): (Seq[ScanFileEntry], Seq[ScanError], Int) = {
    ArchiveExpanders.expandArchiveSource(
      conf,
      datasetPath,
      timestamp,
      archivePath,
      logicalIdentifier,
      stagingPaths,
      ignoreMatcher,
      archiveExpansionDepth
    )
  }
}
