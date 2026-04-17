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

  def supportsBatchScan(group: ScanGroup): Boolean = {
    group.format != XlsxFormat
  }

  def expandPhysicalSource(
    conf: org.apache.hadoop.conf.Configuration,
    datasetPath: String,
    timestamp: String,
    physicalPath: String,
    logicalIdentifier: String,
    groupingDirectoryPath: String,
    stagingPaths: ArrayBuffer[String],
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
        detectPhysicalFormat(conf, physicalPath)
      } catch {
        case NonFatal(e) =>
          return (
            Seq.empty,
            Seq(ScanError(datasetPath, timestamp, logicalIdentifier, Option(e.getMessage).getOrElse(e.getClass.getSimpleName))),
            0
          )
      }
    detectedFormat match {
      case Some(format) if ArchiveFormats.contains(format) && archiveExpansionDepth < MaxArchiveExpansionDepth =>
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
      case Some(format) if ArchiveFormats.contains(format) =>
        (
          Seq.empty,
          Seq(ScanError(datasetPath, timestamp, logicalIdentifier, s"Nested archive expansion is not supported: $logicalIdentifier")),
          0
        )
      case Some(XlsxFormat) =>
        val (entries, errors) = expandWorkbookSource(conf, datasetPath, timestamp, physicalPath, logicalIdentifier)
        (entries, errors, 0)
      case Some(format) =>
        (
          Seq(
            ScanFileEntry(
              sourceKey = physicalPath,
              physicalPath = physicalPath,
              directoryPath = groupingDirectoryPath,
              format = format,
              logicalIdentifier = logicalIdentifier,
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
    logicalIdentifier: String
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
              readOptions = ScanReadOptions(sheetName = Some(sheetName)),
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
