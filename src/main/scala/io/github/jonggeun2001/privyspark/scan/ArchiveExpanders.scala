package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.config.IgnoreMatcher
import io.github.jonggeun2001.privyspark.format.FormatDetector
import io.github.jonggeun2001.privyspark.model.{ScanError, ScanFileEntry}
import io.github.jonggeun2001.privyspark.scan.ArchiveStaging._
import io.github.jonggeun2001.privyspark.scan.archive.{ArchiveExpandLoop, ArchiveIOUtil, RarHandler, SevenZHandler, TarHandler, ZipHandler}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

private[privyspark] object ArchiveExpanders {
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
    val context = ArchiveExpandLoop.ArchiveExpansionContext(
      conf,
      datasetPath,
      timestamp,
      archivePath,
      logicalIdentifier,
      stagingPaths,
      ignoreMatcher,
      archiveExpansionDepth
    )

    try {
      FormatDetector.detect(archivePath).flatMap(_.archiveFormat) match {
        case Some(format) if format == ZipFormat || format == JarFormat =>
          ArchiveExpandLoop.run(context)(archiveErrors => ZipHandler.open(conf, archivePath, archiveErrors, datasetPath, timestamp, logicalIdentifier))
        case Some(TarFormat) =>
          ArchiveExpandLoop.run(context)(archiveErrors => Right(TarHandler.open(conf, archivePath, archiveErrors, datasetPath, timestamp, logicalIdentifier)))
        case Some(SevenZFormat) =>
          ArchiveIOUtil.withLocalArchiveFile(conf, archivePath) { localArchivePath =>
            ArchiveExpandLoop.run(context)(archiveErrors => SevenZHandler.open(localArchivePath, archiveErrors, datasetPath, timestamp, logicalIdentifier))
          }
        case Some(RarFormat) =>
          ArchiveIOUtil.withLocalArchiveFile(conf, archivePath) { localArchivePath =>
            ArchiveExpandLoop.run(context)(archiveErrors => RarHandler.open(localArchivePath, archiveErrors, datasetPath, timestamp, logicalIdentifier))
          }
        case Some(other) =>
          archiveReadFailed(datasetPath, timestamp, logicalIdentifier, s"Unsupported archive format: $other")
        case None =>
          archiveReadFailed(datasetPath, timestamp, logicalIdentifier, s"Unsupported archive format: $archivePath")
      }
    } catch {
      case NonFatal(e) =>
        archiveReadFailed(datasetPath, timestamp, logicalIdentifier, Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
    }
  }

  private def archiveReadFailed(
    datasetPath: String,
    timestamp: String,
    logicalIdentifier: String,
    reason: String
  ): (Seq[ScanFileEntry], Seq[ScanError], Int) = {
    (Seq.empty, Seq(ScanError(datasetPath, timestamp, logicalIdentifier, s"Archive read failed: $reason")), 0)
  }
}
