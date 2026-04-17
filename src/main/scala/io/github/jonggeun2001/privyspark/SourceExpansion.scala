package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.ArchiveStaging._
import io.github.jonggeun2001.privyspark.ByteProbe._
import io.github.jonggeun2001.privyspark.CsvInference.XlsxFormat
import io.github.jonggeun2001.privyspark.WorkbookHelpers.listVisibleWorkbookSheets
import io.github.jonggeun2001.privyspark.config.IgnoreMatcher
import io.github.jonggeun2001.privyspark.model.{PiiRule, ScanError, ScanFileEntry, ScanGroup, ScanReadOptions}
import org.apache.hadoop.fs.Path

import java.io.ByteArrayOutputStream
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.ZipInputStream
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
    val sourcePath = new Path(archivePath)
    val fs = sourcePath.getFileSystem(conf)
    val extractedEntries = ArrayBuffer.empty[ScanFileEntry]
    val archiveErrors = ArrayBuffer.empty[ScanError]
    val ignoredArchiveEntries = new AtomicInteger(0)
    val stagingBase = new Path(fs.getHomeDirectory, ".privyspark-staging")
    val stagingRoot = new Path(
      stagingBase,
      s"archive-${System.currentTimeMillis()}-${math.abs(scala.util.Random.nextLong())}"
    )
    val archiveInputStream = fs.open(sourcePath)
    val zipInputStream = new ZipInputStream(archiveInputStream)
    val stagedTargetPaths = scala.collection.mutable.Set.empty[String]
    var stagingPrepared = false

    def ensureArchiveStagingReady(): Either[String, Unit] = {
      if (stagingPrepared) {
        Right(())
      } else if (!fs.exists(stagingBase) && !fs.mkdirs(stagingBase)) {
        Left(s"Archive staging base creation failed: ${stagingBase.toString}")
      } else if (!fs.mkdirs(stagingRoot) && !fs.exists(stagingRoot)) {
        Left(s"Archive staging directory creation failed: ${stagingRoot.toString}")
      } else {
        stagingPaths += stagingRoot.toString
        stagingPrepared = true
        Right(())
      }
    }

    def reserveStagedTargetPath(normalizedEntryName: String, targetPath: Path): Either[String, Unit] = {
      val targetComparablePath = PathIdentifiers.canonicalizePath(targetPath.toString)
      if (stagedTargetPaths.add(targetComparablePath)) Right(()) else Left(s"Conflicting archive entry path: $normalizedEntryName")
    }

    try {
      var entry = zipInputStream.getNextEntry
      while (entry != null) {
        if (!entry.isDirectory) {
          val normalizedEntryName = normalizeArchiveEntryName(entry.getName)
          val childLogicalIdentifier = s"$logicalIdentifier!$normalizedEntryName"
          if (entry.getSize == 0L) {
            DriverLogger.debug(
              "archive_entry_skipped",
              "archive" -> logicalIdentifier,
              "entry" -> normalizedEntryName,
              "reason" -> "zero_byte"
            )
          } else {
            safeResolveArchiveEntryPath(stagingRoot, normalizedEntryName) match {
              case Some(targetPath) =>
                ignoreMatcher.matched(childLogicalIdentifier, datasetPath) match {
                  case Some(pattern) =>
                    ignoredArchiveEntries.incrementAndGet()
                    DriverLogger.debug(
                      "archive_entry_skipped",
                      "archive" -> logicalIdentifier,
                      "entry" -> childLogicalIdentifier,
                      "reason" -> "ignored",
                      "pattern" -> pattern
                    )
                  case None =>
                    try {
                      FormatDetector.infer(normalizedEntryName) match {
                        case Some(format) if ArchiveFormats.contains(format) && archiveExpansionDepth >= MaxArchiveExpansionDepth =>
                          if (zipInputStream.read() >= 0) {
                            archiveErrors += ScanError(
                              datasetPath,
                              timestamp,
                              childLogicalIdentifier,
                              s"Nested archive expansion is not supported: $childLogicalIdentifier"
                            )
                          } else {
                            DriverLogger.debug(
                              "archive_entry_skipped",
                              "archive" -> logicalIdentifier,
                              "entry" -> childLogicalIdentifier,
                              "reason" -> "zero_byte"
                            )
                          }
                        case Some(_) =>
                          val buffer = new Array[Byte](8192)
                          var bytesRead = zipInputStream.read(buffer)
                          if (bytesRead < 0) {
                            DriverLogger.debug(
                              "archive_entry_skipped",
                              "archive" -> logicalIdentifier,
                              "entry" -> childLogicalIdentifier,
                              "reason" -> "zero_byte"
                            )
                          } else {
                            ensureArchiveStagingReady() match {
                              case Left(errorMessage) =>
                                archiveErrors += ScanError(datasetPath, timestamp, childLogicalIdentifier, errorMessage)
                              case Right(_) =>
                                reserveStagedTargetPath(normalizedEntryName, targetPath) match {
                                  case Left(errorMessage) =>
                                    archiveErrors += ScanError(datasetPath, timestamp, childLogicalIdentifier, errorMessage)
                                  case Right(_) =>
                                    ensureArchiveEntryParent(fs, targetPath) match {
                                      case Left(errorMessage) =>
                                        archiveErrors += ScanError(datasetPath, timestamp, childLogicalIdentifier, errorMessage)
                                      case Right(_) =>
                                        val outputStream = fs.create(targetPath, true)
                                        try {
                                          while (bytesRead >= 0) {
                                            if (bytesRead > 0) {
                                              outputStream.write(buffer, 0, bytesRead)
                                            }
                                            bytesRead = zipInputStream.read(buffer)
                                          }
                                        } finally {
                                          outputStream.close()
                                        }

                                        val (childEntries, childErrors, childIgnoredEntries) = expandPhysicalSource(
                                          conf,
                                          datasetPath,
                                          timestamp,
                                          targetPath.toString,
                                          childLogicalIdentifier,
                                          logicalIdentifier,
                                          stagingPaths,
                                          ignoreMatcher = ignoreMatcher,
                                          archiveExpansionDepth = archiveExpansionDepth,
                                          forceDisableDirectoryIdentifier = true
                                        )
                                        extractedEntries ++= childEntries
                                        archiveErrors ++= childErrors
                                        ignoredArchiveEntries.addAndGet(childIgnoredEntries)
                                    }
                                }
                            }
                          }
                        case None if FormatDetector.shouldSkipProbe(normalizedEntryName) =>
                          val declaredEmpty = entry.getSize == 0L || entry.getCompressedSize == 0L
                          val firstByte = if (declaredEmpty) -1 else zipInputStream.read()
                          if (firstByte >= 0) {
                            archiveErrors += ScanError(
                              datasetPath,
                              timestamp,
                              childLogicalIdentifier,
                              s"Unsupported file format: $childLogicalIdentifier"
                            )
                          } else {
                            DriverLogger.debug(
                              "archive_entry_skipped",
                              "archive" -> logicalIdentifier,
                              "entry" -> childLogicalIdentifier,
                              "reason" -> "zero_byte"
                            )
                          }
                        case None =>
                          val probeBuffer = new ByteArrayOutputStream()
                          val buffer = new Array[Byte](8192)
                          var outputStream: org.apache.hadoop.fs.FSDataOutputStream = null
                          var detectedFormat: Option[String] = None
                          var archiveEntryError: Option[String] = None
                          var probeRejected = false
                          var archiveEntryHasContent = false
                          var archiveEntrySkipped = false
                          var targetPathReserved = false
                          var bytesRead = zipInputStream.read(buffer)

                          def materializeDetectedEntry(format: String, bytesForProbe: Int, currentChunkSize: Int): Unit = {
                            ensureArchiveStagingReady() match {
                              case Left(errorMessage) =>
                                archiveEntryError = Some(errorMessage)
                              case Right(_) =>
                                if (!targetPathReserved) {
                                  reserveStagedTargetPath(normalizedEntryName, targetPath) match {
                                    case Left(errorMessage) =>
                                      archiveEntryError = Some(errorMessage)
                                    case Right(_) =>
                                      targetPathReserved = true
                                  }
                                }
                                if (archiveEntryError.isEmpty) {
                                  ensureArchiveEntryParent(fs, targetPath) match {
                                    case Left(errorMessage) =>
                                      archiveEntryError = Some(errorMessage)
                                    case Right(_) =>
                                      outputStream = fs.create(targetPath, true)
                                      outputStream.write(probeBuffer.toByteArray)
                                      if (currentChunkSize > bytesForProbe) {
                                        outputStream.write(buffer, bytesForProbe, currentChunkSize - bytesForProbe)
                                      }
                                      detectedFormat = Some(format)
                                  }
                                }
                            }
                          }

                          try {
                            while (bytesRead >= 0 && archiveEntryError.isEmpty) {
                              if (bytesRead > 0) {
                                archiveEntryHasContent = true
                                if (detectedFormat.isDefined) {
                                  outputStream.write(buffer, 0, bytesRead)
                                } else if (!probeRejected) {
                                  val remainingProbeSpace = TextProbeByteLimit - probeBuffer.size()
                                  val bytesForProbe = math.min(bytesRead, math.max(0, remainingProbeSpace))
                                  if (bytesForProbe > 0) {
                                    probeBuffer.write(buffer, 0, bytesForProbe)
                                  }

                                  val probeBytes = probeBuffer.toByteArray
                                  val probeComplete = probeBuffer.size() >= TextProbeByteLimit
                                  val probeTruncated = probeComplete && bytesRead > bytesForProbe
                                  val format = inferMagicByteFormat(probeBytes).orElse {
                                    if (probeTruncated) inferTextFormat(probeBytes, allowIncompleteTrailingSequence = true) else None
                                  }

                                  format match {
                                    case Some(value) =>
                                      materializeDetectedEntry(value, bytesForProbe, bytesRead)
                                    case None if probeTruncated =>
                                      probeRejected = true
                                    case None =>
                                      ()
                                  }
                                }
                              }
                              bytesRead = zipInputStream.read(buffer)
                            }

                            if (archiveEntryError.isEmpty && detectedFormat.isEmpty && !probeRejected) {
                              if (archiveEntryHasContent || probeBuffer.size() > 0) {
                                inferMagicByteFormat(probeBuffer.toByteArray)
                                  .orElse(inferTextFormat(probeBuffer.toByteArray, allowIncompleteTrailingSequence = false)) match {
                                  case Some(format) =>
                                    materializeDetectedEntry(format, bytesForProbe = probeBuffer.size(), currentChunkSize = probeBuffer.size())
                                  case None =>
                                    probeRejected = true
                                }
                              } else {
                                archiveEntrySkipped = true
                              }
                            }
                            archiveEntryError match {
                              case Some(errorMessage) =>
                                archiveErrors += ScanError(datasetPath, timestamp, childLogicalIdentifier, errorMessage)
                              case None =>
                                if (archiveEntrySkipped) {
                                  ()
                                } else detectedFormat match {
                                  case Some(format) =>
                                    extractedEntries += ScanFileEntry(
                                      sourceKey = targetPath.toString,
                                      physicalPath = targetPath.toString,
                                      directoryPath = logicalIdentifier,
                                      format = format,
                                      logicalIdentifier = childLogicalIdentifier,
                                      allowDirectoryIdentifier = false
                                    )
                                  case None =>
                                    archiveErrors += ScanError(
                                      datasetPath,
                                      timestamp,
                                      childLogicalIdentifier,
                                      s"Unsupported file format: $childLogicalIdentifier"
                                    )
                                }
                            }
                          } finally {
                            if (outputStream != null) {
                              outputStream.close()
                            }
                          }
                      }
                    } catch {
                      case NonFatal(e) =>
                        archiveErrors += ScanError(
                          datasetPath,
                          timestamp,
                          childLogicalIdentifier,
                          s"Archive entry materialization failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}"
                        )
                    }
                }
              case None =>
                if (zipInputStream.read() >= 0) {
                  archiveErrors += ScanError(
                    datasetPath,
                    timestamp,
                    childLogicalIdentifier,
                    s"Unsafe archive entry path: $normalizedEntryName"
                  )
                } else {
                  DriverLogger.debug(
                    "archive_entry_skipped",
                    "archive" -> logicalIdentifier,
                    "entry" -> childLogicalIdentifier,
                    "reason" -> "zero_byte"
                  )
                }
            }
          }
        }
        zipInputStream.closeEntry()
        entry = zipInputStream.getNextEntry
      }
    } catch {
      case NonFatal(e) =>
        archiveErrors += ScanError(
          datasetPath,
          timestamp,
          logicalIdentifier,
          s"Archive read failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}"
        )
    } finally {
      zipInputStream.close()
      archiveInputStream.close()
    }

    (extractedEntries.toSeq, archiveErrors.toSeq, ignoredArchiveEntries.get())
  }
}
