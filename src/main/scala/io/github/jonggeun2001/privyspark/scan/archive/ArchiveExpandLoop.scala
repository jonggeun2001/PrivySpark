package io.github.jonggeun2001.privyspark.scan.archive

import io.github.jonggeun2001.privyspark.config.IgnoreMatcher
import io.github.jonggeun2001.privyspark.format.FormatDetector
import io.github.jonggeun2001.privyspark.model.{ScanError, ScanFileEntry}
import io.github.jonggeun2001.privyspark.scan.ArchiveStaging._
import io.github.jonggeun2001.privyspark.scan.SourceExpansion
import io.github.jonggeun2001.privyspark.util.DriverLogger
import io.github.jonggeun2001.privyspark.util.PathIdentifiers
import org.apache.hadoop.fs.Path

import java.io.InputStream
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

private[privyspark] object ArchiveExpandLoop {
  final case class ArchiveExpansionContext(
    conf: org.apache.hadoop.conf.Configuration,
    datasetPath: String,
    timestamp: String,
    archivePath: String,
    logicalIdentifier: String,
    stagingPaths: ArrayBuffer[String],
    ignoreMatcher: IgnoreMatcher,
    archiveExpansionDepth: Int
  )

  def run(
    context: ArchiveExpansionContext
  )(openHandler: ArrayBuffer[ScanError] => Either[String, ArchiveEntryHandler]): (Seq[ScanFileEntry], Seq[ScanError], Int) = {
    val sourcePath = new Path(context.archivePath)
    val fs = sourcePath.getFileSystem(context.conf)
    val archiveModifiedTime = fs.getFileStatus(sourcePath).getModificationTime
    val extractedEntries = ArrayBuffer.empty[ScanFileEntry]
    val archiveErrors = ArrayBuffer.empty[ScanError]
    val ignoredArchiveEntries = new AtomicInteger(0)
    val stagingBase = new Path(fs.getHomeDirectory, ".privyspark-staging")
    val stagingRoot = new Path(
      stagingBase,
      s"archive-${System.currentTimeMillis()}-${math.abs(scala.util.Random.nextLong())}"
    )
    val stagedTargetPaths = mutable.Set.empty[String]
    var stagingPrepared = false

    def addArchiveError(fileIdentifier: String, message: String): Unit = {
      archiveErrors += ScanError(context.datasetPath, context.timestamp, fileIdentifier, message)
    }

    def ensureArchiveStagingReady(): Either[String, Unit] = {
      if (stagingPrepared) {
        Right(())
      } else if (!fs.exists(stagingBase) && !fs.mkdirs(stagingBase)) {
        Left(s"Archive staging base creation failed: ${stagingBase.toString}")
      } else if (!fs.mkdirs(stagingRoot) && !fs.exists(stagingRoot)) {
        Left(s"Archive staging directory creation failed: ${stagingRoot.toString}")
      } else {
        context.stagingPaths += stagingRoot.toString
        stagingPrepared = true
        Right(())
      }
    }

    def reserveStagedTargetPath(normalizedEntryName: String, targetPath: Path): Either[String, Unit] = {
      val targetComparablePath = PathIdentifiers.canonicalizePath(targetPath.toString)
      if (stagedTargetPaths.add(targetComparablePath)) Right(()) else Left(s"Conflicting archive entry path: $normalizedEntryName")
    }

    def processEntry(
      entryName: String,
      isDirectory: Boolean,
      declaredSize: Long,
      entryInputStream: InputStream
    ): Unit = {
      if (isDirectory) {
        return
      }

      val normalizedEntryName = normalizeArchiveEntryName(entryName)
      val childLogicalIdentifier = s"${context.logicalIdentifier}!$normalizedEntryName"

      try {
        if (declaredSize == 0L) {
          DriverLogger.debug(
            "archive_entry_skipped",
            "archive" -> context.logicalIdentifier,
            "entry" -> childLogicalIdentifier,
            "reason" -> "zero_byte"
          )
          return
        }

        safeResolveArchiveEntryPath(stagingRoot, normalizedEntryName) match {
          case None =>
            val firstChunk = ArchiveIOUtil.readFirstChunk(entryInputStream)
            if (firstChunk.isEmpty) {
              DriverLogger.debug(
                "archive_entry_skipped",
                "archive" -> context.logicalIdentifier,
                "entry" -> childLogicalIdentifier,
                "reason" -> "zero_byte"
              )
            } else {
              ArchiveIOUtil.drainInputStream(entryInputStream)
              addArchiveError(childLogicalIdentifier, s"Unsafe archive entry path: $normalizedEntryName")
            }

          case Some(targetPath) =>
            context.ignoreMatcher.matched(childLogicalIdentifier, context.datasetPath) match {
              case Some(pattern) =>
                ignoredArchiveEntries.incrementAndGet()
                ArchiveIOUtil.drainInputStream(entryInputStream)
                DriverLogger.debug(
                  "archive_entry_skipped",
                  "archive" -> context.logicalIdentifier,
                  "entry" -> childLogicalIdentifier,
                  "reason" -> "ignored",
                  "pattern" -> pattern
                )

              case None =>
                val firstChunk = ArchiveIOUtil.readFirstChunk(entryInputStream)
                if (firstChunk.isEmpty) {
                  DriverLogger.debug(
                    "archive_entry_skipped",
                    "archive" -> context.logicalIdentifier,
                    "entry" -> childLogicalIdentifier,
                    "reason" -> "zero_byte"
                  )
                } else {
                  val pathFormat = FormatDetector.infer(normalizedEntryName)
                  val shouldRejectNestedArchive =
                    pathFormat.exists(ArchiveFormats.contains) && context.archiveExpansionDepth >= MaxArchiveExpansionDepth
                  val shouldRejectProbe =
                    pathFormat.isEmpty && FormatDetector.shouldSkipProbe(normalizedEntryName)
                  val initialBytes =
                    if (pathFormat.isDefined) {
                      Some(firstChunk)
                    } else if (shouldRejectProbe) {
                      None
                    } else {
                      val (probeBytes, detectedFormat) = ArchiveIOUtil.probeEntryContent(firstChunk, entryInputStream)
                      if (detectedFormat.isDefined) Some(probeBytes) else None
                    }

                  if (shouldRejectNestedArchive) {
                    ArchiveIOUtil.drainInputStream(entryInputStream)
                    addArchiveError(childLogicalIdentifier, s"Nested archive expansion is not supported: $childLogicalIdentifier")
                  } else if (shouldRejectProbe || initialBytes.isEmpty) {
                    ArchiveIOUtil.drainInputStream(entryInputStream)
                    addArchiveError(childLogicalIdentifier, s"Unsupported file format: $childLogicalIdentifier")
                  } else {
                    val bytesToMaterialize = initialBytes.get
                    val stagingResult = for {
                      _ <- ensureArchiveStagingReady()
                      _ <- reserveStagedTargetPath(normalizedEntryName, targetPath)
                      _ <- ensureArchiveEntryParent(fs, targetPath)
                    } yield ()

                    stagingResult match {
                      case Left(errorMessage) =>
                        ArchiveIOUtil.drainInputStream(entryInputStream)
                        addArchiveError(childLogicalIdentifier, errorMessage)

                      case Right(_) =>
                        var materializedSuccessfully = false
                        var cleanupPartialTarget = false
                        var outputStream: org.apache.hadoop.fs.FSDataOutputStream = null

                        try {
                          outputStream = fs.create(targetPath, true)
                          outputStream.write(bytesToMaterialize)
                          ArchiveIOUtil.copyRemaining(entryInputStream, outputStream)
                          materializedSuccessfully = true
                        } catch {
                          case NonFatal(e) =>
                            cleanupPartialTarget = true
                            addArchiveError(
                              childLogicalIdentifier,
                              s"Archive entry materialization failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}"
                            )
                        } finally {
                          if (outputStream != null) {
                            try {
                              outputStream.close()
                            } catch {
                              case NonFatal(e) =>
                                materializedSuccessfully = false
                                cleanupPartialTarget = true
                                addArchiveError(
                                  childLogicalIdentifier,
                                  s"Archive entry materialization failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}"
                                )
                            }
                          }

                          if (cleanupPartialTarget) {
                            try {
                              if (fs.exists(targetPath) && !fs.delete(targetPath, false)) {
                                addArchiveError(childLogicalIdentifier, s"Archive entry cleanup failed: ${targetPath.toString}")
                              }
                            } catch {
                              case NonFatal(e) =>
                                addArchiveError(
                                  childLogicalIdentifier,
                                  s"Archive entry cleanup failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}"
                                )
                            }
                          }
                        }

                        if (materializedSuccessfully) {
                          val (childEntries, childErrors, childIgnoredEntries) =
                            SourceExpansion.expandPhysicalSource(
                              context.conf,
                              context.datasetPath,
                              context.timestamp,
                              targetPath.toString,
                              childLogicalIdentifier,
                              context.logicalIdentifier,
                              context.stagingPaths,
                              fileSize = if (declaredSize > 0L) declaredSize else fs.getFileStatus(targetPath).getLen,
                              fileMtimeEpochMs = archiveModifiedTime,
                              ignoreMatcher = context.ignoreMatcher,
                              archiveExpansionDepth = context.archiveExpansionDepth,
                              forceDisableDirectoryIdentifier = true
                            )
                          extractedEntries ++= childEntries
                          archiveErrors ++= childErrors
                          ignoredArchiveEntries.addAndGet(childIgnoredEntries)
                        }
                    }
                  }
                }
            }
        }
      } catch {
        case NonFatal(e) =>
          ArchiveIOUtil.drainInputStreamQuietly(entryInputStream)
          ArchiveIOUtil.addArchiveReadError(archiveErrors, context.datasetPath, context.timestamp, childLogicalIdentifier, e)
      }
    }

    openHandler(archiveErrors) match {
      case Left(errorMessage) =>
        addArchiveError(context.logicalIdentifier, errorMessage)
      case Right(handler) =>
        try {
          while (handler.nextEntry()) {
            handler.withEntryInputStream { entryInputStream =>
              processEntry(handler.entryName, handler.isDirectory, handler.entrySize, entryInputStream)
            }
          }
        } catch {
          case NonFatal(e) =>
            addArchiveError(context.logicalIdentifier, s"Archive read failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}")
        } finally {
          try {
            handler.close()
          } catch {
            case NonFatal(e) =>
              addArchiveError(context.logicalIdentifier, s"Archive read failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}")
          }
        }
    }

    (extractedEntries.toSeq, archiveErrors.toSeq, ignoredArchiveEntries.get())
  }
}
