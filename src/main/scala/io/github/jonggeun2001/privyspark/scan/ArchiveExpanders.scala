package io.github.jonggeun2001.privyspark.scan

import com.github.junrar.Archive
import com.github.junrar.exception.UnsupportedRarV5Exception
import io.github.jonggeun2001.privyspark.config.IgnoreMatcher
import io.github.jonggeun2001.privyspark.format.ByteProbe.{MagicProbeByteLimit, TextProbeByteLimit, inferMagicByteFormat, inferTextFormat}
import io.github.jonggeun2001.privyspark.format.CompressionStreams
import io.github.jonggeun2001.privyspark.format.FormatDetector
import io.github.jonggeun2001.privyspark.model.{ScanError, ScanFileEntry}
import io.github.jonggeun2001.privyspark.scan.ArchiveStaging._
import io.github.jonggeun2001.privyspark.util.{DriverLogger, PathIdentifiers}
import org.apache.commons.compress.PasswordRequiredException
import org.apache.commons.compress.archivers.sevenz.SevenZFile
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream
import org.apache.hadoop.fs.Path

import java.io.{ByteArrayOutputStream, Closeable, InputStream, OutputStream}
import java.nio.file.{Files => NioFiles}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

private[privyspark] object ArchiveExpanders {
  private val CopyBufferSize = 8192

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
    val stagedTargetPaths = mutable.Set.empty[String]
    var stagingPrepared = false

    def addArchiveError(fileIdentifier: String, message: String): Unit = {
      archiveErrors += ScanError(datasetPath, timestamp, fileIdentifier, message)
    }

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
      val childLogicalIdentifier = s"$logicalIdentifier!$normalizedEntryName"

      try {
        if (declaredSize == 0L) {
          DriverLogger.debug(
            "archive_entry_skipped",
            "archive" -> logicalIdentifier,
            "entry" -> childLogicalIdentifier,
            "reason" -> "zero_byte"
          )
          return
        }

        safeResolveArchiveEntryPath(stagingRoot, normalizedEntryName) match {
          case None =>
            val firstChunk = readFirstChunk(entryInputStream)
            if (firstChunk.isEmpty) {
              DriverLogger.debug(
                "archive_entry_skipped",
                "archive" -> logicalIdentifier,
                "entry" -> childLogicalIdentifier,
                "reason" -> "zero_byte"
              )
            } else {
              drainInputStream(entryInputStream)
              addArchiveError(childLogicalIdentifier, s"Unsafe archive entry path: $normalizedEntryName")
            }

          case Some(targetPath) =>
            ignoreMatcher.matched(childLogicalIdentifier, datasetPath) match {
              case Some(pattern) =>
                ignoredArchiveEntries.incrementAndGet()
                drainInputStream(entryInputStream)
                DriverLogger.debug(
                  "archive_entry_skipped",
                  "archive" -> logicalIdentifier,
                  "entry" -> childLogicalIdentifier,
                  "reason" -> "ignored",
                  "pattern" -> pattern
                )

              case None =>
                val firstChunk = readFirstChunk(entryInputStream)
                if (firstChunk.isEmpty) {
                  DriverLogger.debug(
                    "archive_entry_skipped",
                    "archive" -> logicalIdentifier,
                    "entry" -> childLogicalIdentifier,
                    "reason" -> "zero_byte"
                  )
                } else {
                  val pathFormat = FormatDetector.infer(normalizedEntryName)
                  val shouldRejectNestedArchive =
                    pathFormat.exists(ArchiveFormats.contains) && archiveExpansionDepth >= MaxArchiveExpansionDepth
                  val shouldRejectProbe =
                    pathFormat.isEmpty && FormatDetector.shouldSkipProbe(normalizedEntryName)
                  val initialBytes =
                    if (pathFormat.isDefined) {
                      Some(firstChunk)
                    } else if (shouldRejectProbe) {
                      None
                    } else {
                      val (probeBytes, detectedFormat) = probeEntryContent(firstChunk, entryInputStream)
                      if (detectedFormat.isDefined) Some(probeBytes) else None
                    }

                  if (shouldRejectNestedArchive) {
                    drainInputStream(entryInputStream)
                    addArchiveError(
                      childLogicalIdentifier,
                      s"Nested archive expansion is not supported: $childLogicalIdentifier"
                    )
                  } else if (shouldRejectProbe || initialBytes.isEmpty) {
                    drainInputStream(entryInputStream)
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
                        drainInputStream(entryInputStream)
                        addArchiveError(childLogicalIdentifier, errorMessage)

                      case Right(_) =>
                        var materializedSuccessfully = false
                        var cleanupPartialTarget = false
                        var outputStream: org.apache.hadoop.fs.FSDataOutputStream = null

                        try {
                          outputStream = fs.create(targetPath, true)
                          outputStream.write(bytesToMaterialize)
                          copyRemaining(entryInputStream, outputStream)
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
                                addArchiveError(
                                  childLogicalIdentifier,
                                  s"Archive entry cleanup failed: ${targetPath.toString}"
                                )
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
            }
        }
      } catch {
        case NonFatal(e) =>
          drainInputStreamQuietly(entryInputStream)
          addArchiveReadError(archiveErrors, datasetPath, timestamp, childLogicalIdentifier, e)
      }
    }

    FormatDetector.detect(archivePath).flatMap(_.archiveFormat) match {
      case Some(format) if format == ZipFormat || format == JarFormat =>
        expandZipLikeArchive(conf, archivePath, processEntry, archiveErrors, datasetPath, timestamp, logicalIdentifier)
      case Some(TarFormat) =>
        expandTarArchive(conf, archivePath, processEntry, archiveErrors, datasetPath, timestamp, logicalIdentifier)
      case Some(SevenZFormat) =>
        expandSevenZArchive(conf, archivePath, processEntry, archiveErrors, datasetPath, timestamp, logicalIdentifier)
      case Some(RarFormat) =>
        expandRarArchive(conf, archivePath, processEntry, archiveErrors, datasetPath, timestamp, logicalIdentifier)
      case Some(other) =>
        addArchiveError(logicalIdentifier, s"Archive read failed: Unsupported archive format: $other")
      case None =>
        addArchiveError(logicalIdentifier, s"Archive read failed: Unsupported archive format: $archivePath")
    }

    (extractedEntries.toSeq, archiveErrors.toSeq, ignoredArchiveEntries.get())
  }

  private def expandZipLikeArchive(
    conf: org.apache.hadoop.conf.Configuration,
    archivePath: String,
    processEntry: (String, Boolean, Long, InputStream) => Unit,
    archiveErrors: ArrayBuffer[ScanError],
    datasetPath: String,
    timestamp: String,
    logicalIdentifier: String
  ): Unit = {
    val sourcePath = new Path(archivePath)
    val fs = sourcePath.getFileSystem(conf)

    try {
      if (hasEncryptedZipEntry(fs, sourcePath, archiveErrors, datasetPath, timestamp, logicalIdentifier)) {
        archiveErrors += ScanError(
          datasetPath,
          timestamp,
          logicalIdentifier,
          s"Password-protected archive is not supported: $logicalIdentifier"
        )
      } else {
        var rawInputStream: InputStream = null
        var zipInputStream: ZipArchiveInputStream = null

        try {
          rawInputStream = fs.open(sourcePath)
          zipInputStream = new ZipArchiveInputStream(rawInputStream)
          var entry = zipInputStream.getNextZipEntry
          while (entry != null) {
            if (!entry.isDirectory) {
              val childLogicalIdentifier = archiveEntryLogicalIdentifier(logicalIdentifier, entry.getName)
              if (!zipInputStream.canReadEntryData(entry)) {
                archiveErrors += ScanError(
                  datasetPath,
                  timestamp,
                  childLogicalIdentifier,
                  s"Archive read failed: Unsupported ZIP feature: $childLogicalIdentifier"
                )
              } else {
                processEntry(entry.getName, entry.isDirectory, entry.getSize, zipInputStream)
              }
            }
            entry = zipInputStream.getNextZipEntry
          }
        } finally {
          closeArchiveResources(
            Seq(zipInputStream, rawInputStream),
            archiveErrors,
            datasetPath,
            timestamp,
            logicalIdentifier
          )
        }
      }
    } catch {
      case NonFatal(e) =>
        archiveErrors += ScanError(
          datasetPath,
          timestamp,
          logicalIdentifier,
          s"Archive read failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}"
        )
    }
  }

  private def expandTarArchive(
    conf: org.apache.hadoop.conf.Configuration,
    archivePath: String,
    processEntry: (String, Boolean, Long, InputStream) => Unit,
    archiveErrors: ArrayBuffer[ScanError],
    datasetPath: String,
    timestamp: String,
    logicalIdentifier: String
  ): Unit = {
    val sourcePath = new Path(archivePath)
    val fs = sourcePath.getFileSystem(conf)
    var rawInputStream: InputStream = null
    var archiveInputStream: InputStream = null
    var tarInputStream: TarArchiveInputStream = null

    try {
      rawInputStream = fs.open(sourcePath)
      archiveInputStream = CompressionStreams.wrapInputStream(
        rawInputStream,
        FormatDetector.detect(archivePath).flatMap(_.codec)
      )
      tarInputStream = new TarArchiveInputStream(archiveInputStream)
      var entry = tarInputStream.getNextEntry
      while (entry != null) {
        processEntry(entry.getName, entry.isDirectory, entry.getSize, tarInputStream)
        entry = tarInputStream.getNextEntry
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
      closeArchiveResources(
        Seq(tarInputStream, archiveInputStream, rawInputStream),
        archiveErrors,
        datasetPath,
        timestamp,
        logicalIdentifier
      )
    }
  }

  private def expandSevenZArchive(
    conf: org.apache.hadoop.conf.Configuration,
    archivePath: String,
    processEntry: (String, Boolean, Long, InputStream) => Unit,
    archiveErrors: ArrayBuffer[ScanError],
    datasetPath: String,
    timestamp: String,
    logicalIdentifier: String
  ): Unit = {
    try {
      withLocalArchiveFile(conf, archivePath) { localArchivePath =>
        if (isPasswordProtectedSevenZArchive(localArchivePath)) {
          archiveErrors += ScanError(
            datasetPath,
            timestamp,
            logicalIdentifier,
            s"Password-protected archive is not supported: $logicalIdentifier"
          )
        } else {
          var archiveFile: SevenZFile = null
          try {
            archiveFile = SevenZFile.builder().setFile(localArchivePath.toFile).get()
            var entry = archiveFile.getNextEntry
            while (entry != null) {
              withArchiveEntryInputStream(
                entry.getName,
                logicalIdentifier,
                archiveErrors,
                datasetPath,
                timestamp
              )(archiveFile.getInputStream(entry)) { entryInputStream =>
                processEntry(entry.getName, entry.isDirectory, entry.getSize, entryInputStream)
              }
              entry = archiveFile.getNextEntry
            }
          } catch {
            case _: PasswordRequiredException =>
              archiveErrors += ScanError(datasetPath, timestamp, logicalIdentifier, s"Password-protected archive is not supported: $logicalIdentifier")
            case NonFatal(e) =>
              archiveErrors += ScanError(
                datasetPath,
                timestamp,
                logicalIdentifier,
                s"Archive read failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}"
              )
          } finally {
            if (archiveFile != null) {
              archiveFile.close()
            }
          }
        }
      }
    } catch {
      case NonFatal(e) =>
        archiveErrors += ScanError(
          datasetPath,
          timestamp,
          logicalIdentifier,
          s"Archive read failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}"
        )
    }
  }

  private def expandRarArchive(
    conf: org.apache.hadoop.conf.Configuration,
    archivePath: String,
    processEntry: (String, Boolean, Long, InputStream) => Unit,
    archiveErrors: ArrayBuffer[ScanError],
    datasetPath: String,
    timestamp: String,
    logicalIdentifier: String
  ): Unit = {
    try {
      withLocalArchiveFile(conf, archivePath) { localArchivePath =>
        var archive: Archive = null
        try {
          archive = new Archive(localArchivePath.toFile)
          val fileHeaders = archive.getFileHeaders.asScala.toSeq
          val mainHeader = Option(archive.getMainHeader)
          val passwordProtected =
            archive.isPasswordProtected ||
              archive.isEncrypted ||
              mainHeader.exists(_.isEncrypted) ||
              fileHeaders.exists(_.isEncrypted)
          val multiVolume =
            mainHeader.exists(_.isMultiVolume) ||
              fileHeaders.exists(header => header.isSplitAfter || header.isSplitBefore)

          if (passwordProtected) {
            archiveErrors += ScanError(datasetPath, timestamp, logicalIdentifier, s"Password-protected archive is not supported: $logicalIdentifier")
          } else if (multiVolume) {
            archiveErrors += ScanError(datasetPath, timestamp, logicalIdentifier, s"Multi-volume archive is not supported: $logicalIdentifier")
          } else {
            fileHeaders.foreach { header =>
              if (!header.isDirectory) {
                withArchiveEntryInputStream(
                  header.getFileName,
                  logicalIdentifier,
                  archiveErrors,
                  datasetPath,
                  timestamp
                )(archive.getInputStream(header)) { entryInputStream =>
                  processEntry(header.getFileName, false, header.getFullUnpackSize, entryInputStream)
                }
              }
            }
          }
        } catch {
          case _: UnsupportedRarV5Exception =>
            archiveErrors += ScanError(datasetPath, timestamp, logicalIdentifier, s"RAR5 archives are not supported: $logicalIdentifier")
          case NonFatal(e) =>
            archiveErrors += ScanError(
              datasetPath,
              timestamp,
              logicalIdentifier,
              s"Archive read failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}"
            )
        } finally {
          if (archive != null) {
            archive.close()
          }
        }
      }
    } catch {
      case NonFatal(e) =>
        archiveErrors += ScanError(
          datasetPath,
          timestamp,
          logicalIdentifier,
          s"Archive read failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}"
        )
    }
  }

  private def withLocalArchiveFile[T](
    conf: org.apache.hadoop.conf.Configuration,
    archivePath: String
  )(block: java.nio.file.Path => T): T = {
    val sourcePath = new Path(archivePath)
    val fs = sourcePath.getFileSystem(conf)
    val tempDirectory = NioFiles.createTempDirectory("privyspark-archive-")
    val localArchivePath = tempDirectory.resolve(sourcePath.getName)
    val inputStream = fs.open(sourcePath)

    try {
      val outputStream = NioFiles.newOutputStream(localArchivePath)
      try {
        copyRemaining(inputStream, outputStream)
      } finally {
        outputStream.close()
      }
      block(localArchivePath)
    } finally {
      inputStream.close()
      NioFiles.deleteIfExists(localArchivePath)
      NioFiles.deleteIfExists(tempDirectory)
    }
  }

  private def readFirstChunk(inputStream: InputStream): Array[Byte] = {
    val buffer = new Array[Byte](CopyBufferSize)
    val bytesRead = readChunk(inputStream, buffer)
    if (bytesRead <= 0) Array.emptyByteArray else java.util.Arrays.copyOf(buffer, bytesRead)
  }

  private def probeEntryContent(
    firstChunk: Array[Byte],
    inputStream: InputStream
  ): (Array[Byte], Option[String]) = {
    val probeBuffer = new ByteArrayOutputStream()
    probeBuffer.write(firstChunk)
    var reachedEof = false

    while (probeBuffer.size() < TextProbeByteLimit && !reachedEof) {
      val probeChunk = new Array[Byte](math.min(CopyBufferSize, TextProbeByteLimit - probeBuffer.size()))
      val bytesRead = readChunk(inputStream, probeChunk)
      if (bytesRead < 0) {
        reachedEof = true
      } else if (bytesRead > 0) {
        probeBuffer.write(probeChunk, 0, bytesRead)
      }
    }

    val probeBytes = probeBuffer.toByteArray
    val magicProbeBytes = java.util.Arrays.copyOf(probeBytes, math.min(probeBytes.length, MagicProbeByteLimit))
    val inferredFormat = inferMagicByteFormat(magicProbeBytes)
      .orElse(inferTextFormat(probeBytes, allowIncompleteTrailingSequence = !reachedEof))
    (probeBytes, inferredFormat)
  }

  private def readChunk(inputStream: InputStream, buffer: Array[Byte]): Int = {
    var bytesRead = inputStream.read(buffer)
    if (bytesRead == 0) {
      val singleByte = inputStream.read()
      if (singleByte < 0) {
        bytesRead = -1
      } else {
        buffer(0) = singleByte.toByte
        bytesRead = 1
      }
    }
    bytesRead
  }

  private def copyRemaining(inputStream: InputStream, outputStream: OutputStream): Unit = {
    val buffer = new Array[Byte](CopyBufferSize)
    var bytesRead = readChunk(inputStream, buffer)
    while (bytesRead >= 0) {
      if (bytesRead > 0) {
        outputStream.write(buffer, 0, bytesRead)
      }
      bytesRead = readChunk(inputStream, buffer)
    }
  }

  private def drainInputStream(inputStream: InputStream): Unit = {
    val buffer = new Array[Byte](CopyBufferSize)
    var bytesRead = readChunk(inputStream, buffer)
    while (bytesRead >= 0) {
      bytesRead = readChunk(inputStream, buffer)
    }
  }

  private def drainInputStreamQuietly(inputStream: InputStream): Unit = {
    try {
      drainInputStream(inputStream)
    } catch {
      case NonFatal(_) =>
    }
  }

  private def withArchiveEntryInputStream(
    entryName: String,
    logicalIdentifier: String,
    archiveErrors: ArrayBuffer[ScanError],
    datasetPath: String,
    timestamp: String
  )(openInputStream: => InputStream)(processEntry: InputStream => Unit): Unit = {
    val childLogicalIdentifier = archiveEntryLogicalIdentifier(logicalIdentifier, entryName)
    var entryInputStream: InputStream = null

    try {
      entryInputStream = openInputStream
      processEntry(entryInputStream)
    } catch {
      case e: PasswordRequiredException =>
        throw e
      case NonFatal(e) =>
        addArchiveReadError(archiveErrors, datasetPath, timestamp, childLogicalIdentifier, e)
    } finally {
      closeArchiveEntryInputStream(entryInputStream, archiveErrors, datasetPath, timestamp, childLogicalIdentifier)
    }
  }

  private def closeArchiveEntryInputStream(
    entryInputStream: InputStream,
    archiveErrors: ArrayBuffer[ScanError],
    datasetPath: String,
    timestamp: String,
    childLogicalIdentifier: String
  ): Unit = {
    if (entryInputStream != null) {
      try {
        entryInputStream.close()
      } catch {
        case NonFatal(e) =>
          addArchiveReadError(archiveErrors, datasetPath, timestamp, childLogicalIdentifier, e)
      }
    }
  }

  private def isPasswordProtectedSevenZArchive(localArchivePath: java.nio.file.Path): Boolean = {
    var archiveFile: SevenZFile = null

    try {
      archiveFile = SevenZFile.builder().setFile(localArchivePath.toFile).get()
      archiveFile.getEntries.asScala.exists { entry =>
        if (entry.isDirectory || !entry.hasStream()) {
          false
        } else {
          try {
            val entryInputStream = archiveFile.getInputStream(entry)
            try {
              entryInputStream.read()
              false
            } catch {
              case _: PasswordRequiredException => true
              case NonFatal(_) => false
            } finally {
              try {
                entryInputStream.close()
              } catch {
                case _: PasswordRequiredException => return true
                case NonFatal(_) =>
              }
            }
          } catch {
            case _: PasswordRequiredException => true
            case NonFatal(_) => false
          }
        }
      }
    } catch {
      case _: PasswordRequiredException => true
    } finally {
      if (archiveFile != null) {
        archiveFile.close()
      }
    }
  }

  private def hasEncryptedZipEntry(
    fs: org.apache.hadoop.fs.FileSystem,
    sourcePath: Path,
    archiveErrors: ArrayBuffer[ScanError],
    datasetPath: String,
    timestamp: String,
    logicalIdentifier: String
  ): Boolean = {
    var rawInputStream: InputStream = null
    var zipInputStream: ZipArchiveInputStream = null

    try {
      rawInputStream = fs.open(sourcePath)
      zipInputStream = new ZipArchiveInputStream(rawInputStream)
      var entry = zipInputStream.getNextZipEntry
      while (entry != null) {
        if (Option(entry.getGeneralPurposeBit).exists(_.usesEncryption())) {
          return true
        }
        entry = zipInputStream.getNextZipEntry
      }
      false
    } finally {
      closeArchiveResources(
        Seq(zipInputStream, rawInputStream),
        archiveErrors,
        datasetPath,
        timestamp,
        logicalIdentifier
      )
    }
  }

  private def closeArchiveResources(
    resources: Seq[Closeable],
    archiveErrors: ArrayBuffer[ScanError],
    datasetPath: String,
    timestamp: String,
    logicalIdentifier: String
  ): Unit = {
    val distinctResources = resources.foldLeft(Vector.empty[Closeable]) {
      case (acc, null) => acc
      case (acc, resource) if acc.exists(existing => existing eq resource) => acc
      case (acc, resource) => acc :+ resource
    }

    var closeFailureRecorded = false
    distinctResources.foreach { resource =>
      try {
        resource.close()
      } catch {
        case NonFatal(e) if !closeFailureRecorded =>
          closeFailureRecorded = true
          archiveErrors += ScanError(
            datasetPath,
            timestamp,
            logicalIdentifier,
            s"Archive read failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}"
          )
        case NonFatal(_) =>
      }
    }
  }

  private def addArchiveReadError(
    archiveErrors: ArrayBuffer[ScanError],
    datasetPath: String,
    timestamp: String,
    fileIdentifier: String,
    error: Throwable
  ): Unit = {
    archiveErrors += ScanError(
      datasetPath,
      timestamp,
      fileIdentifier,
      s"Archive read failed: ${Option(error.getMessage).getOrElse(error.getClass.getSimpleName)}"
    )
  }

  private def archiveEntryLogicalIdentifier(logicalIdentifier: String, entryName: String): String = {
    s"$logicalIdentifier!${normalizeArchiveEntryName(entryName)}"
  }

}
