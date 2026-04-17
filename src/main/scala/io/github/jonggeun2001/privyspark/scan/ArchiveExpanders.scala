package io.github.jonggeun2001.privyspark.scan

import com.github.junrar.Archive
import com.github.junrar.exception.UnsupportedRarV5Exception
import io.github.jonggeun2001.privyspark.config.IgnoreMatcher
import io.github.jonggeun2001.privyspark.format.CompressionStreams
import io.github.jonggeun2001.privyspark.format.FormatDetector
import io.github.jonggeun2001.privyspark.model.{ScanError, ScanFileEntry}
import io.github.jonggeun2001.privyspark.scan.ArchiveStaging._
import io.github.jonggeun2001.privyspark.util.{DriverLogger, PathIdentifiers}
import org.apache.commons.compress.PasswordRequiredException
import org.apache.commons.compress.archivers.sevenz.SevenZFile
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.hadoop.fs.Path

import java.io.{InputStream, OutputStream}
import java.nio.file.{Files => NioFiles}
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.ZipInputStream
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
                    var outputStream: org.apache.hadoop.fs.FSDataOutputStream = null
                    try {
                      outputStream = fs.create(targetPath, true)
                      outputStream.write(firstChunk)
                      copyRemaining(entryInputStream, outputStream)
                    } catch {
                      case NonFatal(e) =>
                        addArchiveError(
                          childLogicalIdentifier,
                          s"Archive entry materialization failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}"
                        )
                    } finally {
                      if (outputStream != null) {
                        outputStream.close()
                      }
                    }

                    if (fs.exists(targetPath) && fs.getFileStatus(targetPath).getLen > 0L) {
                      val (childEntries, childErrors, childIgnoredEntries) = SourceExpansion.expandPhysicalSource(
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

    FormatDetector.detect(archivePath).flatMap(_.archiveFormat) match {
      case Some(format) if format == ZipFormat || format == JarFormat =>
        expandZipLikeArchive(sourcePath, fs, processEntry, archiveErrors, datasetPath, timestamp, logicalIdentifier)
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
    sourcePath: Path,
    fs: org.apache.hadoop.fs.FileSystem,
    processEntry: (String, Boolean, Long, InputStream) => Unit,
    archiveErrors: ArrayBuffer[ScanError],
    datasetPath: String,
    timestamp: String,
    logicalIdentifier: String
  ): Unit = {
    val archiveInputStream = fs.open(sourcePath)
    val zipInputStream = new ZipInputStream(archiveInputStream)

    try {
      var entry = zipInputStream.getNextEntry
      while (entry != null) {
        try {
          processEntry(entry.getName, entry.isDirectory, entry.getSize, zipInputStream)
        } finally {
          zipInputStream.closeEntry()
        }
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
    val rawInputStream = fs.open(sourcePath)
    val archiveInputStream = CompressionStreams.wrapInputStream(
      rawInputStream,
      FormatDetector.detect(archivePath).flatMap(_.codec)
    )
    val tarInputStream = new TarArchiveInputStream(archiveInputStream)

    try {
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
      tarInputStream.close()
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
    withLocalArchiveFile(conf, archivePath) { localArchivePath =>
      var archiveFile: SevenZFile = null
      try {
        archiveFile = SevenZFile.builder().setFile(localArchivePath.toFile).get()
        var entry = archiveFile.getNextEntry
        while (entry != null) {
          val entryInputStream = archiveFile.getInputStream(entry)
          try {
            processEntry(entry.getName, entry.isDirectory, entry.getSize, entryInputStream)
          } finally {
            entryInputStream.close()
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

  private def expandRarArchive(
    conf: org.apache.hadoop.conf.Configuration,
    archivePath: String,
    processEntry: (String, Boolean, Long, InputStream) => Unit,
    archiveErrors: ArrayBuffer[ScanError],
    datasetPath: String,
    timestamp: String,
    logicalIdentifier: String
  ): Unit = {
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
              val entryInputStream = archive.getInputStream(header)
              try {
                processEntry(header.getFileName, false, header.getFullUnpackSize, entryInputStream)
              } finally {
                entryInputStream.close()
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

}
