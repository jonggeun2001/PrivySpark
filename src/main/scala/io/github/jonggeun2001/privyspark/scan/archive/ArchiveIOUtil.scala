package io.github.jonggeun2001.privyspark.scan.archive

import io.github.jonggeun2001.privyspark.format.ByteProbe.{MagicProbeByteLimit, TextProbeByteLimit, inferMagicByteFormat, inferTextFormat}
import io.github.jonggeun2001.privyspark.model.ScanError
import io.github.jonggeun2001.privyspark.scan.ArchiveStaging.normalizeArchiveEntryName
import org.apache.commons.compress.PasswordRequiredException
import org.apache.commons.compress.archivers.sevenz.SevenZFile
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream
import org.apache.hadoop.fs.Path

import java.io.{ByteArrayOutputStream, Closeable, InputStream, OutputStream}
import java.nio.file.{Files => NioFiles}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

private[privyspark] object ArchiveIOUtil {
  private val CopyBufferSize = 8192

  def withLocalArchiveFile[T](
    conf: org.apache.hadoop.conf.Configuration,
    archivePath: String
  )(block: java.nio.file.Path => T): T = {
    val sourcePath = new Path(archivePath)
    val fs = sourcePath.getFileSystem(conf)
    val tempDirectory = NioFiles.createTempDirectory("privyspark-archive-")
    val localArchivePath = tempDirectory.resolve(sourcePath.getName)
    var inputStream: InputStream = null
    var result: Option[T] = None
    var failure: Throwable = null

    def recordFailure(error: Throwable): Unit = {
      if (failure == null) {
        failure = error
      } else if (failure ne error) {
        failure.addSuppressed(error)
      }
    }

    def attempt(action: => Unit): Unit = {
      try {
        action
      } catch {
        case error: Throwable =>
          recordFailure(error)
      }
    }

    try {
      inputStream = fs.open(sourcePath)
      var outputStream: OutputStream = null

      try {
        outputStream = NioFiles.newOutputStream(localArchivePath)
        copyRemaining(inputStream, outputStream)
        result = Some(block(localArchivePath))
      } catch {
        case error: Throwable =>
          recordFailure(error)
      } finally {
        if (outputStream != null) {
          attempt(outputStream.close())
        }
      }
    } catch {
      case error: Throwable =>
        recordFailure(error)
    } finally {
      if (inputStream != null) {
        attempt(inputStream.close())
      }
      attempt(NioFiles.deleteIfExists(localArchivePath))
      attempt(NioFiles.deleteIfExists(tempDirectory))
    }

    if (failure != null) {
      throw failure
    }
    result.get
  }

  def readFirstChunk(inputStream: InputStream): Array[Byte] = {
    val buffer = new Array[Byte](CopyBufferSize)
    val bytesRead = readChunk(inputStream, buffer)
    if (bytesRead <= 0) Array.emptyByteArray else java.util.Arrays.copyOf(buffer, bytesRead)
  }

  def probeEntryContent(
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

  def copyRemaining(inputStream: InputStream, outputStream: OutputStream): Unit = {
    val buffer = new Array[Byte](CopyBufferSize)
    var bytesRead = readChunk(inputStream, buffer)
    while (bytesRead >= 0) {
      if (bytesRead > 0) {
        outputStream.write(buffer, 0, bytesRead)
      }
      bytesRead = readChunk(inputStream, buffer)
    }
  }

  def drainInputStream(inputStream: InputStream): Unit = {
    val buffer = new Array[Byte](CopyBufferSize)
    var bytesRead = readChunk(inputStream, buffer)
    while (bytesRead >= 0) {
      bytesRead = readChunk(inputStream, buffer)
    }
  }

  def drainInputStreamQuietly(inputStream: InputStream): Unit = {
    try {
      drainInputStream(inputStream)
    } catch {
      case NonFatal(_) =>
    }
  }

  def withArchiveEntryInputStream(
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

  def closeArchiveResources(
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

  def addArchiveReadError(
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

  def hasEncryptedZipEntry(
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

  def isPasswordProtectedSevenZArchive(localArchivePath: java.nio.file.Path): Boolean = {
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

  def archiveEntryLogicalIdentifier(logicalIdentifier: String, entryName: String): String = {
    s"$logicalIdentifier!${normalizeArchiveEntryName(entryName)}"
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
}
