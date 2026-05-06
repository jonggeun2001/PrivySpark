package io.github.jonggeun2001.privyspark.review.fingerprint

import io.github.jonggeun2001.privyspark.scan.archive.ArchiveStaging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.io.{InputStream, OutputStream}
import java.nio.file.{Files => NioFiles}
import java.util.zip.CRC32
import scala.util.control.NonFatal

private[review] object Crc32Stream {
  val DefaultChecksumAlgo = "CRC32"
  private val CopyBufferSize = 8192

  private[fingerprint] def checksumFromInputStream(inputStream: InputStream): Either[String, (Long, String)] = {
    val crc32 = new CRC32()
    val buffer = new Array[Byte](CopyBufferSize)
    var totalBytes = 0L

    try {
      var bytesRead = inputStream.read(buffer)
      while (bytesRead >= 0) {
        if (bytesRead > 0) {
          crc32.update(buffer, 0, bytesRead)
          totalBytes += bytesRead.toLong
        }
        bytesRead = inputStream.read(buffer)
      }
      Right(totalBytes -> f"${crc32.getValue}%08x")
    } catch {
      case NonFatal(e) =>
        Left(s"Checksum calculation failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}")
    } finally {
      closeQuietly(inputStream)
    }
  }

  private[fingerprint] def crc32ForFile(conf: Configuration, physicalPath: String): Either[String, String] = {
    val path = new Path(physicalPath)
    val fs = path.getFileSystem(conf)
    var inputStream: InputStream = null

    try {
      inputStream = fs.open(path)
      checksumFromInputStream(inputStream).map(_._2)
    } catch {
      case NonFatal(e) =>
        closeQuietly(inputStream)
        Left(s"Checksum calculation failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}")
    }
  }

  private[fingerprint] def withLocalArchiveFile[T](
    conf: Configuration,
    archivePath: String
  )(block: java.nio.file.Path => Either[String, T]): Either[String, T] = {
    val sourcePath = new Path(archivePath)
    val fs = sourcePath.getFileSystem(conf)
    val tempDirectory = NioFiles.createTempDirectory("privyspark-review-archive-")
    val localArchivePath = tempDirectory.resolve(sourcePath.getName)
    var inputStream: InputStream = null
    var outputStream: OutputStream = null

    try {
      inputStream = fs.open(sourcePath)
      outputStream = NioFiles.newOutputStream(localArchivePath)
      val buffer = new Array[Byte](CopyBufferSize)
      var bytesRead = inputStream.read(buffer)
      while (bytesRead >= 0) {
        if (bytesRead > 0) {
          outputStream.write(buffer, 0, bytesRead)
        }
        bytesRead = inputStream.read(buffer)
      }
      block(localArchivePath)
    } catch {
      case NonFatal(e) =>
        Left(s"Archive read failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}")
    } finally {
      closeQuietly(outputStream)
      closeQuietly(inputStream)
      closeQuietly(localArchivePath)
      closeQuietly(tempDirectory)
    }
  }

  private[fingerprint] def normalizeArchiveEntryName(entryName: String): String =
    ArchiveStaging.normalizeArchiveEntryName(entryName)

  private[fingerprint] def closeQuietly(resource: AutoCloseable): Unit = {
    if (resource != null) {
      try {
        resource.close()
      } catch {
        case NonFatal(_) => ()
      }
    }
  }

  private def closeQuietly(path: java.nio.file.Path): Unit = {
    if (path != null) {
      try {
        NioFiles.deleteIfExists(path)
      } catch {
        case NonFatal(_) => ()
      }
    }
  }
}
