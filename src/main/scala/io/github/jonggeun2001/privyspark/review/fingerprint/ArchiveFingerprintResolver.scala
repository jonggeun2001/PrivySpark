package io.github.jonggeun2001.privyspark.review.fingerprint

import com.github.junrar.Archive
import com.github.junrar.exception.UnsupportedRarV5Exception
import io.github.jonggeun2001.privyspark.format.CompressionStreams
import io.github.jonggeun2001.privyspark.format.FormatDetector
import io.github.jonggeun2001.privyspark.review.ResolvedFileFingerprint
import io.github.jonggeun2001.privyspark.scan.archive.ArchiveStaging._
import org.apache.commons.compress.PasswordRequiredException
import org.apache.commons.compress.archivers.sevenz.SevenZFile
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.io.InputStream
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

private[review] object ArchiveFingerprintResolver {
  def parseIdentifier(fileIdentifier: String): Option[(String, String)] = {
    val separatorIndex = Option(fileIdentifier).getOrElse("").indexOf('!')
    if (separatorIndex > 0 && separatorIndex + 1 < fileIdentifier.length) {
      Some(fileIdentifier.substring(0, separatorIndex) -> fileIdentifier.substring(separatorIndex + 1))
    } else {
      None
    }
  }

  def resolve(
    conf: Configuration,
    inputRoot: String,
    archiveIdentifier: String,
    entryName: String,
    originalIdentifier: String
  ): Either[String, ResolvedFileFingerprint] = {
    val archivePath = PathFingerprintResolver.resolveInputPath(conf, inputRoot, archiveIdentifier)
    val path = new Path(archivePath)
    val fs = path.getFileSystem(conf)
    if (!fs.exists(path)) {
      return Left(s"Archive path not found: $originalIdentifier")
    }

    val status = fs.getFileStatus(path)
    val archiveFormat = FormatDetector.detect(archivePath).flatMap(_.archiveFormat)

    archiveFormat match {
      case Some(format) if format == ZipFormat || format == JarFormat =>
        resolveZipEntryFingerprint(conf, archivePath, entryName, originalIdentifier, status)
      case Some(TarFormat) =>
        resolveTarEntryFingerprint(conf, archivePath, entryName, originalIdentifier, status)
      case Some(SevenZFormat) =>
        resolveSevenZEntryFingerprint(conf, archivePath, entryName, originalIdentifier, status)
      case Some(RarFormat) =>
        resolveRarEntryFingerprint(conf, archivePath, entryName, originalIdentifier, status)
      case Some(other) =>
        Left(s"Unsupported archive format: $other")
      case None =>
        Left(s"Unsupported archive format: $originalIdentifier")
    }
  }

  private def resolveZipEntryFingerprint(
    conf: Configuration,
    archivePath: String,
    entryName: String,
    originalIdentifier: String,
    status: org.apache.hadoop.fs.FileStatus
  ): Either[String, ResolvedFileFingerprint] = {
    val sourcePath = new Path(archivePath)
    val fs = sourcePath.getFileSystem(conf)
    var rawInputStream: InputStream = null
    var zipInputStream: ZipArchiveInputStream = null

    try {
      rawInputStream = fs.open(sourcePath)
      zipInputStream = new ZipArchiveInputStream(rawInputStream)
      var entry = zipInputStream.getNextZipEntry
      while (entry != null) {
        if (!entry.isDirectory && Crc32Stream.normalizeArchiveEntryName(entry.getName) == entryName) {
          return Crc32Stream.checksumFromInputStream(zipInputStream).map {
            case (fileSize, checksum) =>
              ResolvedFileFingerprint(
                fileIdentifier = originalIdentifier,
                physicalPath = archivePath,
                fileSize = if (entry.getSize >= 0L) entry.getSize else fileSize,
                fileMtimeEpochMs = status.getModificationTime,
                fileChecksumAlgo = Crc32Stream.DefaultChecksumAlgo,
                fileChecksum = checksum
              )
          }
        }
        entry = zipInputStream.getNextZipEntry
      }
      Left(s"Archive entry not found: $originalIdentifier")
    } catch {
      case NonFatal(e) =>
        Left(s"Archive read failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}")
    } finally {
      Crc32Stream.closeQuietly(zipInputStream)
      Crc32Stream.closeQuietly(rawInputStream)
    }
  }

  private def resolveTarEntryFingerprint(
    conf: Configuration,
    archivePath: String,
    entryName: String,
    originalIdentifier: String,
    status: org.apache.hadoop.fs.FileStatus
  ): Either[String, ResolvedFileFingerprint] = {
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
        if (!entry.isDirectory && Crc32Stream.normalizeArchiveEntryName(entry.getName) == entryName) {
          return Crc32Stream.checksumFromInputStream(tarInputStream).map {
            case (fileSize, checksum) =>
              ResolvedFileFingerprint(
                fileIdentifier = originalIdentifier,
                physicalPath = archivePath,
                fileSize = if (entry.getSize >= 0L) entry.getSize else fileSize,
                fileMtimeEpochMs = status.getModificationTime,
                fileChecksumAlgo = Crc32Stream.DefaultChecksumAlgo,
                fileChecksum = checksum
              )
          }
        }
        entry = tarInputStream.getNextEntry
      }
      Left(s"Archive entry not found: $originalIdentifier")
    } catch {
      case NonFatal(e) =>
        Left(s"Archive read failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}")
    } finally {
      Crc32Stream.closeQuietly(tarInputStream)
      Crc32Stream.closeQuietly(archiveInputStream)
      Crc32Stream.closeQuietly(rawInputStream)
    }
  }

  private def resolveSevenZEntryFingerprint(
    conf: Configuration,
    archivePath: String,
    entryName: String,
    originalIdentifier: String,
    status: org.apache.hadoop.fs.FileStatus
  ): Either[String, ResolvedFileFingerprint] = {
    try {
      Crc32Stream.withLocalArchiveFile(conf, archivePath) { localArchivePath =>
        var archiveFile: SevenZFile = null
        try {
          archiveFile = SevenZFile.builder().setFile(localArchivePath.toFile).get()
          var entry = archiveFile.getNextEntry
          while (entry != null) {
            if (!entry.isDirectory && Crc32Stream.normalizeArchiveEntryName(entry.getName) == entryName) {
              val entryInputStream = archiveFile.getInputStream(entry)
              Crc32Stream.checksumFromInputStream(entryInputStream) match {
                case Right((fileSize, checksum)) =>
                  return Right(
                    ResolvedFileFingerprint(
                      fileIdentifier = originalIdentifier,
                      physicalPath = archivePath,
                      fileSize = if (entry.getSize >= 0L) entry.getSize else fileSize,
                      fileMtimeEpochMs = status.getModificationTime,
                      fileChecksumAlgo = Crc32Stream.DefaultChecksumAlgo,
                      fileChecksum = checksum
                    )
                  )
                case Left(errorMessage) =>
                  return Left(errorMessage)
              }
            }
            entry = archiveFile.getNextEntry
          }
          Left(s"Archive entry not found: $originalIdentifier")
        } catch {
          case _: PasswordRequiredException =>
            Left(s"Password-protected archive is not supported: $originalIdentifier")
          case NonFatal(e) =>
            Left(s"Archive read failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}")
        } finally {
          Crc32Stream.closeQuietly(archiveFile)
        }
      }
    } catch {
      case NonFatal(e) =>
        Left(s"Archive read failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}")
    }
  }

  private def resolveRarEntryFingerprint(
    conf: Configuration,
    archivePath: String,
    entryName: String,
    originalIdentifier: String,
    status: org.apache.hadoop.fs.FileStatus
  ): Either[String, ResolvedFileFingerprint] = {
    try {
      Crc32Stream.withLocalArchiveFile(conf, archivePath) { localArchivePath =>
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
            Left(s"Password-protected archive is not supported: $originalIdentifier")
          } else if (multiVolume) {
            Left(s"Multi-volume archive is not supported: $originalIdentifier")
          } else {
            fileHeaders.find(header => !header.isDirectory && Crc32Stream.normalizeArchiveEntryName(header.getFileName) == entryName) match {
              case Some(header) =>
                val entryInputStream = archive.getInputStream(header)
                Crc32Stream.checksumFromInputStream(entryInputStream).map {
                  case (fileSize, checksum) =>
                    ResolvedFileFingerprint(
                      fileIdentifier = originalIdentifier,
                      physicalPath = archivePath,
                      fileSize = if (header.getFullUnpackSize >= 0L) header.getFullUnpackSize else fileSize,
                      fileMtimeEpochMs = status.getModificationTime,
                      fileChecksumAlgo = Crc32Stream.DefaultChecksumAlgo,
                      fileChecksum = checksum
                    )
                }
              case None =>
                Left(s"Archive entry not found: $originalIdentifier")
            }
          }
        } catch {
          case _: UnsupportedRarV5Exception =>
            Left(s"RAR5 archives are not supported: $originalIdentifier")
          case NonFatal(e) =>
            Left(s"Archive read failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}")
        } finally {
          Crc32Stream.closeQuietly(archive)
        }
      }
    } catch {
      case NonFatal(e) =>
        Left(s"Archive read failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}")
    }
  }
}
