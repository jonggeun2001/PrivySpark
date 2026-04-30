package io.github.jonggeun2001.privyspark.review

import com.github.junrar.Archive
import com.github.junrar.exception.UnsupportedRarV5Exception
import io.github.jonggeun2001.privyspark.format.ByteProbe.detectPhysicalFormat
import io.github.jonggeun2001.privyspark.format.CompressionStreams
import io.github.jonggeun2001.privyspark.format.CsvInference.XlsxFormat
import io.github.jonggeun2001.privyspark.format.FormatDetector
import io.github.jonggeun2001.privyspark.format.WorkbookHelpers.listVisibleWorkbookSheets
import io.github.jonggeun2001.privyspark.scan.archive.ArchiveStaging._
import io.github.jonggeun2001.privyspark.util.PathIdentifiers.resolveRelativeIdentifier
import org.apache.commons.compress.PasswordRequiredException
import org.apache.commons.compress.archivers.sevenz.SevenZFile
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.io.{InputStream, OutputStream}
import java.nio.file.{Files => NioFiles}
import java.util.zip.CRC32
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

object FileIdentifierResolver {
  val DefaultChecksumAlgo = "CRC32"
  private val CopyBufferSize = 8192

  def resolveFingerprints(
    conf: Configuration,
    inputRoot: String,
    fileIdentifier: String
  ): Either[String, Seq[ResolvedFileFingerprint]] = {
    parseArchiveIdentifier(fileIdentifier) match {
      case Some((archiveIdentifier, entryName)) =>
        resolveArchiveEntryFingerprint(conf, inputRoot, archiveIdentifier, entryName, fileIdentifier).map(Seq(_))
      case None =>
        parseWorkbookIdentifier(conf, inputRoot, fileIdentifier) match {
          case Some((workbookIdentifier, sheetName)) =>
            resolveWorkbookFingerprint(conf, inputRoot, workbookIdentifier, sheetName, fileIdentifier).map(Seq(_))
          case None =>
            resolvePathFingerprint(conf, inputRoot, fileIdentifier)
        }
    }
  }

  private def resolvePathFingerprint(
    conf: Configuration,
    inputRoot: String,
    fileIdentifier: String
  ): Either[String, Seq[ResolvedFileFingerprint]] = {
    val resolvedPath = resolveInputPath(conf, inputRoot, fileIdentifier)
    val path = new Path(resolvedPath)
    val fs = path.getFileSystem(conf)
    if (!fs.exists(path)) {
      Left(s"Resolved path not found: $fileIdentifier")
    } else {
      val status = fs.getFileStatus(path)
      if (status.isDirectory) {
        resolveDirectoryFingerprints(conf, inputRoot, path)
      } else {
        resolveFlatFileFingerprint(conf, fileIdentifier, resolvedPath, status).map(Seq(_))
      }
    }
  }

  private def resolveDirectoryFingerprints(
    conf: Configuration,
    inputRoot: String,
    directoryPath: Path
  ): Either[String, Seq[ResolvedFileFingerprint]] = {
    val fs = directoryPath.getFileSystem(conf)
    val statuses = Option(fs.listStatus(directoryPath)).getOrElse(Array.empty)
      .filter(status => status.isFile)
      .sortBy(_.getPath.toString)

    val fingerprints = scala.collection.mutable.ArrayBuffer.empty[ResolvedFileFingerprint]
    statuses.foreach { status =>
      val childPath = status.getPath.toString
      val childIdentifier = resolveRelativeIdentifier(inputRoot, childPath)
      resolveFlatFileFingerprint(conf, childIdentifier, childPath, status) match {
        case Right(fingerprint) =>
          fingerprints += fingerprint
        case Left(errorMessage) =>
          return Left(errorMessage)
      }
    }

    Right(fingerprints.toSeq)
  }

  private def resolveFlatFileFingerprint(
    conf: Configuration,
    fileIdentifier: String,
    physicalPath: String,
    status: org.apache.hadoop.fs.FileStatus
  ): Either[String, ResolvedFileFingerprint] = {
    crc32ForFile(conf, physicalPath).map { checksum =>
      ResolvedFileFingerprint(
        fileIdentifier = fileIdentifier,
        physicalPath = physicalPath,
        fileSize = status.getLen,
        fileMtimeEpochMs = status.getModificationTime,
        fileChecksumAlgo = DefaultChecksumAlgo,
        fileChecksum = checksum
      )
    }
  }

  private def resolveWorkbookFingerprint(
    conf: Configuration,
    inputRoot: String,
    workbookIdentifier: String,
    sheetName: String,
    originalIdentifier: String
  ): Either[String, ResolvedFileFingerprint] = {
    val workbookPath = resolveInputPath(conf, inputRoot, workbookIdentifier)
    val path = new Path(workbookPath)
    val fs = path.getFileSystem(conf)

    if (!fs.exists(path)) {
      Left(s"Workbook path not found: $originalIdentifier")
    } else {
      listVisibleWorkbookSheets(conf, workbookPath) match {
        case Right(sheetNames) if sheetNames.contains(sheetName) =>
          val status = fs.getFileStatus(path)
          crc32ForFile(conf, workbookPath).map { checksum =>
            ResolvedFileFingerprint(
              fileIdentifier = originalIdentifier,
              physicalPath = workbookPath,
              fileSize = status.getLen,
              fileMtimeEpochMs = status.getModificationTime,
              fileChecksumAlgo = DefaultChecksumAlgo,
              fileChecksum = checksum
            )
          }
        case Right(_) =>
          Left(s"Workbook sheet not found: $originalIdentifier")
        case Left(errorMessage) =>
          Left(s"Workbook read failed: $errorMessage")
      }
    }
  }

  private def resolveArchiveEntryFingerprint(
    conf: Configuration,
    inputRoot: String,
    archiveIdentifier: String,
    entryName: String,
    originalIdentifier: String
  ): Either[String, ResolvedFileFingerprint] = {
    val archivePath = resolveInputPath(conf, inputRoot, archiveIdentifier)
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
        if (!entry.isDirectory && normalizeArchiveEntryName(entry.getName) == entryName) {
          return checksumFromInputStream(zipInputStream).map {
            case (fileSize, checksum) =>
              ResolvedFileFingerprint(
                fileIdentifier = originalIdentifier,
                physicalPath = archivePath,
                fileSize = if (entry.getSize >= 0L) entry.getSize else fileSize,
                fileMtimeEpochMs = status.getModificationTime,
                fileChecksumAlgo = DefaultChecksumAlgo,
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
      closeQuietly(zipInputStream)
      closeQuietly(rawInputStream)
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
        if (!entry.isDirectory && normalizeArchiveEntryName(entry.getName) == entryName) {
          return checksumFromInputStream(tarInputStream).map {
            case (fileSize, checksum) =>
              ResolvedFileFingerprint(
                fileIdentifier = originalIdentifier,
                physicalPath = archivePath,
                fileSize = if (entry.getSize >= 0L) entry.getSize else fileSize,
                fileMtimeEpochMs = status.getModificationTime,
                fileChecksumAlgo = DefaultChecksumAlgo,
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
      closeQuietly(tarInputStream)
      closeQuietly(archiveInputStream)
      closeQuietly(rawInputStream)
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
      withLocalArchiveFile(conf, archivePath) { localArchivePath =>
        var archiveFile: SevenZFile = null
        try {
          archiveFile = SevenZFile.builder().setFile(localArchivePath.toFile).get()
          var entry = archiveFile.getNextEntry
          while (entry != null) {
            if (!entry.isDirectory && normalizeArchiveEntryName(entry.getName) == entryName) {
              val entryInputStream = archiveFile.getInputStream(entry)
              checksumFromInputStream(entryInputStream) match {
                case Right((fileSize, checksum)) =>
                  return Right(
                    ResolvedFileFingerprint(
                      fileIdentifier = originalIdentifier,
                      physicalPath = archivePath,
                      fileSize = if (entry.getSize >= 0L) entry.getSize else fileSize,
                      fileMtimeEpochMs = status.getModificationTime,
                      fileChecksumAlgo = DefaultChecksumAlgo,
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
          closeQuietly(archiveFile)
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
            Left(s"Password-protected archive is not supported: $originalIdentifier")
          } else if (multiVolume) {
            Left(s"Multi-volume archive is not supported: $originalIdentifier")
          } else {
            fileHeaders.find(header => !header.isDirectory && normalizeArchiveEntryName(header.getFileName) == entryName) match {
              case Some(header) =>
                val entryInputStream = archive.getInputStream(header)
                checksumFromInputStream(entryInputStream).map {
                  case (fileSize, checksum) =>
                    ResolvedFileFingerprint(
                      fileIdentifier = originalIdentifier,
                      physicalPath = archivePath,
                      fileSize = if (header.getFullUnpackSize >= 0L) header.getFullUnpackSize else fileSize,
                      fileMtimeEpochMs = status.getModificationTime,
                      fileChecksumAlgo = DefaultChecksumAlgo,
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
          closeQuietly(archive)
        }
      }
    } catch {
      case NonFatal(e) =>
        Left(s"Archive read failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}")
    }
  }

  private def checksumFromInputStream(inputStream: InputStream): Either[String, (Long, String)] = {
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

  private def crc32ForFile(conf: Configuration, physicalPath: String): Either[String, String] = {
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

  private def parseArchiveIdentifier(fileIdentifier: String): Option[(String, String)] = {
    val separatorIndex = Option(fileIdentifier).getOrElse("").indexOf('!')
    if (separatorIndex > 0 && separatorIndex + 1 < fileIdentifier.length) {
      Some(fileIdentifier.substring(0, separatorIndex) -> fileIdentifier.substring(separatorIndex + 1))
    } else {
      None
    }
  }

  private def parseWorkbookIdentifier(
    conf: Configuration,
    inputRoot: String,
    fileIdentifier: String
  ): Option[(String, String)] = {
    val separatorIndex = Option(fileIdentifier).getOrElse("").lastIndexOf('#')
    if (separatorIndex <= 0 || separatorIndex + 1 >= fileIdentifier.length) {
      None
    } else {
      val workbookIdentifier = fileIdentifier.substring(0, separatorIndex)
      val workbookPath = resolveInputPath(conf, inputRoot, workbookIdentifier)
      val path = new Path(workbookPath)
      val fs = path.getFileSystem(conf)

      if (fs.exists(path) && detectPhysicalFormat(conf, workbookPath).contains(XlsxFormat)) {
        Some(workbookIdentifier -> fileIdentifier.substring(separatorIndex + 1))
      } else {
        None
      }
    }
  }

  private def resolveInputPath(
    conf: Configuration,
    inputRoot: String,
    relativeIdentifier: String
  ): String = {
    val inputPath = new Path(inputRoot)
    val fs = inputPath.getFileSystem(conf)
    val inputIsFile = fs.exists(inputPath) && fs.getFileStatus(inputPath).isFile
    val normalizedIdentifier = Option(relativeIdentifier).getOrElse("")

    if (normalizedIdentifier == "." || normalizedIdentifier.isEmpty) {
      inputRoot
    } else if (inputIsFile && normalizedIdentifier == inputPath.getName) {
      inputRoot
    } else if (inputIsFile) {
      Option(inputPath.getParent).map(parent => new Path(parent, normalizedIdentifier).toString).getOrElse(inputRoot)
    } else {
      new Path(inputPath, normalizedIdentifier).toString
    }
  }

  private def withLocalArchiveFile[T](
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

  private def closeQuietly(resource: AutoCloseable): Unit = {
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
