package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.fsio.ManagedPaths.deleteStagingPath
import io.github.jonggeun2001.privyspark.model.ScanGroup
import io.github.jonggeun2001.privyspark.review.{FileIdentifierResolver, RecordedFileFingerprint, ReviewScopeFingerprintCodec, ReviewScopeIdentifierCodec}
import io.github.jonggeun2001.privyspark.util.{DriverLogger, DriverTcpConnectionLogger}
import io.github.jonggeun2001.privyspark.util.PathIdentifiers.{resolveLogicalIdentifier, resolvePhysicalPath}
import org.apache.hadoop.fs.Path

import java.io.{InputStream, OutputStream}
import java.util.UUID
import java.util.zip.CRC32
import scala.util.control.NonFatal

private[privyspark] object ReviewSnapshotLog {
  final case class StagedFileSnapshot(
    stagedRoot: String,
    stagedPath: String,
    recordedFingerprint: RecordedFileFingerprint
  )

  private val CopyBufferSize = 8192

  def stageFileSnapshot(
    conf: org.apache.hadoop.conf.Configuration,
    datasetPath: String,
    group: ScanGroup,
    sourceKey: String
  ): Either[String, StagedFileSnapshot] = {
    val sourcePath = new Path(resolvePhysicalPath(group, sourceKey))
    val sourceFs = sourcePath.getFileSystem(conf)
    val logicalIdentifier = resolveLogicalIdentifier(group, datasetPath, sourceKey)
    val stagingBase = new Path(sourceFs.getHomeDirectory, ".privyspark-scan-staging")
    val stagingRoot = new Path(stagingBase, UUID.randomUUID().toString)
    val stagedPath = new Path(stagingRoot, sourcePath.getName)
    var inputStream: InputStream = null
    var outputStream: OutputStream = null
    var bytesCopied = 0L
    val startNanos = System.nanoTime()

    try {
      DriverTcpConnectionLogger.debugSnapshot(
        "group_scan_tcp_snapshot",
        "phase" -> "review_snapshot_stage_start",
        "file" -> sourcePath.toString,
        "file_identifier" -> logicalIdentifier
      )
      val sourceStatus = sourceFs.getFileStatus(sourcePath)
      val expectedFileSize = group.fileSizesByKey.getOrElse(sourceKey, sourceStatus.getLen)
      val expectedFileMtimeEpochMs = group.fileMtimesByKey.getOrElse(sourceKey, sourceStatus.getModificationTime)
      if (!sourceFs.exists(stagingBase) && !sourceFs.mkdirs(stagingBase)) {
        return Left(s"Scan staging base creation failed: ${stagingBase.toString}")
      }
      if (!sourceFs.mkdirs(stagingRoot) && !sourceFs.exists(stagingRoot)) {
        return Left(s"Scan staging directory creation failed: ${stagingRoot.toString}")
      }

      inputStream = sourceFs.open(sourcePath)
      outputStream = sourceFs.create(stagedPath, true)
      val crc32 = new CRC32()
      val buffer = new Array[Byte](CopyBufferSize)
      var bytesRead = inputStream.read(buffer)
      while (bytesRead >= 0) {
        if (bytesRead > 0) {
          outputStream.write(buffer, 0, bytesRead)
          crc32.update(buffer, 0, bytesRead)
          bytesCopied += bytesRead
        }
        bytesRead = inputStream.read(buffer)
      }

      DriverTcpConnectionLogger.debugSnapshot(
        "group_scan_tcp_snapshot",
        "phase" -> "review_snapshot_stage_complete",
        "file" -> sourcePath.toString,
        "file_identifier" -> logicalIdentifier,
        "bytes_copied" -> bytesCopied,
        "duration_ms" -> elapsedMs(startNanos)
      )
      Right(StagedFileSnapshot(
        stagedRoot = stagingRoot.toString,
        stagedPath = stagedPath.toString,
        recordedFingerprint = RecordedFileFingerprint(
          fileIdentifier = logicalIdentifier,
          fileSize = expectedFileSize,
          fileMtimeEpochMs = expectedFileMtimeEpochMs,
          fileChecksumAlgo = FileIdentifierResolver.DefaultChecksumAlgo,
          fileChecksum = f"${crc32.getValue}%08x"
        )
      ))
    } catch {
      case NonFatal(e) =>
        DriverTcpConnectionLogger.debugSnapshot(
          "group_scan_tcp_snapshot",
          "phase" -> "review_snapshot_stage_error",
          "file" -> sourcePath.toString,
          "file_identifier" -> logicalIdentifier,
          "bytes_copied" -> bytesCopied,
          "duration_ms" -> elapsedMs(startNanos),
          "reason" -> Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        )
        deleteStagingPath(conf, stagingRoot.toString)
        Left(s"Scan staging failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}")
    } finally {
      if (outputStream != null) {
        try outputStream.close() catch {
          case NonFatal(_) => ()
        }
      }
      if (inputStream != null) {
        try inputStream.close() catch {
          case NonFatal(_) => ()
        }
      }
    }
  }

  def captureRecordedFingerprint(
    conf: org.apache.hadoop.conf.Configuration,
    fileIdentifier: String,
    physicalPath: String,
    fileSize: Long,
    fileMtimeEpochMs: Long
  ): RecordedFileFingerprint = {
    val startNanos = System.nanoTime()
    DriverTcpConnectionLogger.debugSnapshot(
      "group_scan_tcp_snapshot",
      "phase" -> "recorded_fingerprint_start",
      "file" -> physicalPath,
      "file_identifier" -> fileIdentifier,
      "file_size" -> fileSize
    )
    try {
      val checksum = crc32Hex(conf, physicalPath)
      DriverTcpConnectionLogger.debugSnapshot(
        "group_scan_tcp_snapshot",
        "phase" -> "recorded_fingerprint_complete",
        "file" -> physicalPath,
        "file_identifier" -> fileIdentifier,
        "file_size" -> fileSize,
        "duration_ms" -> elapsedMs(startNanos)
      )
      recordedFingerprint(fileIdentifier, fileSize, fileMtimeEpochMs, checksum)
    } catch {
      case NonFatal(e) =>
        DriverTcpConnectionLogger.debugSnapshot(
          "group_scan_tcp_snapshot",
          "phase" -> "recorded_fingerprint_error",
          "file" -> physicalPath,
          "file_identifier" -> fileIdentifier,
          "file_size" -> fileSize,
          "duration_ms" -> elapsedMs(startNanos),
          "reason" -> Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        )
        throw e
    }
  }

  def recordedFingerprint(
    fileIdentifier: String,
    fileSize: Long,
    fileMtimeEpochMs: Long,
    checksum: String
  ): RecordedFileFingerprint = {
    RecordedFileFingerprint(
      fileIdentifier = fileIdentifier,
      fileSize = fileSize,
      fileMtimeEpochMs = fileMtimeEpochMs,
      fileChecksumAlgo = FileIdentifierResolver.DefaultChecksumAlgo,
      fileChecksum = checksum
    )
  }

  def parseReviewScopeIdentifiers(rawValue: String): Seq[String] = {
    ReviewScopeIdentifierCodec.decode(rawValue) match {
      case Right(identifiers) =>
        identifiers
      case Left(errorMessage) =>
        throw new IllegalArgumentException(errorMessage)
    }
  }

  def encodeRecordedFingerprint(recordedFingerprint: Option[RecordedFileFingerprint]): String =
    recordedFingerprint.map(fingerprint => ReviewScopeFingerprintCodec.encode(Seq(fingerprint))).getOrElse("")

  def logReviewSnapshotStart(
    mode: String,
    matchedFiles: Int,
    selectedFiles: Int
  ): Unit = {
    DriverLogger.debug(
      "group_scan_review_snapshot_start",
      "mode" -> mode,
      "matched_files" -> matchedFiles,
      "selected_files" -> selectedFiles
    )
  }

  def logReviewSnapshotFile(
    mode: String,
    physicalPath: String,
    fileIdentifier: String
  ): Unit = {
    DriverLogger.debug(
      "group_scan_review_snapshot_file",
      "mode" -> mode,
      "file" -> physicalPath,
      "file_identifier" -> fileIdentifier
    )
  }

  def logReviewSnapshotSkipped(
    mode: String,
    matchedFiles: Int,
    selectedFiles: Int,
    physicalPath: Option[String] = None,
    fileIdentifier: Option[String] = None
  ): Unit = {
    val baseFields = Seq(
      "mode" -> mode,
      "matched_files" -> matchedFiles,
      "selected_files" -> selectedFiles
    )
    val optionalFields = Seq(
      physicalPath.map("file" -> _),
      fileIdentifier.map("file_identifier" -> _)
    ).flatten
    DriverLogger.debug("group_scan_review_snapshot_skipped", (baseFields ++ optionalFields): _*)
  }

  private def crc32Hex(
    conf: org.apache.hadoop.conf.Configuration,
    physicalPath: String
  ): String = {
    val sourcePath = new Path(physicalPath)
    val fs = sourcePath.getFileSystem(conf)
    var inputStream: InputStream = null

    try {
      inputStream = fs.open(sourcePath)
      val crc32 = new CRC32()
      val buffer = new Array[Byte](CopyBufferSize)
      var bytesRead = inputStream.read(buffer)
      while (bytesRead >= 0) {
        if (bytesRead > 0) {
          crc32.update(buffer, 0, bytesRead)
        }
        bytesRead = inputStream.read(buffer)
      }
      f"${crc32.getValue}%08x"
    } finally {
      if (inputStream != null) {
        try inputStream.close() catch {
          case NonFatal(_) => ()
        }
      }
    }
  }

  private def elapsedMs(startNanos: Long): Long =
    (System.nanoTime() - startNanos) / 1000000L
}
