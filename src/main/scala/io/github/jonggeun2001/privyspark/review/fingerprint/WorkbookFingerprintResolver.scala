package io.github.jonggeun2001.privyspark.review.fingerprint

import io.github.jonggeun2001.privyspark.format.ByteProbe.detectPhysicalFormat
import io.github.jonggeun2001.privyspark.format.CsvInference.XlsxFormat
import io.github.jonggeun2001.privyspark.format.WorkbookHelpers.listVisibleWorkbookSheets
import io.github.jonggeun2001.privyspark.review.ResolvedFileFingerprint
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

private[review] object WorkbookFingerprintResolver {
  def parseIdentifier(
    conf: Configuration,
    inputRoot: String,
    fileIdentifier: String
  ): Option[(String, String)] = {
    val separatorIndex = Option(fileIdentifier).getOrElse("").lastIndexOf('#')
    if (separatorIndex <= 0 || separatorIndex + 1 >= fileIdentifier.length) {
      None
    } else {
      val workbookIdentifier = fileIdentifier.substring(0, separatorIndex)
      val workbookPath = PathFingerprintResolver.resolveInputPath(conf, inputRoot, workbookIdentifier)
      val path = new Path(workbookPath)
      val fs = path.getFileSystem(conf)

      if (fs.exists(path) && detectPhysicalFormat(conf, workbookPath).contains(XlsxFormat)) {
        Some(workbookIdentifier -> fileIdentifier.substring(separatorIndex + 1))
      } else {
        None
      }
    }
  }

  def resolve(
    conf: Configuration,
    inputRoot: String,
    workbookIdentifier: String,
    sheetName: String,
    originalIdentifier: String
  ): Either[String, ResolvedFileFingerprint] = {
    val workbookPath = PathFingerprintResolver.resolveInputPath(conf, inputRoot, workbookIdentifier)
    val path = new Path(workbookPath)
    val fs = path.getFileSystem(conf)

    if (!fs.exists(path)) {
      Left(s"Workbook path not found: $originalIdentifier")
    } else {
      listVisibleWorkbookSheets(conf, workbookPath) match {
        case Right(sheetNames) if sheetNames.contains(sheetName) =>
          val status = fs.getFileStatus(path)
          Crc32Stream.crc32ForFile(conf, workbookPath).map { checksum =>
            ResolvedFileFingerprint(
              fileIdentifier = originalIdentifier,
              physicalPath = workbookPath,
              fileSize = status.getLen,
              fileMtimeEpochMs = status.getModificationTime,
              fileChecksumAlgo = Crc32Stream.DefaultChecksumAlgo,
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
}
