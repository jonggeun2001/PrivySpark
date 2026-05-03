package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.review.fingerprint.{
  ArchiveFingerprintResolver,
  Crc32Stream,
  PathFingerprintResolver,
  WorkbookFingerprintResolver
}
import org.apache.hadoop.conf.Configuration

object FileIdentifierResolver {
  val DefaultChecksumAlgo = Crc32Stream.DefaultChecksumAlgo

  def resolveFingerprints(
    conf: Configuration,
    inputRoot: String,
    fileIdentifier: String
  ): Either[String, Seq[ResolvedFileFingerprint]] = {
    ArchiveFingerprintResolver.parseIdentifier(fileIdentifier) match {
      case Some((archiveIdentifier, entryName)) =>
        ArchiveFingerprintResolver.resolve(conf, inputRoot, archiveIdentifier, entryName, fileIdentifier).map(Seq(_))
      case None =>
        WorkbookFingerprintResolver.parseIdentifier(conf, inputRoot, fileIdentifier) match {
          case Some((workbookIdentifier, sheetName)) =>
            WorkbookFingerprintResolver.resolve(conf, inputRoot, workbookIdentifier, sheetName, fileIdentifier).map(Seq(_))
          case None =>
            PathFingerprintResolver.resolve(conf, inputRoot, fileIdentifier)
        }
    }
  }
}
