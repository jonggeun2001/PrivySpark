package io.github.jonggeun2001.privyspark.scan.archive

import com.github.junrar.Archive
import com.github.junrar.exception.UnsupportedRarV5Exception
import com.github.junrar.rarfile.FileHeader
import io.github.jonggeun2001.privyspark.model.ScanError

import java.io.InputStream
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

private[privyspark] object RarHandler {
  def open(
    localArchivePath: java.nio.file.Path,
    archiveErrors: ArrayBuffer[ScanError],
    datasetPath: String,
    timestamp: String,
    logicalIdentifier: String
  ): Either[String, ArchiveEntryHandler] = {
    try {
      val archive = new Archive(localArchivePath.toFile)
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
        archive.close()
        Left(s"Password-protected archive is not supported: $logicalIdentifier")
      } else if (multiVolume) {
        archive.close()
        Left(s"Multi-volume archive is not supported: $logicalIdentifier")
      } else {
        Right(new ArchiveEntryHandler {
          private val fileHeaderIterator = fileHeaders.iterator.filterNot(_.isDirectory)
          private var currentHeader: FileHeader = null

          override def nextEntry(): Boolean = {
            if (fileHeaderIterator.hasNext) {
              currentHeader = fileHeaderIterator.next()
              true
            } else {
              currentHeader = null
              false
            }
          }

          override def entryName: String = currentHeader.getFileName

          override def entrySize: Long = currentHeader.getFullUnpackSize

          override def isDirectory: Boolean = false

          override def withEntryInputStream(process: InputStream => Unit): Unit = {
            ArchiveIOUtil.withArchiveEntryInputStream(
              entryName,
              logicalIdentifier,
              archiveErrors,
              datasetPath,
              timestamp
            )(archive.getInputStream(currentHeader))(process)
          }

          override def close(): Unit = {
            archive.close()
          }
        })
      }
    } catch {
      case _: UnsupportedRarV5Exception =>
        Left(s"RAR5 archives are not supported: $logicalIdentifier")
      case NonFatal(e) =>
        Left(s"Archive read failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}")
    }
  }
}
