package io.github.jonggeun2001.privyspark.scan.archive

import io.github.jonggeun2001.privyspark.model.ScanError
import org.apache.commons.compress.PasswordRequiredException
import org.apache.commons.compress.archivers.sevenz.{SevenZArchiveEntry, SevenZFile}

import java.io.InputStream
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

private[privyspark] object SevenZHandler {
  def open(
    localArchivePath: java.nio.file.Path,
    archiveErrors: ArrayBuffer[ScanError],
    datasetPath: String,
    timestamp: String,
    logicalIdentifier: String
  ): Either[String, ArchiveEntryHandler] = {
    if (ArchiveIOUtil.isPasswordProtectedSevenZArchive(localArchivePath)) {
      Left(s"Password-protected archive is not supported: $logicalIdentifier")
    } else {
      try {
        val archiveFile = SevenZFile.builder().setFile(localArchivePath.toFile).get()
        Right(new ArchiveEntryHandler {
          private var currentEntry: SevenZArchiveEntry = null

          override def nextEntry(): Boolean = {
            currentEntry = archiveFile.getNextEntry
            currentEntry != null
          }

          override def entryName: String = currentEntry.getName

          override def entrySize: Long = currentEntry.getSize

          override def isDirectory: Boolean = currentEntry.isDirectory

          override def withEntryInputStream(process: InputStream => Unit): Unit = {
            ArchiveIOUtil.withArchiveEntryInputStream(
              entryName,
              logicalIdentifier,
              archiveErrors,
              datasetPath,
              timestamp
            )(archiveFile.getInputStream(currentEntry))(process)
          }

          override def close(): Unit = {
            if (archiveFile != null) {
              archiveFile.close()
            }
          }
        })
      } catch {
        case _: PasswordRequiredException =>
          Left(s"Password-protected archive is not supported: $logicalIdentifier")
        case NonFatal(e) =>
          Left(s"Archive read failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}")
      }
    }
  }
}
