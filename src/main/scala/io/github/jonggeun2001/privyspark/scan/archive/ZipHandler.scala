package io.github.jonggeun2001.privyspark.scan.archive

import io.github.jonggeun2001.privyspark.model.ScanError
import org.apache.commons.compress.archivers.zip.{ZipArchiveEntry, ZipArchiveInputStream}
import org.apache.hadoop.fs.Path

import java.io.InputStream
import scala.collection.mutable.ArrayBuffer

private[privyspark] object ZipHandler {
  def open(
    conf: org.apache.hadoop.conf.Configuration,
    archivePath: String,
    archiveErrors: ArrayBuffer[ScanError],
    datasetPath: String,
    timestamp: String,
    logicalIdentifier: String
  ): Either[String, ArchiveEntryHandler] = {
    val sourcePath = new Path(archivePath)
    val fs = sourcePath.getFileSystem(conf)

    if (ArchiveIOUtil.hasEncryptedZipEntry(fs, sourcePath, archiveErrors, datasetPath, timestamp, logicalIdentifier)) {
      Left(s"Password-protected archive is not supported: $logicalIdentifier")
    } else {
      val rawInputStream = fs.open(sourcePath)
      val zipInputStream = new ZipArchiveInputStream(rawInputStream)

      Right(new ArchiveEntryHandler {
        private var currentEntry: ZipArchiveEntry = null

        override def nextEntry(): Boolean = {
          currentEntry = zipInputStream.getNextZipEntry
          while (currentEntry != null && !currentEntry.isDirectory && !zipInputStream.canReadEntryData(currentEntry)) {
            val childLogicalIdentifier = ArchiveIOUtil.archiveEntryLogicalIdentifier(logicalIdentifier, currentEntry.getName)
            archiveErrors += ScanError(
              datasetPath,
              timestamp,
              childLogicalIdentifier,
              s"Archive read failed: Unsupported ZIP feature: $childLogicalIdentifier"
            )
            currentEntry = zipInputStream.getNextZipEntry
          }
          currentEntry != null
        }

        override def entryName: String = currentEntry.getName

        override def entrySize: Long = currentEntry.getSize

        override def isDirectory: Boolean = currentEntry.isDirectory

        override def withEntryInputStream(process: InputStream => Unit): Unit = {
          process(zipInputStream)
        }

        override def close(): Unit = {
          ArchiveIOUtil.closeArchiveResources(
            Seq(zipInputStream, rawInputStream),
            archiveErrors,
            datasetPath,
            timestamp,
            logicalIdentifier
          )
        }
      })
    }
  }
}
