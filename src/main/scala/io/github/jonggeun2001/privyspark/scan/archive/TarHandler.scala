package io.github.jonggeun2001.privyspark.scan.archive

import io.github.jonggeun2001.privyspark.format.CompressionStreams
import io.github.jonggeun2001.privyspark.format.FormatDetector
import io.github.jonggeun2001.privyspark.model.ScanError
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}
import org.apache.hadoop.fs.Path

import java.io.InputStream
import scala.collection.mutable.ArrayBuffer

private[privyspark] object TarHandler {
  def open(
    conf: org.apache.hadoop.conf.Configuration,
    archivePath: String,
    archiveErrors: ArrayBuffer[ScanError],
    datasetPath: String,
    timestamp: String,
    logicalIdentifier: String
  ): ArchiveEntryHandler = {
    val sourcePath = new Path(archivePath)
    val fs = sourcePath.getFileSystem(conf)
    val rawInputStream = fs.open(sourcePath)
    val archiveInputStream = CompressionStreams.wrapInputStream(
      rawInputStream,
      FormatDetector.detect(archivePath).flatMap(_.codec)
    )
    val tarInputStream = new TarArchiveInputStream(archiveInputStream)

    new ArchiveEntryHandler {
      private var currentEntry: TarArchiveEntry = null

      override def nextEntry(): Boolean = {
        currentEntry = tarInputStream.getNextEntry
        currentEntry != null
      }

      override def entryName: String = currentEntry.getName

      override def entrySize: Long = currentEntry.getSize

      override def isDirectory: Boolean = currentEntry.isDirectory

      override def withEntryInputStream(process: InputStream => Unit): Unit = {
        process(tarInputStream)
      }

      override def close(): Unit = {
        ArchiveIOUtil.closeArchiveResources(
          Seq(tarInputStream, archiveInputStream, rawInputStream),
          archiveErrors,
          datasetPath,
          timestamp,
          logicalIdentifier
        )
      }
    }
  }
}
