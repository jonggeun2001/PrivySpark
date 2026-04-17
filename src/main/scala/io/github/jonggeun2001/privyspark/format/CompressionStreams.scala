package io.github.jonggeun2001.privyspark.format

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream
import org.apache.hadoop.fs.Path

import java.io.{BufferedInputStream, InputStream}
import scala.util.control.NonFatal

private[privyspark] object CompressionStreams {
  def wrapInputStream(rawInputStream: InputStream, codec: Option[String]): InputStream = {
    codec match {
      case Some("gz") => new GzipCompressorInputStream(new BufferedInputStream(rawInputStream), true)
      case Some("bz2") => new BZip2CompressorInputStream(new BufferedInputStream(rawInputStream), true)
      case Some("xz") => new XZCompressorInputStream(new BufferedInputStream(rawInputStream), true)
      case Some("zst") => new ZstdCompressorInputStream(new BufferedInputStream(rawInputStream))
      case None => rawInputStream
      case Some(other) => throw new IllegalArgumentException(s"Unsupported compression codec: $other")
    }
  }

  def openDirectInputStream(
    conf: org.apache.hadoop.conf.Configuration,
    filePath: String
  ): InputStream = {
    val sourcePath = new Path(filePath)
    val fs = sourcePath.getFileSystem(conf)
    val rawInputStream = fs.open(sourcePath)
    try {
      wrapInputStream(
        rawInputStream,
        FormatDetector.detect(filePath).filterNot(_.isArchive).flatMap(_.codec)
      )
    } catch {
      case NonFatal(e) =>
        rawInputStream.close()
        throw e
    }
  }
}
