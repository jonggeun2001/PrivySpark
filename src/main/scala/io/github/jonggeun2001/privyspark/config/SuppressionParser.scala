package io.github.jonggeun2001.privyspark.config

import io.github.jonggeun2001.privyspark.model.Suppression
import io.github.jonggeun2001.privyspark.util.DriverLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkEnv, SparkFiles}

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.mutable.ArrayBuffer

private[privyspark] object SuppressionParser {
  final case class ParsedSuppression(suppression: Suppression, source: String)

  def parseCliSuppressions(
    conf: Configuration,
    inline: Seq[String],
    file: Option[String]
  ): Seq[Suppression] = {
    parseCliSuppressionEntries(conf, inline, file).map(_.suppression)
  }

  def parseCliSuppressionEntries(
    conf: Configuration,
    inline: Seq[String],
    file: Option[String]
  ): Seq[ParsedSuppression] = {
    inline.zipWithIndex.map {
      case (rawValue, index) => parseSuppressionSpec(rawValue, s"cli:${index + 1}")
    } ++ file.toSeq.flatMap(loadSuppressionFile(conf, _))
  }

  def warnUnknownSuppressions(
    suppressions: Seq[ParsedSuppression],
    definedPiiTypes: Set[String]
  ): Unit = {
    suppressions.foreach { parsedSuppression =>
      if (!definedPiiTypes.contains(parsedSuppression.suppression.piiType)) {
        DriverLogger.warn(
          "ruleset_suppression_unknown_pii_type",
          "column" -> parsedSuppression.suppression.columnName,
          "pii_type" -> parsedSuppression.suppression.piiType,
          "suppression_source" -> parsedSuppression.source
        )
      }
    }
  }

  private def loadSuppressionFile(conf: Configuration, path: String): Seq[ParsedSuppression] = {
    val normalizedPath = Option(path).map(_.trim).getOrElse("")
    if (normalizedPath.isEmpty) {
      throw new IllegalArgumentException("suppression-file must not be blank")
    } else {
      resolveLocalSuppressionFile(normalizedPath) match {
        case Some(localPath) =>
          val reader = Files.newBufferedReader(localPath, StandardCharsets.UTF_8)
          readSuppressions(reader, s"file:$normalizedPath")
        case None =>
          val hadoopPath = new Path(normalizedPath)
          val fs = hadoopPath.getFileSystem(conf)
          val reader = new BufferedReader(new InputStreamReader(fs.open(hadoopPath), StandardCharsets.UTF_8))
          readSuppressions(reader, s"file:$normalizedPath")
      }
    }
  }

  private def resolveLocalSuppressionFile(path: String): Option[java.nio.file.Path] = {
    val hadoopPath = new Path(path)
    val uri = hadoopPath.toUri

    if (uri.getScheme != null || uri.getAuthority != null) {
      None
    } else {
      val sparkFilesCandidate = Option(SparkEnv.get).map(_ => Paths.get(SparkFiles.get(path)))
      val workingDirectoryCandidate = Paths.get(path)

      Seq(sparkFilesCandidate, Some(workingDirectoryCandidate)).flatten.collectFirst {
        case candidate if Files.exists(candidate) => candidate.toAbsolutePath.normalize()
      }
    }
  }

  private def readSuppressions(reader: BufferedReader, source: String): Seq[ParsedSuppression] = {
    val suppressions = ArrayBuffer.empty[ParsedSuppression]

    try {
      var lineNumber = 1
      var line = reader.readLine()
      while (line != null) {
        normalizeSuppressionLine(line).foreach { spec =>
          suppressions += parseSuppressionSpec(spec, s"$source:$lineNumber")
        }
        line = reader.readLine()
        lineNumber += 1
      }
    } finally {
      reader.close()
    }

    suppressions.toSeq
  }

  private def normalizeSuppressionLine(rawValue: String): Option[String] = {
    val trimmed = Option(rawValue).map(_.trim).getOrElse("")
    if (trimmed.isEmpty || trimmed.startsWith("#")) None else Some(trimmed)
  }

  private def parseSuppressionSpec(rawValue: String, source: String): ParsedSuppression = {
    val trimmed = Option(rawValue).map(_.trim).getOrElse("")
    val delimiterIndex = trimmed.lastIndexOf(':')
    val columnName =
      if (delimiterIndex > 0 && delimiterIndex < trimmed.length - 1) trimmed.substring(0, delimiterIndex).trim else ""
    val piiType =
      if (delimiterIndex > 0 && delimiterIndex < trimmed.length - 1) trimmed.substring(delimiterIndex + 1).trim else ""

    if (columnName.isEmpty || piiType.isEmpty) {
      throw new IllegalArgumentException(s"Invalid suppression entry ($source): $rawValue")
    }

    ParsedSuppression(Suppression(columnName, piiType), source)
  }
}
