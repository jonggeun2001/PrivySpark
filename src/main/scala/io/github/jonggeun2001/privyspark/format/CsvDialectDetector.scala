package io.github.jonggeun2001.privyspark.format

import io.github.jonggeun2001.privyspark.format.ByteProbe.TextFormat
import io.github.jonggeun2001.privyspark.format.CsvHeaderHeuristic.{parseCsvLine, readFirstNonBlankCsvLines}
import io.github.jonggeun2001.privyspark.model.{CsvDialect, ScanReadOptions}
import io.github.jonggeun2001.privyspark.scan.CsvHeadCache
import org.apache.spark.sql.SparkSession

import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import java.io.StringReader
import scala.collection.mutable
import scala.util.control.NonFatal

private[privyspark] object CsvDialectDetector {
  private val SingleCharacterCandidates = Seq(",", "\t", ";", "|", ":", "\u001f")
  private val CandidatePriority = SingleCharacterCandidates.zipWithIndex.toMap.withDefaultValue(SingleCharacterCandidates.size)
  private val MaxInconsistentLineRatio = 0.2

  private case class CandidateScore(
    dialect: CsvDialect,
    medianColumns: Int,
    inconsistentLines: Int,
    symbolOnlyFields: Int,
    priority: Int
  ) {
    val value: Int =
      medianColumns * 100 - inconsistentLines * 100 - symbolOnlyFields * 80 - dialect.delimiter.length
  }

  def detectDialect(
    spark: SparkSession,
    filePath: String,
    csvHeadCache: CsvHeadCache = new CsvHeadCache()
  ): Option[CsvDialect] = {
    val lines = readFirstNonBlankCsvLines(spark, filePath, CsvHeadCache.CachedLineLimit, csvHeadCache)
    detectDialectFromLines(spark, lines)
  }

  def detectDialectFromLines(spark: SparkSession, lines: Seq[String]): Option[CsvDialect] = {
    val sampleLines = lines.map(line => Option(line).getOrElse("")).filter(_.trim.nonEmpty)
    if (sampleLines.isEmpty) {
      None
    } else {
      (detectWithUnivocity(spark, sampleLines).toSeq ++ scoreCandidates(spark, sampleLines))
        .sortBy(score => (-score.value, score.priority, score.dialect.delimiter))
        .headOption
        .map(_.dialect)
    }
  }

  def refineDetectedFormat(
    spark: SparkSession,
    filePath: String,
    format: String,
    readOptions: ScanReadOptions,
    csvHeadCache: CsvHeadCache = new CsvHeadCache()
  ): (String, ScanReadOptions) = {
    if (readOptions.csvDialect.isDefined || readOptions.textEncoding.isDefined) {
      (format, readOptions)
    } else if (format == "csv") {
      val refinedReadOptions = detectDialect(spark, filePath, csvHeadCache)
        .filter(requiresExplicitReadOption)
        .map(dialect => readOptions.copy(csvDialect = Some(dialect)))
        .getOrElse(readOptions)
      (format, refinedReadOptions)
    } else if (format == TextFormat) {
      detectDialect(spark, filePath, csvHeadCache) match {
        case Some(dialect) =>
          val refinedReadOptions =
            if (dialect == CsvDialect()) readOptions else readOptions.copy(csvDialect = Some(dialect))
          ("csv", refinedReadOptions)
        case None =>
          (format, readOptions)
      }
    } else {
      (format, readOptions)
    }
  }

  private def requiresExplicitReadOption(dialect: CsvDialect): Boolean = {
    dialect.delimiter != CsvDialect().delimiter || dialect.quote != CsvDialect().quote
  }

  private def detectWithUnivocity(spark: SparkSession, lines: Seq[String]): Option[CandidateScore] = {
    try {
      val settings = new CsvParserSettings()
      settings.detectFormatAutomatically(SingleCharacterCandidates.map(_.charAt(0)): _*)
      settings.setFormatDetectorRowSampleCount(lines.size)
      val parser = new CsvParser(settings)
      parser.parseAll(new StringReader(lines.mkString("\n")))
      Option(parser.getDetectedFormat).flatMap { format =>
        val delimiter = Option(format.getDelimiterString).getOrElse(format.getDelimiter.toString)
        val dialect = CsvDialect(
          delimiter = delimiter,
          quote = format.getQuote,
          escape = format.getQuoteEscape
        )
        scoreDialect(spark, lines, dialect)
      }
    } catch {
      case NonFatal(_) => None
    }
  }

  private def scoreCandidates(spark: SparkSession, lines: Seq[String]): Seq[CandidateScore] = {
    val candidates = (SingleCharacterCandidates ++ discoverMultiCharacterCandidates(lines)).distinct
    candidates
      .flatMap(delimiter => scoreDialect(spark, lines, CsvDialect(delimiter = delimiter)))
      .sortBy(score => (-score.value, score.priority, score.dialect.delimiter))
  }

  private def scoreDialect(
    spark: SparkSession,
    lines: Seq[String],
    dialect: CsvDialect
  ): Option[CandidateScore] = {
    val parsedRows = try {
      lines.map(line => parseCsvLine(spark, line, dialect).toSeq)
    } catch {
      case NonFatal(_) => return None
    }
    val columnCounts = parsedRows.map(_.length)
    val candidateCounts = columnCounts.filter(_ >= 2)
    if (candidateCounts.isEmpty) {
      return None
    }

    val medianColumns = median(candidateCounts)
    val inconsistentLines = columnCounts.count(_ != medianColumns)
    val inconsistentRatio = inconsistentLines.toDouble / math.max(1, columnCounts.size).toDouble
    if (medianColumns < 2 || inconsistentRatio > MaxInconsistentLineRatio) {
      None
    } else {
      Some(CandidateScore(
        dialect = dialect,
        medianColumns = medianColumns,
        inconsistentLines = inconsistentLines,
        symbolOnlyFields = parsedRows.map(countSymbolOnlyFields).sum,
        priority = CandidatePriority(dialect.delimiter)
      ))
    }
  }

  private def median(values: Seq[Int]): Int = {
    val sorted = values.sorted
    sorted(sorted.size / 2)
  }

  private def countSymbolOnlyFields(fields: Seq[String]): Int = {
    fields.count { field =>
      val trimmed = Option(field).getOrElse("").trim
      trimmed.nonEmpty && trimmed.forall(ch => !Character.isLetterOrDigit(ch))
    }
  }

  private def discoverMultiCharacterCandidates(lines: Seq[String]): Seq[String] = {
    val countsByCandidate = mutable.Map.empty[String, Int].withDefaultValue(0)
    lines.foreach { line =>
      extractNonAlnumRuns(line).distinct.foreach { candidate =>
        countsByCandidate.update(candidate, countsByCandidate(candidate) + 1)
      }
    }
    val requiredLineCount = math.max(2, math.ceil(lines.size * 0.8).toInt)
    countsByCandidate.collect {
      case (candidate, lineCount) if lineCount >= requiredLineCount => candidate
    }.toSeq
  }

  private def extractNonAlnumRuns(line: String): Seq[String] = {
    val candidates = mutable.ArrayBuffer.empty[String]
    var index = 0
    while (index < line.length) {
      if (isDelimiterLike(line.charAt(index))) {
        val start = index
        while (index < line.length && isDelimiterLike(line.charAt(index))) {
          index += 1
        }
        val run = line.substring(start, index)
        if (run.length >= 2) {
          if (run.length <= 3) {
            candidates += run
          } else {
            2.to(3).foreach { length =>
              run.sliding(length).foreach(candidates += _)
            }
          }
        }
      } else {
        index += 1
      }
    }
    candidates.toSeq
  }

  private def isDelimiterLike(ch: Char): Boolean = {
    !Character.isLetterOrDigit(ch) &&
      !Character.isWhitespace(ch) &&
      ch != '"' &&
      ch != '\''
  }
}
