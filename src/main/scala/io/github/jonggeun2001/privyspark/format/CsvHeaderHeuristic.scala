package io.github.jonggeun2001.privyspark.format

import io.github.jonggeun2001.privyspark.fsio.RetryIO
import io.github.jonggeun2001.privyspark.scan.CsvHeadCache
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.execution.datasources.csv.CSVUtils

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets

import com.univocity.parsers.csv.CsvParser
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

private[privyspark] object CsvHeaderHeuristic {
  private val CommonCsvHeaderTokens = Set(
    "id",
    "name",
    "first",
    "last",
    "full",
    "maker",
    "model",
    "email",
    "mail",
    "phone",
    "tel",
    "mobile",
    "city",
    "state",
    "country",
    "이름",
    "이메일",
    "도시",
    "국가",
    "주소",
    "전화번호",
    "address",
    "addr",
    "zip",
    "postal",
    "code",
    "user",
    "account",
    "customer",
    "created",
    "updated",
    "timestamp",
    "date",
    "time",
    "age",
    "gender",
    "status",
    "type",
    "amount",
    "price",
    "count",
    "number",
    "value",
    "description",
    "product",
    "item"
  )

  def parseHeaderFields(line: String): Seq[String] = {
    val sanitizedLine = Option(line).getOrElse("").stripPrefix("\uFEFF")
    val fields = ArrayBuffer.empty[String]
    val buffer = new java.lang.StringBuilder
    var index = 0
    var inQuotes = false

    while (index < sanitizedLine.length) {
      val ch = sanitizedLine.charAt(index)
      if (ch == '"') {
        val nextIsEscapedQuote = inQuotes && index + 1 < sanitizedLine.length && sanitizedLine.charAt(index + 1) == '"'
        if (nextIsEscapedQuote) {
          buffer.append('"')
          index += 1
        } else {
          inQuotes = !inQuotes
        }
      } else if (ch == ',' && !inQuotes) {
        fields += buffer.toString()
        buffer.setLength(0)
      } else {
        buffer.append(ch)
      }
      index += 1
    }

    fields += buffer.toString()
    fields.toSeq
  }

  def inferCsvHeaderSignature(
    spark: SparkSession,
    filePath: String,
    csvHeadCache: CsvHeadCache = new CsvHeadCache()
  ): Either[String, String] = {
    try {
      val lines = readFirstNonBlankCsvLines(spark, filePath, maxLines = CsvHeadCache.CachedLineLimit, csvHeadCache)
      Right(inferCsvHeaderSignatureFromLines(spark, lines))
    } catch {
      case NonFatal(e) =>
        Left(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
    }
  }

  private def createCsvOptions(spark: SparkSession): CSVOptions = {
    new CSVOptions(
      scala.collection.immutable.Map("header" -> "true", "inferSchema" -> "false"),
      false,
      spark.sessionState.conf.sessionLocalTimeZone,
      spark.sessionState.conf.columnNameOfCorruptRecord
    )
  }

  private[privyspark] def parseCsvLine(spark: SparkSession, line: String): Array[String] = {
    val parser = new CsvParser(createCsvOptions(spark).asParserSettings)
    Option(parser.parseLine(Option(line).getOrElse("").stripPrefix("\uFEFF"))).getOrElse(Array.empty[String])
  }

  private def loadFirstNonBlankCsvLines(
    spark: SparkSession,
    filePath: String,
    maxLines: Int
  ): Seq[String] = {
    RetryIO.withFileReadRetry(spark, Seq(filePath), "csv_line_sample") {
      val path = new Path(filePath)
      val reader = new BufferedReader(new InputStreamReader(
        CompressionStreams.openDirectInputStream(spark.sparkContext.hadoopConfiguration, path.toString),
        StandardCharsets.UTF_8
      ))
      try {
        val lines = ArrayBuffer.empty[String]
        var line: String = reader.readLine()
        while (line != null && lines.size < maxLines) {
          if (line.trim.nonEmpty) {
            lines += line
          }
          line = reader.readLine()
        }
        lines.toSeq
      } finally {
        reader.close()
      }
    }
  }

  private[privyspark] def readFirstNonBlankCsvLines(
    spark: SparkSession,
    filePath: String,
    maxLines: Int,
    csvHeadCache: CsvHeadCache = new CsvHeadCache()
  ): Seq[String] = {
    if (maxLines <= CsvHeadCache.CachedLineLimit) {
      csvHeadCache.getOrRead(filePath) {
        loadFirstNonBlankCsvLines(spark, filePath, CsvHeadCache.CachedLineLimit)
      }.take(maxLines)
    } else {
      loadFirstNonBlankCsvLines(spark, filePath, maxLines)
    }
  }

  private[privyspark] def inferCsvHeaderSignatureFromLines(
    spark: SparkSession,
    lines: Seq[String]
  ): String = {
    val csvOptions = createCsvOptions(spark)
    val headerLine = lines.headOption.getOrElse(throw new IllegalArgumentException("Empty or missing CSV header"))
    val headerColumns = CSVUtils.makeSafeHeader(
      parseCsvLine(spark, headerLine),
      spark.sessionState.conf.caseSensitiveAnalysis,
      csvOptions
    )
    headerColumns.map(_.toLowerCase).mkString("|")
  }

  private def isNumericLikeField(value: String): Boolean = {
    val trimmed = Option(value).getOrElse("").trim
    trimmed.nonEmpty && trimmed.matches("[-+]?\\d+(\\.\\d+)?")
  }

  private def classifyCsvField(value: String): String = {
    val trimmed = Option(value).getOrElse("").trim
    if (trimmed.isEmpty) {
      "empty"
    } else if (trimmed.matches("[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}")) {
      "email"
    } else if (trimmed.matches("\\d{2,3}-\\d{3,4}-\\d{4}")) {
      "phone"
    } else if (isNumericLikeField(trimmed)) {
      "numeric"
    } else if (trimmed.exists(_.isDigit)) {
      "mixed"
    } else {
      "plain_text"
    }
  }

  private def isStructuredCsvFieldKind(kind: String): Boolean = {
    kind match {
      case "email" | "phone" | "numeric" | "mixed" => true
      case _ => false
    }
  }

  private def isStructuredCsvFieldForHeaderHeuristic(value: String, kind: String): Boolean = {
    if (kind == "mixed" && looksLikeCsvHeaderField(value)) {
      false
    } else {
      isStructuredCsvFieldKind(kind)
    }
  }

  private def tokenizeCsvHeaderField(value: String): Seq[String] = {
    Option(value).getOrElse("").trim.toLowerCase
      .split("[\\s_./-]+")
      .filter(_.nonEmpty)
      .flatMap { token =>
        val normalizedToken = token.replaceAll("\\d+$", "")
        if (normalizedToken.nonEmpty && normalizedToken != token) Seq(token, normalizedToken) else Seq(token)
      }
      .toSeq
  }

  private def looksLikeCsvHeaderField(value: String): Boolean = {
    val trimmed = Option(value).getOrElse("").trim
    trimmed.nonEmpty &&
      !trimmed.contains("@") &&
      isCsvHeaderFieldShape(trimmed)
  }

  private def hasStrongCsvHeaderSignal(value: String): Boolean = {
    val trimmed = Option(value).getOrElse("").trim
    val tokens = tokenizeCsvHeaderField(trimmed)
    tokens.exists(CommonCsvHeaderTokens.contains) ||
      trimmed.exists(ch => ch == '_' || ch == '-' || ch == ' ')
  }

  private def scoreCsvHeaderField(value: String): Int = {
    val trimmed = Option(value).getOrElse("").trim
    val fieldKind = classifyCsvField(trimmed)
    if (trimmed.isEmpty) {
      -2
    } else if (fieldKind != "plain_text" && !(fieldKind == "mixed" && looksLikeCsvHeaderField(trimmed))) {
      -2
    } else {
      val tokens = tokenizeCsvHeaderField(trimmed)
      val commonTokenScore = tokens.count(CommonCsvHeaderTokens.contains) * 2
      val separatorScore = if (trimmed.exists(ch => ch == '_' || ch == '-' || ch == ' ')) 1 else 0
      val lowercaseWordScore =
        if (trimmed.nonEmpty && trimmed.forall(isCsvHeaderLowercaseLikeChar)) 1
        else 0
      val alphaOnlyScore = if (isCsvHeaderFieldShape(trimmed)) 1 else 0
      commonTokenScore + separatorScore + lowercaseWordScore + alphaOnlyScore
    }
  }

  private def scoreCsvHeaderRow(fields: Seq[String]): Int = {
    fields.map(scoreCsvHeaderField).sum
  }

  private def looksLikeCsvHeaderRow(fields: Seq[String]): Boolean = {
    fields.nonEmpty &&
      fields.forall(looksLikeCsvHeaderField)
  }

  private def isCsvHeaderFieldShape(value: String): Boolean = {
    value.nonEmpty &&
      Character.isLetter(value.charAt(0)) &&
      value.forall(isCsvHeaderFieldChar)
  }

  private def isCsvHeaderFieldChar(ch: Char): Boolean = {
    Character.isLetterOrDigit(ch) || ch == '_' || ch == ' ' || ch == '.' || ch == '/' || ch == '-'
  }

  private def isCsvHeaderLowercaseLikeChar(ch: Char): Boolean = {
    ch.isWhitespace || ch == '_' || ch == '-' || ch == '.' || ch == '/' || !Character.isUpperCase(ch)
  }

  private[privyspark] def detectCsvHasHeaderFromLines(
    spark: SparkSession,
    lines: Seq[String]
  ): Boolean = {
    val firstRowFields = lines.headOption.map(parseCsvLine(spark, _).toSeq).getOrElse(Seq.empty)
    if (firstRowFields.isEmpty) {
      false
    } else {
      val normalizedFields = firstRowFields.map(field => Option(field).getOrElse("").trim.toLowerCase)
      val hasDuplicateFields = normalizedFields.nonEmpty && normalizedFields.distinct.size != normalizedFields.size
      val allNumericFields = firstRowFields.nonEmpty && firstRowFields.forall(isNumericLikeField)
      val firstRowFieldKinds = firstRowFields.map(classifyCsvField)
      val firstRowHasStructuredData = firstRowFields.zip(firstRowFieldKinds).exists {
        case (field, kind) => isStructuredCsvFieldForHeaderHeuristic(field, kind)
      }
      if (hasDuplicateFields || allNumericFields || firstRowHasStructuredData || !looksLikeCsvHeaderRow(firstRowFields)) {
        return false
      }

      if (lines.size <= 1) {
        return firstRowFields.exists(hasStrongCsvHeaderSignal)
      }

      val secondRowFields = parseCsvLine(spark, lines(1)).toSeq
      if (firstRowFields.size != secondRowFields.size) {
        return true
      }

      val secondRowFieldKinds = secondRowFields.map(classifyCsvField)
      if (secondRowFieldKinds.exists(isStructuredCsvFieldKind)) {
        return true
      }

      val firstHeaderScore = scoreCsvHeaderRow(firstRowFields)
      val secondHeaderScore = scoreCsvHeaderRow(secondRowFields)
      secondHeaderScore <= firstHeaderScore
    }
  }
}
