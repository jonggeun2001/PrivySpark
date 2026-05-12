package io.github.jonggeun2001.privyspark.format

import io.github.jonggeun2001.privyspark.format.ByteProbe.TextFormat
import io.github.jonggeun2001.privyspark.format.CsvHeaderHeuristic.{detectCsvHasHeaderFromLines, inferCsvHeaderSignatureFromLines, parseCsvLine, readFirstNonBlankCsvLines}
import io.github.jonggeun2001.privyspark.format.WorkbookHelpers.inferWorkbookSheetSchemaSignature
import io.github.jonggeun2001.privyspark.fsio.RetryIO
import io.github.jonggeun2001.privyspark.model.{CachedSchemaSignature, CsvDialect, ScanReadOptions}
import io.github.jonggeun2001.privyspark.scan.{CsvHeadCache, SchemaSignatureCache}
import io.github.jonggeun2001.privyspark.util.{DriverLogger, DriverTcpConnectionLogger}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.nio.charset.Charset
import java.util.UUID
import scala.util.control.NonFatal

private[privyspark] object CsvInference {
  val FileIdentifierColumn = "__privyspark_file_identifier"
  val XlsxFormat = "xlsx"
  val AvroFormat = "avro"
  private val TextSchemaSignature = "value"
  private val SparkGlobSpecialCharacters = Set('\\', '*', '?', '[', ']', '{', '}')

  def detectCsvHasHeader(
    spark: SparkSession,
    filePath: String,
    csvHeadCache: CsvHeadCache = new CsvHeadCache(),
    readOptions: ScanReadOptions = ScanReadOptions()
  ): Boolean = {
    val lines = readFirstNonBlankCsvLines(spark, filePath, maxLines = CsvHeadCache.CachedLineLimit, csvHeadCache)
    detectCsvHasHeaderFromLines(spark, lines, csvDialect(readOptions))
  }

  def inferCsvSchemaSignature(
    spark: SparkSession,
    filePath: String,
    csvHeadCache: CsvHeadCache = new CsvHeadCache(),
    schemaSigCache: SchemaSignatureCache = new SchemaSignatureCache(),
    readOptions: ScanReadOptions = ScanReadOptions()
  ): Either[String, (String, Boolean)] = {
    try {
      val dialect = csvDialect(readOptions)
      val cached = schemaSigCache.getOrCompute(filePath, "csv", readOptions) {
        val lines = readFirstNonBlankCsvLines(spark, filePath, maxLines = CsvHeadCache.CachedLineLimit, csvHeadCache)
        val csvHasHeader = detectCsvHasHeaderFromLines(spark, lines, dialect)
        if (csvHasHeader) {
          CachedSchemaSignature(inferCsvHeaderSignatureFromLines(spark, lines, dialect), csvHasHeader = true)
        } else {
          val firstDataLine = lines.headOption
            .getOrElse(throw new IllegalArgumentException("Empty CSV file"))
          val columnCount = parseCsvLine(spark, firstDataLine, dialect).length
          CachedSchemaSignature(s"cols:$columnCount", csvHasHeader = false)
        }
      }
      Right((cached.signature, cached.csvHasHeader))
    } catch {
      case NonFatal(e) =>
        Left(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
    }
  }

  def inferSchemaSignature(
    spark: SparkSession,
    format: String,
    filePath: String,
    readOptions: ScanReadOptions = ScanReadOptions(),
    schemaSigCache: SchemaSignatureCache = new SchemaSignatureCache()
  ): Either[String, String] = {
    if (format == TextFormat) {
      Right(TextSchemaSignature)
    } else {
      try {
        val cached = schemaSigCache.getOrCompute(filePath, format, readOptions) {
          val schemaSignature = RetryIO.withFileReadRetry(spark, Seq(filePath), "schema_detection") {
            if (format == XlsxFormat) {
              val sheetName = readOptions.sheetName.getOrElse {
                throw new IllegalArgumentException("Sheet name is required for xlsx sources")
              }
              inferWorkbookSheetSchemaSignature(spark.sparkContext.hadoopConfiguration, filePath, sheetName) match {
                case Right(signature) => signature
                case Left(errorMessage) => throw new IllegalArgumentException(errorMessage)
              }
            } else {
              val schema = readSchemaSource(spark, format, filePath, readOptions = readOptions).schema
              val normalizedFieldNames = schema.fieldNames.map(_.toLowerCase)
              if (format == "csv") {
                normalizedFieldNames.mkString("|")
              } else {
                normalizedFieldNames.sorted.mkString("|")
              }
            }
          }
          CachedSchemaSignature(schemaSignature, csvHasHeader = true)
        }
        Right(cached.signature)
      } catch {
        case NonFatal(e) =>
          Left(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
      }
    }
  }

  def resolveFileIdentifierColumn(columns: Seq[String]): String = {
    val normalized = columns.map(_.toLowerCase).toSet
    var candidate = FileIdentifierColumn
    var index = 1

    while (normalized.contains(candidate.toLowerCase)) {
      candidate = s"${FileIdentifierColumn}_$index"
      index += 1
    }

    candidate
  }

  def newJsonCorruptRecordColumnName(): String = {
    s"${FileIdentifierColumn}_json_corrupt_${UUID.randomUUID().toString.replace("-", "")}"
  }

  def ensureReadableSourceColumns(
    format: String,
    sourcePaths: Seq[String],
    df: DataFrame,
    internalCorruptRecordColumnName: Option[String] = None
  ): DataFrame = {
    val normalizedFormat = Option(format).map(_.toLowerCase).getOrElse("")
    if (normalizedFormat == "json") {
      val schemaFieldNames = df.schema.fieldNames.toSeq
      internalCorruptRecordColumnName match {
        case Some(columnName) if schemaFieldNames.size == 1 && schemaFieldNames.head == columnName =>
          val sourceDescription = sourcePaths match {
            case Seq(singlePath) => singlePath
            case paths => s"${paths.size} files (first: ${paths.head})"
          }
          throw new IllegalArgumentException(
            s"Malformed json input contains only corrupt records: $sourceDescription"
          )
        case Some(columnName) if schemaFieldNames.contains(columnName) =>
          df.drop(columnName)
        case _ =>
          df
      }
    } else {
      df
    }
  }

  private def csvDialect(readOptions: ScanReadOptions): CsvDialect =
    readOptions.csvDialect.getOrElse(CsvDialect())

  private[privyspark] def literalSparkPath(path: String): String = {
    val raw = Option(path).getOrElse("")
    val builder = new StringBuilder(raw.length)
    raw.foreach { char =>
      if (SparkGlobSpecialCharacters.contains(char)) {
        builder.append('\\')
      }
      builder.append(char)
    }
    builder.toString()
  }

  private def literalSparkPaths(paths: Seq[String]): Seq[String] =
    paths.map(literalSparkPath)

  private def csvReader(
    spark: SparkSession,
    csvHasHeader: Boolean,
    readOptions: ScanReadOptions
  ): org.apache.spark.sql.DataFrameReader = {
    val dialect = csvDialect(readOptions)
    spark.read
      .option("header", csvHasHeader.toString)
      .option("sep", dialect.delimiter)
      .option("quote", dialect.quote.toString)
      .option("escape", dialect.escape.toString)
      .option("inferSchema", "false")
      .option("mode", "PERMISSIVE")
  }

  def readSchemaSource(
    spark: SparkSession,
    format: String,
    filePath: String,
    csvHasHeader: Boolean = true,
    readOptions: ScanReadOptions = ScanReadOptions()
  ): DataFrame = {
    DriverLogger.debug("read_schema_source_start", "format" -> format, "file" -> filePath)
    logReadSchemaSourceTcpSnapshot("read_schema_source_start", format, filePath)
    try {
      val literalFilePath = literalSparkPath(filePath)
      val (df, internalCorruptRecordColumnName) = format match {
        case "csv" =>
          (
            csvReader(spark, csvHasHeader, readOptions).csv(literalFilePath),
            None
          )
        case "json" =>
          val corruptRecordColumnName = newJsonCorruptRecordColumnName()
          (
            spark.read
              .option("mode", "PERMISSIVE")
              .option("columnNameOfCorruptRecord", corruptRecordColumnName)
              .json(literalFilePath),
            Some(corruptRecordColumnName)
          )
        case AvroFormat =>
          (spark.read.format("avro").load(literalFilePath), None)
        case XlsxFormat =>
          (
            readXlsx(spark, filePath, readOptions),
            None
          )
        case TextFormat =>
          (readTextSource(spark, Seq(filePath), readOptions), None)
        case "parquet" =>
          (spark.read.parquet(literalFilePath), None)
        case "orc" =>
          (spark.read.orc(literalFilePath), None)
        case _ =>
          throw new IllegalArgumentException(s"Unsupported format: $format")
      }
      val readableDf = ensureReadableSourceColumns(format, Seq(filePath), df, internalCorruptRecordColumnName)
      logReadSchemaSourceTcpSnapshot("read_schema_source_complete", format, filePath)
      readableDf
    } catch {
      case NonFatal(e) =>
        logReadSchemaSourceTcpSnapshot(
          "read_schema_source_error",
          format,
          filePath,
          "reason" -> Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        )
        throw e
    }
  }

  private def logReadSchemaSourceTcpSnapshot(
    phase: String,
    format: String,
    filePath: String,
    fields: (String, Any)*
  ): Unit = {
    DriverTcpConnectionLogger.debugSnapshot(
      "read_schema_source_tcp_snapshot",
      (Seq(
        "phase" -> phase,
        "format" -> format,
        "file" -> filePath
      ) ++ fields): _*
    )
  }

  def readSource(
    spark: SparkSession,
    format: String,
    filePaths: Seq[String],
    csvHasHeader: Boolean = true,
    readOptions: ScanReadOptions = ScanReadOptions()
  ): DataFrame = {
    require(filePaths.nonEmpty, "filePaths must not be empty")
    DriverLogger.debug("read_source_start", "format" -> format, "files" -> filePaths.size, "first_file" -> filePaths.head)
    val literalFilePaths = literalSparkPaths(filePaths)

    val (df, internalCorruptRecordColumnName) = format match {
      case "csv" =>
        (
          csvReader(spark, csvHasHeader, readOptions).csv(literalFilePaths: _*),
          None
        )
      case "json" =>
        val corruptRecordColumnName = newJsonCorruptRecordColumnName()
        (
          spark.read
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", corruptRecordColumnName)
            .json(literalFilePaths: _*),
          Some(corruptRecordColumnName)
        )
      case AvroFormat =>
        (spark.read.format("avro").load(literalFilePaths: _*), None)
      case XlsxFormat =>
        require(filePaths.size == 1, "xlsx sources must be read one sheet at a time")
        (
          readXlsx(spark, filePaths.head, readOptions),
          None
        )
      case TextFormat =>
        (readTextSource(spark, filePaths, readOptions), None)
      case "parquet" =>
        (spark.read.parquet(literalFilePaths: _*), None)
      case "orc" =>
        (spark.read.orc(literalFilePaths: _*), None)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported format: $format")
    }
    ensureReadableSourceColumns(format, filePaths, df, internalCorruptRecordColumnName)
  }

  private def readTextSource(spark: SparkSession, filePaths: Seq[String], readOptions: ScanReadOptions): DataFrame = {
    readOptions.textEncoding match {
      case Some(encoding) =>
        val encodingName = encoding
        val rowsByPath = filePaths.map { filePath =>
          spark.sparkContext
            .newAPIHadoopFile(
              literalSparkPath(filePath),
              classOf[TextInputFormat],
              classOf[LongWritable],
              classOf[Text]
            )
            .mapPartitions { rows =>
              val charset = Charset.forName(encodingName)
              rows.map { case (_, text) =>
                Row(new String(text.getBytes, 0, text.getLength, charset))
              }
            }
        }
        val rows = spark.sparkContext.union(rowsByPath)
        spark.createDataFrame(rows, StructType(Seq(StructField("value", StringType, nullable = true))))

      case None =>
        spark.read.text(literalSparkPaths(filePaths): _*)
    }
  }

  private def readXlsx(spark: SparkSession, filePath: String, readOptions: ScanReadOptions): DataFrame = {
    val sheetName = readOptions.sheetName.getOrElse {
      throw new IllegalArgumentException("Sheet name is required for xlsx sources")
    }
    WorkbookSheetStreamer.readSheetDataFrame(spark, filePath, sheetName)
  }
}
