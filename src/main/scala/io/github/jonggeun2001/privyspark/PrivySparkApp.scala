package io.github.jonggeun2001.privyspark

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, input_file_name, regexp_extract}
import io.github.jonggeun2001.privyspark.config.RulesetLoader
import io.github.jonggeun2001.privyspark.model.{PiiRule, ScanError, ScanResult}

import java.time.Instant
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

object PrivySparkApp {
  private[privyspark] final case class ScanFileEntry(filePath: String, directoryPath: String, format: String)
  private[privyspark] final case class ScanGroup(
    directoryPath: String,
    format: String,
    schemaSignature: String,
    filePaths: Seq[String]
  )
  private[privyspark] final case class DirectoryScanPlan(
    groups: Seq[ScanGroup],
    errors: Seq[ScanError],
    totalFiles: Int,
    directoryCount: Int
  )

  private val FileIdentifierColumn = "__privyspark_file_identifier"
  private[privyspark] val MaxFilesPerGroupBatchScan = 1000

  def main(args: Array[String]): Unit = {
    val normalizedArgs = if (args.headOption.contains("scan")) args.drop(1) else args

    val config = Cli.parse(normalizedArgs).getOrElse {
      System.exit(2)
      throw new IllegalStateException("unreachable")
    }

    if (!PathValidator.isAbsolute(config.inputPath)) {
      System.err.println(s"--path must be an absolute path or URI: ${config.inputPath}")
      System.exit(2)
    }

    if (!PathValidator.isAbsolute(config.outputPath)) {
      System.err.println(s"--output must be an absolute path or URI: ${config.outputPath}")
      System.exit(2)
    }

    val spark = SparkSession.builder().appName("PrivySpark").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      runScan(spark, config)
    } catch {
      case NonFatal(e) =>
        System.err.println(s"[PrivySpark] failed: ${e.getMessage}")
        System.exit(1)
    } finally {
      spark.stop()
    }
  }

  private def runScan(spark: SparkSession, config: CliConfig): Unit = {
    val rules = RulesetLoader.load(config.ruleset)
    val timestamp = Instant.now().toString
    val scanPlan = scanDirectoryStructure(spark, config.inputPath, config.inputPath, timestamp)

    val results = ArrayBuffer.empty[ScanResult]
    val errors = ArrayBuffer.empty[ScanError] ++ scanPlan.errors

    scanPlan.groups.foreach { group =>
      val (groupResults, groupErrors) =
        scanGroup(spark, config.inputPath, group, rules, config.sampleRatio, timestamp)
      results ++= groupResults
      errors ++= groupErrors
    }

    writeReports(spark, config.outputPath, results.toSeq, errors.toSeq)

    println(
      s"[PrivySpark] scanned_files=${scanPlan.totalFiles}, grouped_dirs=${scanPlan.directoryCount}, groups=${scanPlan.groups.size}, detections=${results.size}, errors=${errors.size}"
    )
  }

  private[privyspark] def scanDirectoryStructure(
    spark: SparkSession,
    inputPath: String,
    datasetPath: String,
    timestamp: String
  ): DirectoryScanPlan = {
    val conf = spark.sparkContext.hadoopConfiguration
    val path = new Path(inputPath)
    val fs = path.getFileSystem(conf)

    if (!fs.exists(path)) {
      throw new IllegalArgumentException(s"Input path not found: $inputPath")
    }

    val files = if (fs.getFileStatus(path).isFile) {
      Seq(path.toString)
    } else {
      val iter = fs.listFiles(path, true)
      val files = ArrayBuffer.empty[String]
      while (iter.hasNext) {
        val status = iter.next()
        if (status.isFile) {
          files += status.getPath.toString
        }
      }
      files.toSeq.sorted
    }

    val supportedFiles = ArrayBuffer.empty[ScanFileEntry]
    val errors = ArrayBuffer.empty[ScanError]

    files.foreach { filePath =>
      FormatDetector.infer(filePath) match {
        case Some(format) =>
          val parentDirectory = Option(new Path(filePath).getParent).map(_.toString).getOrElse(filePath)
          supportedFiles += ScanFileEntry(filePath, parentDirectory, format)
        case None =>
          errors += ScanError(
            datasetPath,
            timestamp,
            new Path(filePath).getName,
            s"Unsupported file format: $filePath"
          )
      }
    }

    val groupedByDirectoryAndFormat = supportedFiles
      .groupBy(file => (file.directoryPath, file.format))
      .toSeq
      .sortBy { case ((directoryPath, format), _) => (directoryPath, format) }
      .map {
        case ((directoryPath, format), groupedFiles) =>
          ScanGroup(
            directoryPath = directoryPath,
            format = format,
            schemaSignature = "",
            filePaths = groupedFiles.map(_.filePath).sorted
          )
      }

    val schemaAwareGroups = ArrayBuffer.empty[ScanGroup]
    groupedByDirectoryAndFormat.foreach { group =>
      val (splitGroups, splitErrors) = splitGroupBySchema(spark, datasetPath, timestamp, group)
      schemaAwareGroups ++= splitGroups
      errors ++= splitErrors
    }

    val directoryCount = files
      .map(filePath => Option(new Path(filePath).getParent).map(_.toString).getOrElse(filePath))
      .distinct
      .size

    DirectoryScanPlan(
      groups = schemaAwareGroups.toSeq.sortBy(group => (group.directoryPath, group.format, group.schemaSignature)),
      errors = errors.toSeq,
      totalFiles = files.size,
      directoryCount = directoryCount
    )
  }

  private def splitGroupBySchema(
    spark: SparkSession,
    datasetPath: String,
    timestamp: String,
    group: ScanGroup
  ): (Seq[ScanGroup], Seq[ScanError]) = {
    val filesBySchema = scala.collection.mutable.Map.empty[String, ArrayBuffer[String]]
    val errors = ArrayBuffer.empty[ScanError]

    group.filePaths.foreach { filePath =>
      inferSchemaSignature(spark, group.format, filePath) match {
        case Right(schemaSignature) =>
          val groupedFiles = filesBySchema.getOrElseUpdate(schemaSignature, ArrayBuffer.empty[String])
          groupedFiles += filePath
        case Left(errorMessage) =>
          errors += ScanError(
            datasetPath,
            timestamp,
            new Path(filePath).getName,
            s"Schema detection failed: $errorMessage"
          )
      }
    }

    val groups = filesBySchema.toSeq
      .sortBy { case (schemaSignature, _) => schemaSignature }
      .map {
        case (schemaSignature, groupedFiles) =>
          group.copy(schemaSignature = schemaSignature, filePaths = groupedFiles.toSeq.sorted)
      }

    (groups, errors.toSeq)
  }

  private def inferSchemaSignature(
    spark: SparkSession,
    format: String,
    filePath: String
  ): Either[String, String] = {
    try {
      val schema = readSchemaSource(spark, format, filePath).schema
      val schemaSignature = schema.fieldNames.map(_.toLowerCase).sorted.mkString("|")
      Right(schemaSignature)
    } catch {
      case NonFatal(e) =>
        Left(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
    }
  }

  private def readSchemaSource(spark: SparkSession, format: String, filePath: String): DataFrame = {
    format match {
      case "csv" =>
        spark.read
          .option("header", "true")
          .option("inferSchema", "false")
          .option("mode", "PERMISSIVE")
          .csv(filePath)
      case "json" =>
        spark.read
          .option("mode", "PERMISSIVE")
          .json(filePath)
      case "parquet" =>
        spark.read.parquet(filePath)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported format: $format")
    }
  }

  private[privyspark] def scanGroup(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    maxFilesPerGroupBatchScan: Int = MaxFilesPerGroupBatchScan
  ): (Seq[ScanResult], Seq[ScanError]) = {
    if (group.filePaths.size > maxFilesPerGroupBatchScan) {
      System.err.println(
        s"[PrivySpark] group_scan_fallback directory=${group.directoryPath} format=${group.format} files=${group.filePaths.size} reason=group_size_limit_exceeded($maxFilesPerGroupBatchScan)"
      )
      return scanGroupByFile(spark, datasetPath, group, rules, sampleRatio, timestamp)
    }

    try {
      val results = scanGroupBatch(spark, datasetPath, group, rules, sampleRatio, timestamp)
      (results, Seq.empty)
    } catch {
      case NonFatal(e) =>
        System.err.println(
          s"[PrivySpark] group_scan_fallback directory=${group.directoryPath} format=${group.format} schema=${group.schemaSignature} files=${group.filePaths.size} reason=${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}"
        )
        scanGroupByFile(spark, datasetPath, group, rules, sampleRatio, timestamp)
    }
  }

  private def scanGroupByFile(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String
  ): (Seq[ScanResult], Seq[ScanError]) = {
    val fallbackResults = ArrayBuffer.empty[ScanResult]
    val fallbackErrors = ArrayBuffer.empty[ScanError]
    group.filePaths.foreach { filePath =>
      scanFile(spark, datasetPath, filePath, rules, sampleRatio, timestamp) match {
        case Right(fileResults) => fallbackResults ++= fileResults
        case Left(error) => fallbackErrors += error
      }
    }

    (fallbackResults.toSeq, fallbackErrors.toSeq)
  }

  private[privyspark] def scanGroupBatch(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String
  ): Seq[ScanResult] = {
    val sourceDf = readSource(spark, group.format, group.filePaths)
      .withColumn(FileIdentifierColumn, regexp_extract(input_file_name(), "([^/]+)$", 1))

    val sampledDf = if (sampleRatio >= 1.0) sourceDf else sourceDf.sample(withReplacement = false, sampleRatio)

    sampledDf.cache()
    try {
      val sampledRowsByFile = sampledDf
        .groupBy(col(FileIdentifierColumn))
        .count()
        .collect()
        .flatMap { row =>
          val fileIdentifier = if (row.isNullAt(0)) null else row.getString(0)
          val rowCount = if (row.isNullAt(1)) 0L else row.getLong(1)
          if (fileIdentifier == null || fileIdentifier.isEmpty || rowCount <= 0L) {
            None
          } else {
            Some(fileIdentifier -> rowCount)
          }
        }
        .toMap

      if (sampledRowsByFile.isEmpty) {
        Seq.empty
      } else {
        val matchCounts = DetectionAggregator.aggregateByFile(sampledDf, FileIdentifierColumn, rules)
        matchCounts.flatMap { matchCount =>
          sampledRowsByFile.get(matchCount.fileIdentifier).map { sampledRowCount =>
            val matchRatio = matchCount.count.toDouble / sampledRowCount.toDouble
            ScanResult(
              dataset_path = datasetPath,
              scan_timestamp = timestamp,
              file_identifier = matchCount.fileIdentifier,
              column_name = matchCount.columnName,
              pii_type = matchCount.piiType,
              match_count = matchCount.count,
              match_ratio = matchRatio,
              confidence = matchRatio
            )
          }
        }
      }
    } finally {
      sampledDf.unpersist(blocking = true)
    }
  }

  private def scanFile(
    spark: SparkSession,
    datasetPath: String,
    filePath: String,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String
  ): Either[ScanError, Seq[ScanResult]] = {
    val fileIdentifier = new Path(filePath).getName

    try {
      val format = FormatDetector.infer(filePath).getOrElse {
        return Left(ScanError(datasetPath, timestamp, fileIdentifier, s"Unsupported file format: $filePath"))
      }

      val sourceDf = readSource(spark, format, Seq(filePath))
      val sampledDf = if (sampleRatio >= 1.0) sourceDf else sourceDf.sample(withReplacement = false, sampleRatio)

      sampledDf.cache()
      try {
        val sampledRowCount = sampledDf.count()

        if (sampledRowCount == 0L) {
          Right(Seq.empty)
        } else {
          val matchCounts = DetectionAggregator.aggregate(sampledDf, rules)
          val fileResults = matchCounts.map { matchCount =>
            val matchRatio = matchCount.count.toDouble / sampledRowCount.toDouble
            ScanResult(
              dataset_path = datasetPath,
              scan_timestamp = timestamp,
              file_identifier = fileIdentifier,
              column_name = matchCount.columnName,
              pii_type = matchCount.piiType,
              match_count = matchCount.count,
              match_ratio = matchRatio,
              confidence = matchRatio
            )
          }

          Right(fileResults)
        }
      } finally {
        sampledDf.unpersist(blocking = true)
      }
    } catch {
      case NonFatal(e) =>
        Left(ScanError(datasetPath, timestamp, fileIdentifier, Option(e.getMessage).getOrElse(e.getClass.getSimpleName)))
    }
  }

  private def readSource(spark: SparkSession, format: String, filePaths: Seq[String]): DataFrame = {
    require(filePaths.nonEmpty, "filePaths must not be empty")

    format match {
      case "csv" =>
        spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .option("mode", "PERMISSIVE")
          .csv(filePaths: _*)
      case "json" =>
        spark.read
          .option("mode", "PERMISSIVE")
          .json(filePaths: _*)
      case "parquet" =>
        spark.read.parquet(filePaths: _*)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported format: $format")
    }
  }

  private def writeReports(
    spark: SparkSession,
    outputRoot: String,
    results: Seq[ScanResult],
    errors: Seq[ScanError]
  ): Unit = {
    import spark.implicits._

    val root = outputRoot.stripSuffix("/")
    val resultDf = spark.createDataset(results).toDF()
    val errorDf = spark.createDataset(errors).toDF()

    val resultParquetPath = s"$root/parquet/scan_results"
    val errorParquetPath = s"$root/parquet/scan_errors"
    val resultExcelPath = s"$root/excel/scan_results.xlsx"
    val errorExcelPath = s"$root/excel/scan_errors.xlsx"

    resultDf.write.mode("overwrite").parquet(resultParquetPath)
    errorDf.write.mode("overwrite").parquet(errorParquetPath)

    resultDf.write
      .format("com.crealytics.spark.excel")
      .option("header", "true")
      .mode("overwrite")
      .save(resultExcelPath)

    errorDf.write
      .format("com.crealytics.spark.excel")
      .option("header", "true")
      .mode("overwrite")
      .save(errorExcelPath)
  }
}
