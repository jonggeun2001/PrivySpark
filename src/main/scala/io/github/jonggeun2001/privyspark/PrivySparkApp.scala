package io.github.jonggeun2001.privyspark

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.github.jonggeun2001.privyspark.config.RulesetLoader
import io.github.jonggeun2001.privyspark.model.{PiiRule, ScanError, ScanResult}

import java.time.Instant
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

object PrivySparkApp {
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
    val files = listFiles(spark, config.inputPath)
    val timestamp = Instant.now().toString

    val results = ArrayBuffer.empty[ScanResult]
    val errors = ArrayBuffer.empty[ScanError]

    files.foreach { filePath =>
      scanFile(spark, config.inputPath, filePath, rules, config.sampleRatio, timestamp) match {
        case Right(fileResults) => results ++= fileResults
        case Left(error) => errors += error
      }
    }

    writeReports(spark, config.outputPath, results.toSeq, errors.toSeq)

    println(s"[PrivySpark] scanned_files=${files.size}, detections=${results.size}, errors=${errors.size}")
  }

  private def listFiles(spark: SparkSession, inputPath: String): Seq[String] = {
    val conf = spark.sparkContext.hadoopConfiguration
    val path = new Path(inputPath)
    val fs = path.getFileSystem(conf)

    if (!fs.exists(path)) {
      throw new IllegalArgumentException(s"Input path not found: $inputPath")
    }

    if (fs.getFileStatus(path).isFile) {
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

      val sourceDf = readSource(spark, format, filePath)
      val sampledDf = if (sampleRatio >= 1.0) sourceDf else sourceDf.sample(withReplacement = false, sampleRatio)
      val sampledRowCount = sampledDf.count()

      if (sampledRowCount == 0L) {
        Right(Seq.empty)
      } else {
        val fileResults = ArrayBuffer.empty[ScanResult]

        sampledDf.columns.foreach { columnName =>
          val columnValue = col(columnName).cast(StringType)

          rules.foreach { rule =>
            val matchCount = sampledDf.filter(columnValue.isNotNull && columnValue.rlike(rule.regex)).count()
            if (matchCount > 0L) {
              val matchRatio = matchCount.toDouble / sampledRowCount.toDouble
              fileResults += ScanResult(
                dataset_path = datasetPath,
                scan_timestamp = timestamp,
                file_identifier = fileIdentifier,
                column_name = columnName,
                pii_type = rule.piiType,
                match_count = matchCount,
                match_ratio = matchRatio,
                confidence = matchRatio
              )
            }
          }
        }

        Right(fileResults.toSeq)
      }
    } catch {
      case NonFatal(e) =>
        Left(ScanError(datasetPath, timestamp, fileIdentifier, Option(e.getMessage).getOrElse(e.getClass.getSimpleName)))
    }
  }

  private def readSource(spark: SparkSession, format: String, filePath: String): DataFrame = {
    format match {
      case "csv" =>
        spark.read
          .option("header", "true")
          .option("inferSchema", "true")
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
