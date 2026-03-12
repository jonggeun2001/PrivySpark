package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.DetectionAggregator.MatchCount
import io.github.jonggeun2001.privyspark.config.RulesetLoader
import io.github.jonggeun2001.privyspark.model.{PiiRule, ScanError, ScanResult}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, input_file_name}

import java.time.Instant
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

object PrivySparkApp {
  private[privyspark] final case class ScanFileEntry(filePath: String, directoryPath: String, format: String)
  private[privyspark] final case class ScanGroup(
    directoryPath: String,
    format: String,
    schemaSignature: String,
    filePaths: Seq[String],
    useDirectoryIdentifier: Boolean = false
  )
  private[privyspark] final case class DirectoryScanPlan(
    groups: Seq[ScanGroup],
    errors: Seq[ScanError],
    totalFiles: Int,
    directoryCount: Int
  )
  private final case class FileScanMetrics(
    fileIdentifier: String,
    sampledRowCount: Long,
    matchCounts: Seq[MatchCount]
  )

  private val FileIdentifierColumn = "__privyspark_file_identifier"
  private[privyspark] val MaxFilesPerGroupBatchScan = 1000

  private def logDriver(message: String): Unit = {
    System.err.println(s"[PrivySpark] $message")
  }

  private def stripTrailingSlash(path: String): String = {
    val normalized = Option(path).getOrElse("").replace('\\', '/')
    if (normalized == "/") normalized else normalized.replaceAll("/+$", "")
  }

  private def fallbackIdentifier(path: String): String = {
    val normalized = stripTrailingSlash(path)
    Option(new Path(normalized).getName).filter(_.nonEmpty).getOrElse(normalized)
  }

  private[privyspark] def resolveRelativeIdentifier(datasetPath: String, targetPath: String): String = {
    resolveRelativeIdentifier(datasetPath, targetPath, useCurrentDirectoryMarker = false)
  }

  private def resolveDirectoryIdentifier(datasetPath: String, directoryPath: String): String = {
    resolveRelativeIdentifier(datasetPath, directoryPath, useCurrentDirectoryMarker = true)
  }

  private def resolveRelativeIdentifier(
    datasetPath: String,
    targetPath: String,
    useCurrentDirectoryMarker: Boolean
  ): String = {
    val datasetUri = new Path(datasetPath).toUri.normalize()
    val targetUri = new Path(targetPath).toUri.normalize()

    val schemesCompatible =
      datasetUri.getScheme == null || targetUri.getScheme == null || datasetUri.getScheme == targetUri.getScheme
    val authoritiesCompatible =
      datasetUri.getAuthority == null || targetUri.getAuthority == null || datasetUri.getAuthority == targetUri.getAuthority

    val datasetComparablePath = stripTrailingSlash(Option(datasetUri.getPath).filter(_.nonEmpty).getOrElse(datasetPath))
    val targetComparablePath = stripTrailingSlash(Option(targetUri.getPath).filter(_.nonEmpty).getOrElse(targetPath))

    if (!schemesCompatible || !authoritiesCompatible) {
      fallbackIdentifier(targetPath)
    } else if (datasetComparablePath == targetComparablePath) {
      if (useCurrentDirectoryMarker) "." else fallbackIdentifier(targetPath)
    } else {
      val prefix = if (datasetComparablePath == "/") "/" else s"$datasetComparablePath/"
      if (targetComparablePath.startsWith(prefix)) {
        targetComparablePath.substring(prefix.length)
      } else {
        fallbackIdentifier(targetPath)
      }
    }
  }

  private def buildScanResults(
    datasetPath: String,
    timestamp: String,
    fileIdentifier: String,
    sampledRowCount: Long,
    matchCounts: Seq[MatchCount]
  ): Seq[ScanResult] = {
    if (sampledRowCount <= 0L) {
      Seq.empty
    } else {
      matchCounts.map { matchCount =>
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
    }
  }

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
    val directoriesWithPreScanErrors = scala.collection.mutable.Set.empty[String]

    files.foreach { filePath =>
      val parentDirectory = Option(new Path(filePath).getParent).map(_.toString).getOrElse(filePath)
      FormatDetector.infer(filePath) match {
        case Some(format) =>
          supportedFiles += ScanFileEntry(filePath, parentDirectory, format)
        case None =>
          directoriesWithPreScanErrors += parentDirectory
          errors += ScanError(
            datasetPath,
            timestamp,
            resolveRelativeIdentifier(datasetPath, filePath),
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
      if (splitErrors.nonEmpty) {
        directoriesWithPreScanErrors += group.directoryPath
      }
    }

    val groupsPerDirectory = schemaAwareGroups.groupBy(_.directoryPath).map {
      case (directoryPath, groups) => directoryPath -> groups.size
    }

    val finalizedGroups = schemaAwareGroups.map { group =>
      val useDirectoryIdentifier =
        groupsPerDirectory.getOrElse(group.directoryPath, 0) == 1 &&
          group.filePaths.size > 1 &&
          !directoriesWithPreScanErrors.contains(group.directoryPath)
      group.copy(useDirectoryIdentifier = useDirectoryIdentifier)
    }

    val directoryCount = files
      .map(filePath => Option(new Path(filePath).getParent).map(_.toString).getOrElse(filePath))
      .distinct
      .size

    DirectoryScanPlan(
      groups = finalizedGroups.toSeq.sortBy(group => (group.directoryPath, group.format, group.schemaSignature)),
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
            resolveRelativeIdentifier(datasetPath, filePath),
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
      val normalizedFieldNames = schema.fieldNames.map(_.toLowerCase)
      val schemaSignature = if (format == "csv") {
        // CSV는 헤더 순서가 데이터 매핑에 직접 영향을 주므로 순서를 유지한다.
        normalizedFieldNames.mkString("|")
      } else {
        normalizedFieldNames.sorted.mkString("|")
      }
      Right(schemaSignature)
    } catch {
      case NonFatal(e) =>
        Left(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
    }
  }

  private def resolveFileIdentifierColumn(columns: Seq[String]): String = {
    val normalized = columns.map(_.toLowerCase).toSet
    var candidate = FileIdentifierColumn
    var index = 1

    while (normalized.contains(candidate.toLowerCase)) {
      candidate = s"${FileIdentifierColumn}_$index"
      index += 1
    }

    candidate
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
      case "orc" =>
        spark.read.orc(filePath)
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
      logDriver(
        s"group_scan_fallback directory=${group.directoryPath} format=${group.format} files=${group.filePaths.size} reason=group_size_limit_exceeded($maxFilesPerGroupBatchScan)"
      )
      return scanGroupByFile(spark, datasetPath, group, rules, sampleRatio, timestamp)
    }

    try {
      val results = scanGroupBatch(spark, datasetPath, group, rules, sampleRatio, timestamp)
      (results, Seq.empty)
    } catch {
      case NonFatal(e) =>
        logDriver(
          s"group_scan_fallback directory=${group.directoryPath} format=${group.format} schema=${group.schemaSignature} files=${group.filePaths.size} reason=${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}"
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
    logDriver(
      s"group_scan_fallback_execute directory=${group.directoryPath} format=${group.format} schema=${group.schemaSignature} files=${group.filePaths.size} mode=file_scan"
    )
    val successfulFileMetrics = ArrayBuffer.empty[FileScanMetrics]
    val fallbackErrors = ArrayBuffer.empty[ScanError]
    group.filePaths.foreach { filePath =>
      scanFileMetrics(spark, datasetPath, filePath, rules, sampleRatio, timestamp) match {
        case Right(fileMetrics) => successfulFileMetrics += fileMetrics
        case Left(error) => fallbackErrors += error
      }
    }

    val fallbackResults = if (group.useDirectoryIdentifier && fallbackErrors.isEmpty) {
      val sampledRowCount = successfulFileMetrics.map(_.sampledRowCount).sum
      val aggregatedMatchCounts = successfulFileMetrics
        .flatMap(_.matchCounts)
        .groupBy(matchCount => (matchCount.columnName, matchCount.piiType))
        .toSeq
        .sortBy { case ((columnName, piiType), _) => (columnName, piiType) }
        .map {
          case ((columnName, piiType), matchCounts) =>
            MatchCount(columnName, piiType, matchCounts.map(_.count).sum)
        }

      buildScanResults(
        datasetPath,
        timestamp,
        resolveDirectoryIdentifier(datasetPath, group.directoryPath),
        sampledRowCount,
        aggregatedMatchCounts
      )
    } else {
      if (group.useDirectoryIdentifier && fallbackErrors.nonEmpty) {
        logDriver(
          s"group_scan_partial_results directory=${group.directoryPath} format=${group.format} schema=${group.schemaSignature} failed_files=${fallbackErrors.size} mode=file_identifier_preserved"
        )
      }
      successfulFileMetrics.flatMap { fileMetrics =>
        buildScanResults(
          datasetPath,
          timestamp,
          fileMetrics.fileIdentifier,
          fileMetrics.sampledRowCount,
          fileMetrics.matchCounts
        )
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
    val baseDf = readSource(spark, group.format, group.filePaths)
    val fileIdentifierColumn = if (group.useDirectoryIdentifier) {
      None
    } else {
      Some(resolveFileIdentifierColumn(baseDf.columns.toSeq))
    }
    val sourceDf = fileIdentifierColumn match {
      case Some(columnName) => baseDf.withColumn(columnName, input_file_name())
      case None => baseDf
    }

    val sampledDf = if (sampleRatio >= 1.0) sourceDf else sourceDf.sample(withReplacement = false, sampleRatio)

    sampledDf.cache()
    try {
      fileIdentifierColumn match {
        case None =>
          val sampledRowCount = sampledDf.count()
          if (sampledRowCount == 0L) {
            Seq.empty
          } else {
            buildScanResults(
              datasetPath,
              timestamp,
              resolveDirectoryIdentifier(datasetPath, group.directoryPath),
              sampledRowCount,
              DetectionAggregator.aggregate(sampledDf, rules)
            )
          }
        case Some(columnName) =>
          val sampledRowsByFile = sampledDf
            .groupBy(col(columnName))
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
            DetectionAggregator.aggregateByFile(sampledDf, columnName, rules).flatMap { matchCount =>
              sampledRowsByFile.get(matchCount.fileIdentifier).flatMap { sampledRowCount =>
                buildScanResults(
                  datasetPath,
                  timestamp,
                  resolveRelativeIdentifier(datasetPath, matchCount.fileIdentifier),
                  sampledRowCount,
                  Seq(MatchCount(matchCount.columnName, matchCount.piiType, matchCount.count))
                ).headOption
              }
            }
          }
      }
    } finally {
      sampledDf.unpersist(blocking = true)
    }
  }

  private def scanFileMetrics(
    spark: SparkSession,
    datasetPath: String,
    filePath: String,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String
  ): Either[ScanError, FileScanMetrics] = {
    val fileIdentifier = resolveRelativeIdentifier(datasetPath, filePath)

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
          Right(FileScanMetrics(fileIdentifier, sampledRowCount, Seq.empty))
        } else {
          Right(FileScanMetrics(fileIdentifier, sampledRowCount, DetectionAggregator.aggregate(sampledDf, rules)))
        }
      } finally {
        sampledDf.unpersist(blocking = true)
      }
    } catch {
      case NonFatal(e) =>
        Left(ScanError(datasetPath, timestamp, fileIdentifier, Option(e.getMessage).getOrElse(e.getClass.getSimpleName)))
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
    scanFileMetrics(spark, datasetPath, filePath, rules, sampleRatio, timestamp).map { fileMetrics =>
      buildScanResults(
        datasetPath,
        timestamp,
        fileMetrics.fileIdentifier,
        fileMetrics.sampledRowCount,
        fileMetrics.matchCounts
      )
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
      case "orc" =>
        spark.read.orc(filePaths: _*)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported format: $format")
    }
  }

  private[privyspark] def writeReports(
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
    val resultCsvPath = s"$root/csv/scan_results"
    val errorCsvPath = s"$root/csv/scan_errors"

    resultDf.write.mode("overwrite").parquet(resultParquetPath)
    errorDf.write.mode("overwrite").parquet(errorParquetPath)

    resultDf.write
      .option("header", "true")
      .mode("overwrite")
      .csv(resultCsvPath)

    errorDf.write
      .option("header", "true")
      .mode("overwrite")
      .csv(errorCsvPath)
  }
}
