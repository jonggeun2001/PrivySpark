package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.DetectionAggregator.MatchCount
import io.github.jonggeun2001.privyspark.config.RulesetLoader
import io.github.jonggeun2001.privyspark.model.{PiiRule, ScanError, ScanResult}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.execution.datasources.csv.CSVUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, input_file_name}

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Executors
import java.nio.file.NoSuchFileException
import com.univocity.parsers.csv.CsvParser
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal

object PrivySparkApp {
  private[privyspark] final case class ScanFileEntry(filePath: String, directoryPath: String, format: String)
  private[privyspark] final case class ScanGroup(
    directoryPath: String,
    format: String,
    schemaSignature: String,
    filePaths: Seq[String],
    useDirectoryIdentifier: Boolean = false,
    directoryIdentifierEligible: Boolean = false,
    schemaSampled: Boolean = false,
    csvHasHeader: Boolean = true
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
  private[privyspark] val MaxFileReadAttempts = 2
  private[privyspark] val FileReadRetryDelayMillis = 200L
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
  private val GroupParallelismConfKey = "spark.privyspark.groupParallelism"
  private val DefaultGroupParallelism = 4
  private val FileParallelismConfKey = "spark.privyspark.fileParallelism"
  private val DefaultFileParallelism = 3
  private val DebugPropertyName = "privyspark.debug"
  private val DebugEnvName = "PRIVYSPARK_DEBUG"
  private val RetriableFileReadErrorSnippets = Seq(
    "path does not exist",
    "file does not exist",
    "no such file",
    "underlying files have been updated",
    "failed_read_file",
    "encountered error while reading file"
  )
  @volatile private var debugLoggingEnabledCache: java.lang.Boolean = _

  private def logDriver(message: String): Unit = {
    System.err.println(s"[PrivySpark] $message")
  }

  private def isDebugLoggingEnabled: Boolean = {
    val cached = debugLoggingEnabledCache
    if (cached != null) {
      cached.booleanValue()
    } else {
      val rawValue = sys.props.get(DebugPropertyName).orElse(sys.env.get(DebugEnvName))
      val enabled = rawValue.exists { value =>
        value.trim.toLowerCase match {
          case "1" | "true" | "yes" | "on" => true
          case _ => false
        }
      }
      debugLoggingEnabledCache = java.lang.Boolean.valueOf(enabled)
      enabled
    }
  }

  private[privyspark] def resetDebugCache(): Unit = {
    debugLoggingEnabledCache = null
  }

  private def logDebug(event: String, fields: (String, Any)*): Unit = {
    if (!isDebugLoggingEnabled) {
      return
    }

    val suffix = if (fields.isEmpty) {
      ""
    } else {
      fields.map {
        case (key, value) =>
          val renderedValue = if (value == null) "null" else value.toString
          s"$key=$renderedValue"
      }.mkString(" ", " ", "")
    }

    System.err.println(s"[PrivySpark][DEBUG] $event$suffix")
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
        val matchRatio = roundProbability(matchCount.count.toDouble / sampledRowCount.toDouble)
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

  private def roundProbability(value: Double): Double = {
    BigDecimal.decimal(value)
      .setScale(2, scala.math.BigDecimal.RoundingMode.HALF_UP)
      .toDouble
  }

  @tailrec
  private def collectThrowableChain(current: Throwable, acc: Vector[Throwable] = Vector.empty): Vector[Throwable] = {
    if (current == null || acc.contains(current)) {
      acc
    } else {
      collectThrowableChain(current.getCause, acc :+ current)
    }
  }

  private def formatThrowableSummary(error: Throwable): String = {
    collectThrowableChain(error)
      .flatMap { cause =>
        Option(cause.getMessage)
          .filter(_.trim.nonEmpty)
          .map(message => s"${cause.getClass.getSimpleName}: ${message.trim}")
      }
      .headOption
      .getOrElse(error.getClass.getSimpleName)
  }

  private def isRetriableFileReadFailure(error: Throwable): Boolean = {
    collectThrowableChain(error).exists {
      case _: java.io.FileNotFoundException => true
      case _: NoSuchFileException => true
      case cause =>
        val normalizedMessage = Option(cause.getMessage).map(_.toLowerCase).getOrElse("")
        RetriableFileReadErrorSnippets.exists(normalizedMessage.contains)
    }
  }

  private def refreshReadPaths(spark: SparkSession, filePaths: Seq[String]): Unit = {
    val refreshTargets = filePaths.distinct.flatMap { path =>
      Seq(Some(path), Option(new Path(path).getParent).map(_.toString)).flatten
    }.distinct

    refreshTargets.foreach { path =>
      try {
        spark.catalog.refreshByPath(path)
      } catch {
        case NonFatal(_) => ()
      }
    }
  }

  private def pauseBeforeRetry(delayMs: Long): Unit = {
    if (delayMs > 0L) {
      try {
        Thread.sleep(delayMs)
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          throw new IllegalStateException("File read retry interrupted")
      }
    }
  }

  private[privyspark] def withFileReadRetry[A](
    spark: SparkSession,
    filePaths: Seq[String],
    operationName: String,
    maxAttempts: Int = MaxFileReadAttempts,
    retryDelayMs: Long = FileReadRetryDelayMillis
  )(block: => A): A = {
    require(maxAttempts >= 1, "maxAttempts must be >= 1")

    def attempt(attemptNumber: Int): A = {
      try {
        block
      } catch {
        case NonFatal(error) if attemptNumber < maxAttempts && isRetriableFileReadFailure(error) =>
          val nextAttempt = attemptNumber + 1
          val reason = formatThrowableSummary(error)
          logDriver(
            s"file_read_retry operation=$operationName attempt=$nextAttempt/$maxAttempts files=${filePaths.size} reason=$reason"
          )
          logDebug(
            "file_read_retry",
            "operation" -> operationName,
            "attempt" -> nextAttempt,
            "max_attempts" -> maxAttempts,
            "files" -> filePaths.size,
            "reason" -> reason
          )
          refreshReadPaths(spark, filePaths)
          pauseBeforeRetry(retryDelayMs)
          attempt(nextAttempt)
      }
    }

    attempt(1)
  }

  private def resolveParallelism(itemCount: Int, configured: Int): Int = {
    if (itemCount <= 1) 1 else math.max(1, math.min(itemCount, configured))
  }

  private[privyspark] def resolveGroupParallelism(spark: SparkSession, groupCount: Int): Int = {
    resolveParallelism(groupCount, spark.sparkContext.getConf.getInt(GroupParallelismConfKey, DefaultGroupParallelism))
  }

  private[privyspark] def resolveFileParallelism(spark: SparkSession, fileCount: Int): Int = {
    resolveParallelism(fileCount, spark.sparkContext.getConf.getInt(FileParallelismConfKey, DefaultFileParallelism))
  }

  private def executeInParallel[A](parallelism: Int, tasks: Seq[() => A]): Seq[A] = {
    if (tasks.isEmpty) {
      Seq.empty
    } else if (parallelism <= 1 || tasks.size <= 1) {
      tasks.map(task => task())
    } else {
      val pool = Executors.newFixedThreadPool(parallelism)
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
      try {
        Await.result(Future.sequence(tasks.map(task => Future(task()))), Duration.Inf)
      } finally {
        pool.shutdown()
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
    logDebug(
      "scan_run_start",
      "input_path" -> config.inputPath,
      "output_path" -> config.outputPath,
      "ruleset" -> config.ruleset,
      "sample_ratio" -> config.sampleRatio
    )
    val rules = RulesetLoader.load(config.ruleset)
    logDebug("ruleset_loaded", "rules" -> rules.size, "ruleset" -> config.ruleset)
    val timestamp = Instant.now().toString
    val scanPlan = scanDirectoryStructure(spark, config.inputPath, config.inputPath, timestamp)
    logDebug(
      "scan_plan_ready",
      "groups" -> scanPlan.groups.size,
      "plan_errors" -> scanPlan.errors.size,
      "total_files" -> scanPlan.totalFiles,
      "directories" -> scanPlan.directoryCount
    )

    val results = ArrayBuffer.empty[ScanResult]
    val errors = ArrayBuffer.empty[ScanError] ++ scanPlan.errors

    scanGroups(
      spark,
      config.inputPath,
      scanPlan.groups,
      rules,
      config.sampleRatio,
      timestamp
    ).foreach {
      case (_, groupResults, groupErrors) =>
        results ++= groupResults
        errors ++= groupErrors
    }

    logDebug("report_write_start", "results" -> results.size, "errors" -> errors.size, "output_root" -> config.outputPath)
    writeReports(spark, config.outputPath, results.toSeq, errors.toSeq)
    logDebug("report_write_complete", "results" -> results.size, "errors" -> errors.size, "output_root" -> config.outputPath)

    println(
      s"[PrivySpark] scanned_files=${scanPlan.totalFiles}, grouped_dirs=${scanPlan.directoryCount}, groups=${scanPlan.groups.size}, detections=${results.size}, errors=${errors.size}"
    )
  }

  private[privyspark] def scanGroups(
    spark: SparkSession,
    datasetPath: String,
    groups: Seq[ScanGroup],
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    groupParallelism: Int = -1
  ): Seq[(ScanGroup, Seq[ScanResult], Seq[ScanError])] = {
    if (groups.isEmpty) {
      return Seq.empty
    }

    val parallelism = if (groupParallelism > 0) {
      resolveParallelism(groups.size, groupParallelism)
    } else {
      resolveGroupParallelism(spark, groups.size)
    }
    logDebug("group_scan_parallelism", "groups" -> groups.size, "parallelism" -> parallelism)

    executeInParallel(parallelism, groups.map { group =>
      () => {
        logDebug(
          "group_scan_dispatch",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "files" -> group.filePaths.size,
          "use_directory_identifier" -> group.useDirectoryIdentifier,
          "parallelism" -> parallelism
        )
        val (groupResults, groupErrors) =
          scanGroup(spark, datasetPath, group, rules, sampleRatio, timestamp)
        logDebug(
          "group_scan_recorded",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "result_rows" -> groupResults.size,
          "error_rows" -> groupErrors.size
        )
        (group, groupResults, groupErrors)
      }
    })
  }

  private[privyspark] def scanDirectoryStructure(
    spark: SparkSession,
    inputPath: String,
    datasetPath: String,
    timestamp: String
  ): DirectoryScanPlan = {
    logDebug("scan_directory_structure_start", "input_path" -> inputPath, "dataset_path" -> datasetPath)
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
    logDebug("scan_directory_files_discovered", "input_path" -> inputPath, "files" -> files.size)

    val supportedFiles = ArrayBuffer.empty[ScanFileEntry]
    val errors = ArrayBuffer.empty[ScanError]
    val directoriesWithPreScanErrors = scala.collection.mutable.Set.empty[String]

    files.foreach { filePath =>
      val parentDirectory = Option(new Path(filePath).getParent).map(_.toString).getOrElse(filePath)
      FormatDetector.infer(filePath) match {
        case Some(format) =>
          supportedFiles += ScanFileEntry(filePath, parentDirectory, format)
          logDebug("scan_directory_file_supported", "file" -> filePath, "format" -> format, "directory" -> parentDirectory)
        case None =>
          directoriesWithPreScanErrors += parentDirectory
          errors += ScanError(
            datasetPath,
            timestamp,
            resolveRelativeIdentifier(datasetPath, filePath),
            s"Unsupported file format: $filePath"
          )
          logDebug("scan_directory_file_unsupported", "file" -> filePath, "directory" -> parentDirectory)
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
    logDebug("scan_directory_initial_groups_ready", "groups" -> groupedByDirectoryAndFormat.size, "supported_files" -> supportedFiles.size)

    val schemaAwareGroups = ArrayBuffer.empty[ScanGroup]
    groupedByDirectoryAndFormat.foreach { group =>
      val (splitGroups, splitErrors) = splitGroupBySchemaFast(spark, datasetPath, timestamp, group)
      schemaAwareGroups ++= splitGroups
      errors ++= splitErrors
      logDebug(
        "scan_directory_group_schema_split",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "input_files" -> group.filePaths.size,
        "split_groups" -> splitGroups.size,
        "split_errors" -> splitErrors.size
      )
      if (splitErrors.nonEmpty) {
        directoriesWithPreScanErrors += group.directoryPath
      }
    }

    val groupsPerDirectory = schemaAwareGroups.groupBy(_.directoryPath).map {
      case (directoryPath, groups) => directoryPath -> groups.size
    }

    val finalizedGroups = schemaAwareGroups.map { group =>
      val directoryIdentifierEligible =
        groupsPerDirectory.getOrElse(group.directoryPath, 0) == 1 &&
          group.filePaths.size > 1 &&
          !directoriesWithPreScanErrors.contains(group.directoryPath)
      val finalizedGroup = group.copy(
        useDirectoryIdentifier = directoryIdentifierEligible && !group.schemaSampled,
        directoryIdentifierEligible = directoryIdentifierEligible
      )
      logDebug(
        "scan_group_planned",
        "directory" -> finalizedGroup.directoryPath,
        "format" -> finalizedGroup.format,
        "schema" -> finalizedGroup.schemaSignature,
        "files" -> finalizedGroup.filePaths.size,
        "use_directory_identifier" -> finalizedGroup.useDirectoryIdentifier,
        "schema_sampled" -> finalizedGroup.schemaSampled,
        "csv_has_header" -> finalizedGroup.csvHasHeader
      )
      finalizedGroup
    }

    val directoryCount = files
      .map(filePath => Option(new Path(filePath).getParent).map(_.toString).getOrElse(filePath))
      .distinct
      .size
    val plannedGroups = finalizedGroups.toSeq.sortBy(group => (group.directoryPath, group.format, group.schemaSignature))

    logDebug(
      "scan_directory_structure_complete",
      "input_path" -> inputPath,
      "total_files" -> files.size,
      "supported_files" -> supportedFiles.size,
      "groups" -> plannedGroups.size,
      "errors" -> errors.size,
      "directories" -> directoryCount
    )

    DirectoryScanPlan(
      groups = plannedGroups,
      errors = errors.toSeq,
      totalFiles = files.size,
      directoryCount = directoryCount
    )
  }

  private[privyspark] def splitGroupBySchemaFast(
    spark: SparkSession,
    datasetPath: String,
    timestamp: String,
    group: ScanGroup
  ): (Seq[ScanGroup], Seq[ScanError]) = {
    if (group.filePaths.size <= 1) {
      splitGroupBySchema(spark, datasetPath, timestamp, group)
    } else {
      logDebug(
        "scan_group_schema_sample_start",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "files" -> group.filePaths.size
      )

      val sampledSchemaResult = if (group.format == "csv") {
        inferCsvSchemaSignature(spark, group.filePaths.head)
      } else {
        inferSchemaSignature(spark, group.format, group.filePaths.head).map(signature => (signature, true))
      }

      sampledSchemaResult match {
        case Right((schemaSignature, csvHasHeader)) =>
          val (validatedFilePaths, validationErrors) =
            if (group.format == "json") {
              validateSampledJsonFiles(spark, datasetPath, timestamp, group)
            } else {
              (group.filePaths, Seq.empty)
            }

          if (validatedFilePaths.isEmpty) {
            return (Seq.empty, validationErrors)
          }

          val sampledGroup = group.copy(
            schemaSignature = schemaSignature,
            filePaths = validatedFilePaths.sorted,
            schemaSampled = validatedFilePaths.size > 1,
            csvHasHeader = csvHasHeader
          )
          logDebug(
            "scan_group_schema_sample_complete",
            "directory" -> group.directoryPath,
            "format" -> group.format,
            "schema" -> schemaSignature,
            "files" -> validatedFilePaths.size,
            "filtered_errors" -> validationErrors.size,
            "csv_has_header" -> csvHasHeader
          )
          (Seq(sampledGroup), validationErrors)
        case Left(errorMessage) =>
          logDebug(
            "scan_group_schema_sample_fallback",
            "directory" -> group.directoryPath,
            "format" -> group.format,
            "files" -> group.filePaths.size,
            "reason" -> errorMessage
          )
          splitGroupBySchema(spark, datasetPath, timestamp, group)
      }
    }
  }

  private def validateSampledJsonFiles(
    spark: SparkSession,
    datasetPath: String,
    timestamp: String,
    group: ScanGroup
  ): (Seq[String], Seq[ScanError]) = {
    val validFilePaths = ArrayBuffer.empty[String]
    val errors = ArrayBuffer.empty[ScanError]

    group.filePaths.foreach { filePath =>
      try {
        withFileReadRetry(spark, Seq(filePath), "schema_detection") {
          readSchemaSource(spark, group.format, filePath, group.csvHasHeader)
          ()
        }
        validFilePaths += filePath
      } catch {
        case NonFatal(e) =>
          val errorMessage = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
          logDebug(
            "group_schema_signature_failed",
            "directory" -> group.directoryPath,
            "file" -> filePath,
            "format" -> group.format,
            "reason" -> errorMessage
          )
          errors += ScanError(
            datasetPath,
            timestamp,
            resolveRelativeIdentifier(datasetPath, filePath),
            s"Schema detection failed: $errorMessage"
          )
      }
    }

    (validFilePaths.toSeq, errors.toSeq)
  }

  private[privyspark] def splitGroupBySchema(
    spark: SparkSession,
    datasetPath: String,
    timestamp: String,
    group: ScanGroup
  ): (Seq[ScanGroup], Seq[ScanError]) = {
    logDebug(
      "scan_group_schema_split_start",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "files" -> group.filePaths.size
    )
    val filesBySchema = scala.collection.mutable.Map.empty[(String, Boolean), ArrayBuffer[String]]
    val errors = ArrayBuffer.empty[ScanError]

    group.filePaths.foreach { filePath =>
      val schemaResult = if (group.format == "csv") {
        inferCsvSchemaSignature(spark, filePath)
      } else {
        inferSchemaSignature(spark, group.format, filePath).map(signature => (signature, true))
      }

      schemaResult match {
        case Right((schemaSignature, csvHasHeader)) =>
          val groupedFiles = filesBySchema.getOrElseUpdate((schemaSignature, csvHasHeader), ArrayBuffer.empty[String])
          groupedFiles += filePath
          logDebug(
            "group_schema_signature_detected",
            "directory" -> group.directoryPath,
            "file" -> filePath,
            "format" -> group.format,
            "schema" -> schemaSignature,
            "csv_has_header" -> csvHasHeader
          )
        case Left(errorMessage) =>
          logDebug(
            "group_schema_signature_failed",
            "directory" -> group.directoryPath,
            "file" -> filePath,
            "format" -> group.format,
            "reason" -> errorMessage
          )
          errors += ScanError(
            datasetPath,
            timestamp,
            resolveRelativeIdentifier(datasetPath, filePath),
            s"Schema detection failed: $errorMessage"
          )
      }
    }

    val groups = filesBySchema.toSeq
      .sortBy { case ((schemaSignature, csvHasHeader), _) => (schemaSignature, csvHasHeader) }
      .map {
        case ((schemaSignature, csvHasHeader), groupedFiles) =>
          group.copy(
            schemaSignature = schemaSignature,
            filePaths = groupedFiles.toSeq.sorted,
            schemaSampled = false,
            csvHasHeader = csvHasHeader
          )
      }

    logDebug(
      "scan_group_schema_split_complete",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema_groups" -> groups.size,
      "errors" -> errors.size
    )
    (groups, errors.toSeq)
  }

  private[privyspark] def parseHeaderFields(line: String): Seq[String] = {
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

  private[privyspark] def inferCsvHeaderSignature(
    spark: SparkSession,
    filePath: String
  ): Either[String, String] = {
    try {
      val signature = withFileReadRetry(spark, Seq(filePath), "csv_header_signature") {
        val csvOptions = createCsvOptions(spark)
        val headerLine = readFirstNonBlankCsvLines(spark, filePath, maxLines = 1).headOption
          .getOrElse(throw new IllegalArgumentException("Empty or missing CSV header"))
        val headerColumns = CSVUtils.makeSafeHeader(
          parseCsvLine(spark, headerLine),
          spark.sessionState.conf.caseSensitiveAnalysis,
          csvOptions
        )
        headerColumns.map(_.toLowerCase).mkString("|")
      }
      Right(signature)
    } catch {
      case NonFatal(e) =>
        Left(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
    }
  }

  private[privyspark] def detectCsvHasHeader(
    spark: SparkSession,
    filePath: String
  ): Boolean = {
    val lines = readFirstNonBlankCsvLines(spark, filePath, maxLines = 2)
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

  private[privyspark] def inferCsvSchemaSignature(
    spark: SparkSession,
    filePath: String
  ): Either[String, (String, Boolean)] = {
    try {
      val csvHasHeader = detectCsvHasHeader(spark, filePath)
      if (csvHasHeader) {
        inferCsvHeaderSignature(spark, filePath).map(signature => (signature, true))
      } else {
        val firstDataLine = readFirstNonBlankCsvLines(spark, filePath, maxLines = 1).headOption
          .getOrElse(throw new IllegalArgumentException("Empty CSV file"))
        val columnCount = parseCsvLine(spark, firstDataLine).length
        Right((s"cols:$columnCount", false))
      }
    } catch {
      case NonFatal(e) =>
        Left(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
    }
  }

  private[privyspark] def inferSchemaSignature(
    spark: SparkSession,
    format: String,
    filePath: String
  ): Either[String, String] = {
    try {
      val schemaSignature = withFileReadRetry(spark, Seq(filePath), "schema_detection") {
        val schema = readSchemaSource(spark, format, filePath).schema
        val normalizedFieldNames = schema.fieldNames.map(_.toLowerCase)
        if (format == "csv") {
          // CSV는 헤더 순서가 데이터 매핑에 직접 영향을 주므로 순서를 유지한다.
          normalizedFieldNames.mkString("|")
        } else {
          normalizedFieldNames.sorted.mkString("|")
        }
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

  private def newJsonCorruptRecordColumnName(): String = {
    s"${FileIdentifierColumn}_json_corrupt_${UUID.randomUUID().toString.replace("-", "")}"
  }

  private def ensureReadableSourceColumns(
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

  private def readSchemaSource(
    spark: SparkSession,
    format: String,
    filePath: String,
    csvHasHeader: Boolean = true
  ): DataFrame = {
    logDebug("read_schema_source_start", "format" -> format, "file" -> filePath)
    val (df, internalCorruptRecordColumnName) = format match {
      case "csv" =>
        (
          spark.read
            .option("header", csvHasHeader.toString)
            .option("inferSchema", "false")
            .option("mode", "PERMISSIVE")
            .csv(filePath),
          None
        )
      case "json" =>
        val corruptRecordColumnName = newJsonCorruptRecordColumnName()
        (
          spark.read
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", corruptRecordColumnName)
            .json(filePath),
          Some(corruptRecordColumnName)
        )
      case "parquet" =>
        (spark.read.parquet(filePath), None)
      case "orc" =>
        (spark.read.orc(filePath), None)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported format: $format")
    }
    ensureReadableSourceColumns(format, Seq(filePath), df, internalCorruptRecordColumnName)
  }

  private[privyspark] def scanGroup(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String
  ): (Seq[ScanResult], Seq[ScanError]) = {
    logDebug(
      "group_scan_start",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "files" -> group.filePaths.size,
      "sample_ratio" -> sampleRatio,
      "use_directory_identifier" -> group.useDirectoryIdentifier,
      "schema_sampled" -> group.schemaSampled,
      "csv_has_header" -> group.csvHasHeader
    )
    if (group.schemaSampled && group.filePaths.size > 1) {
      val exactSplitResult = rescanSampledGroupWithExactSplit(
        spark,
        datasetPath,
        group,
        rules,
        sampleRatio,
        timestamp,
        "sampled_exact_split"
      )
      logDebug(
        "group_scan_complete",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "schema" -> group.schemaSignature,
        "result_rows" -> exactSplitResult._1.size,
        "error_rows" -> exactSplitResult._2.size,
        "mode" -> "sampled_exact_split"
      )
      return exactSplitResult
    }

    try {
      val results = scanGroupBatch(spark, datasetPath, group, rules, sampleRatio, timestamp)
      logDebug(
        "group_scan_complete",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "schema" -> group.schemaSignature,
        "result_rows" -> results.size,
        "error_rows" -> 0,
        "mode" -> "group_batch_scan"
      )
      (results, Seq.empty)
    } catch {
      case NonFatal(e) =>
        logDriver(
          s"group_scan_fallback directory=${group.directoryPath} format=${group.format} schema=${group.schemaSignature} files=${group.filePaths.size} reason=${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}"
        )
        val errorMessage = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        logDebug(
          "group_scan_fallback_requested",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "files" -> group.filePaths.size,
          "reason" -> errorMessage
        )
        if (group.schemaSampled) {
          logDriver(
            s"group_scan_fallback_execute directory=${group.directoryPath} format=${group.format} schema=${group.schemaSignature} files=${group.filePaths.size} mode=schema_resplit"
          )
          val exactSplitResult = rescanSampledGroupWithExactSplit(
            spark,
            datasetPath,
            group,
            rules,
            sampleRatio,
            timestamp,
            "fallback_schema_resplit"
          )

          logDebug(
            "group_scan_complete",
            "directory" -> group.directoryPath,
            "format" -> group.format,
            "schema" -> group.schemaSignature,
            "result_rows" -> exactSplitResult._1.size,
            "error_rows" -> exactSplitResult._2.size,
            "mode" -> "fallback_schema_resplit"
          )
          exactSplitResult
        } else {
          val fallbackResult = scanGroupByFile(spark, datasetPath, group, rules, sampleRatio, timestamp)
          logDebug(
            "group_scan_complete",
            "directory" -> group.directoryPath,
            "format" -> group.format,
            "schema" -> group.schemaSignature,
            "result_rows" -> fallbackResult._1.size,
            "error_rows" -> fallbackResult._2.size,
            "mode" -> "fallback_file_scan"
          )
          fallbackResult
        }
    }
  }

  private[privyspark] def scanGroupByFile(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    fileParallelism: Int = -1
  ): (Seq[ScanResult], Seq[ScanError]) = {
    logDriver(
      s"group_scan_fallback_execute directory=${group.directoryPath} format=${group.format} schema=${group.schemaSignature} files=${group.filePaths.size} mode=file_scan"
    )
    val parallelism = if (fileParallelism > 0) {
      resolveParallelism(group.filePaths.size, fileParallelism)
    } else {
      resolveFileParallelism(spark, group.filePaths.size)
    }
    logDebug(
      "group_scan_fallback_execute",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "files" -> group.filePaths.size,
      "use_directory_identifier" -> group.useDirectoryIdentifier,
      "parallelism" -> parallelism
    )
    val successfulFileMetrics = ArrayBuffer.empty[FileScanMetrics]
    val fallbackErrors = ArrayBuffer.empty[ScanError]
    executeInParallel(parallelism, group.filePaths.map { filePath =>
      () => {
        logDebug("group_scan_fallback_file_start", "file" -> filePath, "directory" -> group.directoryPath)
        val csvHasHeaderOverride =
          if (group.format == "csv" && group.schemaSampled) None else Some(group.csvHasHeader)
        filePath -> scanFileMetrics(spark, datasetPath, filePath, rules, sampleRatio, timestamp, csvHasHeaderOverride)
      }
    }).foreach {
      case (filePath, fileResult) =>
        fileResult match {
        case Right(fileMetrics) =>
          successfulFileMetrics += fileMetrics
          logDebug(
            "group_scan_fallback_file_success",
            "file" -> filePath,
            "file_identifier" -> fileMetrics.fileIdentifier,
            "sampled_rows" -> fileMetrics.sampledRowCount,
            "matches" -> fileMetrics.matchCounts.size
          )
        case Left(error) =>
          fallbackErrors += error
          logDebug(
            "group_scan_fallback_file_error",
            "file" -> filePath,
            "file_identifier" -> error.file_identifier,
            "reason" -> error.error_message
          )
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
        logDebug(
          "group_scan_partial_results",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "failed_files" -> fallbackErrors.size
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

    logDebug(
      "group_scan_fallback_complete",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "successful_files" -> successfulFileMetrics.size,
      "failed_files" -> fallbackErrors.size,
      "result_rows" -> fallbackResults.size
    )
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
    logDebug(
      "group_scan_batch_start",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "files" -> group.filePaths.size,
      "sample_ratio" -> sampleRatio,
      "use_directory_identifier" -> group.useDirectoryIdentifier
    )
    withFileReadRetry(spark, group.filePaths, "group_batch_scan") {
      val baseDf = readSource(spark, group.format, group.filePaths, group.csvHasHeader)
      val fileIdentifierColumn = if (group.useDirectoryIdentifier) {
        None
      } else {
        Some(resolveFileIdentifierColumn(baseDf.columns.toSeq))
      }
      val sourceDf = fileIdentifierColumn match {
        case Some(columnName) => baseDf.withColumn(columnName, input_file_name())
        case None => baseDf
      }
      logDebug(
        "group_scan_batch_source_ready",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "columns" -> sourceDf.columns.length,
        "file_identifier_mode" -> fileIdentifierColumn.fold("directory")(identity)
      )

      val sampledDf = if (sampleRatio >= 1.0) sourceDf else sourceDf.sample(withReplacement = false, sampleRatio)

      sampledDf.cache()
      try {
        fileIdentifierColumn match {
          case None =>
            val sampledRowCount = sampledDf.count()
            logDebug(
              "group_scan_batch_sampled_rows",
              "directory" -> group.directoryPath,
              "sampled_rows" -> sampledRowCount,
              "mode" -> "directory_identifier"
            )
            if (sampledRowCount == 0L) {
              logDebug(
                "group_scan_batch_complete",
                "directory" -> group.directoryPath,
                "result_rows" -> 0,
                "mode" -> "directory_identifier"
              )
              Seq.empty
            } else {
              val results = buildScanResults(
                datasetPath,
                timestamp,
                resolveDirectoryIdentifier(datasetPath, group.directoryPath),
                sampledRowCount,
                DetectionAggregator.aggregate(sampledDf, rules)
              )
              logDebug(
                "group_scan_batch_complete",
                "directory" -> group.directoryPath,
                "result_rows" -> results.size,
                "mode" -> "directory_identifier"
              )
              results
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
            logDebug(
              "group_scan_batch_sampled_file_rows",
              "directory" -> group.directoryPath,
              "files_with_rows" -> sampledRowsByFile.size,
              "mode" -> "file_identifier"
            )

            if (sampledRowsByFile.isEmpty) {
              logDebug(
                "group_scan_batch_complete",
                "directory" -> group.directoryPath,
                "result_rows" -> 0,
                "mode" -> "file_identifier"
              )
              Seq.empty
            } else {
              val results = DetectionAggregator.aggregateByFile(sampledDf, columnName, rules).flatMap { matchCount =>
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
              logDebug(
                "group_scan_batch_complete",
                "directory" -> group.directoryPath,
                "result_rows" -> results.size,
                "mode" -> "file_identifier"
              )
              results
            }
        }
      } finally {
        sampledDf.unpersist(blocking = false)
      }
    }
  }

  private def scanFileMetrics(
    spark: SparkSession,
    datasetPath: String,
    filePath: String,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    csvHasHeaderOverride: Option[Boolean] = None
  ): Either[ScanError, FileScanMetrics] = {
    val fileIdentifier = resolveRelativeIdentifier(datasetPath, filePath)
    logDebug("scan_file_start", "file" -> filePath, "file_identifier" -> fileIdentifier, "sample_ratio" -> sampleRatio)

    try {
      withFileReadRetry(spark, Seq(filePath), "file_scan") {
        val format = FormatDetector.infer(filePath).getOrElse {
          logDebug("scan_file_error", "file" -> filePath, "file_identifier" -> fileIdentifier, "reason" -> "Unsupported file format")
          return Left(ScanError(datasetPath, timestamp, fileIdentifier, s"Unsupported file format: $filePath"))
        }

        val csvHasHeader = if (format == "csv") {
          csvHasHeaderOverride.getOrElse(detectCsvHasHeader(spark, filePath))
        } else {
          true
        }
        val sourceDf = readSource(spark, format, Seq(filePath), csvHasHeader)
        val sampledDf = if (sampleRatio >= 1.0) sourceDf else sourceDf.sample(withReplacement = false, sampleRatio)

        sampledDf.cache()
        try {
          val sampledRowCount = sampledDf.count()
          logDebug(
            "scan_file_sampled_rows",
            "file" -> filePath,
            "file_identifier" -> fileIdentifier,
            "sampled_rows" -> sampledRowCount
          )

          if (sampledRowCount == 0L) {
            logDebug("scan_file_complete", "file" -> filePath, "file_identifier" -> fileIdentifier, "matches" -> 0)
            Right(FileScanMetrics(fileIdentifier, sampledRowCount, Seq.empty))
          } else {
            val matchCounts = DetectionAggregator.aggregate(sampledDf, rules)
            logDebug(
              "scan_file_complete",
              "file" -> filePath,
              "file_identifier" -> fileIdentifier,
              "matches" -> matchCounts.size
            )
            Right(FileScanMetrics(fileIdentifier, sampledRowCount, matchCounts))
          }
        } finally {
          sampledDf.unpersist(blocking = false)
        }
      }
    } catch {
      case NonFatal(e) =>
        val errorMessage = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        logDebug("scan_file_error", "file" -> filePath, "file_identifier" -> fileIdentifier, "reason" -> errorMessage)
        Left(ScanError(datasetPath, timestamp, fileIdentifier, errorMessage))
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

  private def readSource(
    spark: SparkSession,
    format: String,
    filePaths: Seq[String],
    csvHasHeader: Boolean = true
  ): DataFrame = {
    require(filePaths.nonEmpty, "filePaths must not be empty")
    logDebug("read_source_start", "format" -> format, "files" -> filePaths.size, "first_file" -> filePaths.head)

    val (df, internalCorruptRecordColumnName) = format match {
      case "csv" =>
        (
          spark.read
            .option("header", csvHasHeader.toString)
            .option("inferSchema", "false")
            .option("mode", "PERMISSIVE")
            .csv(filePaths: _*),
          None
        )
      case "json" =>
        val corruptRecordColumnName = newJsonCorruptRecordColumnName()
        (
          spark.read
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", corruptRecordColumnName)
            .json(filePaths: _*),
          Some(corruptRecordColumnName)
        )
      case "parquet" =>
        (spark.read.parquet(filePaths: _*), None)
      case "orc" =>
        (spark.read.orc(filePaths: _*), None)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported format: $format")
    }
    ensureReadableSourceColumns(format, filePaths, df, internalCorruptRecordColumnName)
  }

  private def createCsvOptions(spark: SparkSession): CSVOptions = {
    new CSVOptions(
      scala.collection.immutable.Map("header" -> "true", "inferSchema" -> "false"),
      false,
      spark.sessionState.conf.sessionLocalTimeZone,
      spark.sessionState.conf.columnNameOfCorruptRecord
    )
  }

  private def parseCsvLine(spark: SparkSession, line: String): Array[String] = {
    val parser = new CsvParser(createCsvOptions(spark).asParserSettings)
    Option(parser.parseLine(Option(line).getOrElse("").stripPrefix("\uFEFF"))).getOrElse(Array.empty[String])
  }

  private def readFirstNonBlankCsvLines(
    spark: SparkSession,
    filePath: String,
    maxLines: Int
  ): Seq[String] = {
    withFileReadRetry(spark, Seq(filePath), "csv_line_sample") {
      val path = new Path(filePath)
      val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
      val reader = new BufferedReader(new InputStreamReader(fs.open(path), StandardCharsets.UTF_8))
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

  private def rescanSampledGroupWithExactSplit(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    mode: String
  ): (Seq[ScanResult], Seq[ScanError]) = {
    val (splitGroups, splitErrors) = splitGroupBySchema(
      spark,
      datasetPath,
      timestamp,
      group.copy(schemaSampled = false)
    )
    val exactSplitCanUseDirectoryIdentifier =
      group.directoryIdentifierEligible &&
        splitGroups.size == 1 &&
        splitErrors.isEmpty &&
        splitGroups.head.filePaths.size > 1
    val rescannedGroups = splitGroups.map(_.copy(
      useDirectoryIdentifier = exactSplitCanUseDirectoryIdentifier,
      directoryIdentifierEligible = group.directoryIdentifierEligible,
      schemaSampled = false
    ))

    val rescannedResults = ArrayBuffer.empty[ScanResult]
    val rescannedErrors = ArrayBuffer.empty[ScanError] ++ splitErrors
    rescannedGroups.foreach { rescannedGroup =>
      val (groupResults, groupErrors) = scanGroup(
        spark,
        datasetPath,
        rescannedGroup,
        rules,
        sampleRatio,
        timestamp
      )
      rescannedResults ++= groupResults
      rescannedErrors ++= groupErrors
    }

    logDebug(
      "group_scan_exact_split_complete",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "split_groups" -> splitGroups.size,
      "split_errors" -> splitErrors.size,
      "use_directory_identifier" -> exactSplitCanUseDirectoryIdentifier,
      "mode" -> mode
    )
    (rescannedResults.toSeq, rescannedErrors.toSeq)
  }

  private[privyspark] def writeReports(
    spark: SparkSession,
    outputRoot: String,
    results: Seq[ScanResult],
    errors: Seq[ScanError]
  ): Unit = {
    import spark.implicits._

    val root = outputRoot.stripSuffix("/")
    logDebug("write_reports_materialize", "output_root" -> root, "results" -> results.size, "errors" -> errors.size)
    val resultDf = spark.createDataset(results).toDF().coalesce(1).cache()
    val errorDf = spark.createDataset(errors).toDF().coalesce(1).cache()

    val resultParquetPath = s"$root/parquet/scan_results"
    val errorParquetPath = s"$root/parquet/scan_errors"
    val resultCsvPath = s"$root/csv/scan_results"
    val errorCsvPath = s"$root/csv/scan_errors"

    try {
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
    } finally {
      resultDf.unpersist(blocking = false)
      errorDf.unpersist(blocking = false)
    }

    logDebug(
      "write_reports_complete",
      "output_root" -> root,
      "result_parquet_path" -> resultParquetPath,
      "error_parquet_path" -> errorParquetPath,
      "result_csv_path" -> resultCsvPath,
      "error_csv_path" -> errorCsvPath
    )
  }
}
