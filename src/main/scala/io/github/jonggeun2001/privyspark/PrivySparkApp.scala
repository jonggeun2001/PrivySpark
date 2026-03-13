package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.DetectionAggregator.MatchCount
import io.github.jonggeun2001.privyspark.config.RulesetLoader
import io.github.jonggeun2001.privyspark.model.{PiiRule, ScanError, ScanResult}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.StructType
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
    useDirectoryIdentifier: Boolean = false,
    expectedSchema: Option[StructType] = None
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
    matchCounts: Seq[MatchCount],
    schemaSignature: String
  )

  private val FileIdentifierColumn = "__privyspark_file_identifier"
  private[privyspark] val MaxFilesPerGroupBatchScan = 1000
  private val DebugPropertyName = "privyspark.debug"
  private val DebugEnvName = "PRIVYSPARK_DEBUG"

  private def logDriver(message: String): Unit = {
    System.err.println(s"[PrivySpark] $message")
  }

  private def isDebugLoggingEnabled: Boolean = {
    val rawValue = sys.props.get(DebugPropertyName).orElse(sys.env.get(DebugEnvName))
    rawValue.exists { value =>
      value.trim.toLowerCase match {
        case "1" | "true" | "yes" | "on" => true
        case _ => false
      }
    }
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

    scanPlan.groups.foreach { group =>
      logDebug(
        "group_scan_dispatch",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "schema" -> group.schemaSignature,
        "files" -> group.filePaths.size,
        "use_directory_identifier" -> group.useDirectoryIdentifier
      )
      val (groupResults, groupErrors) =
        scanGroup(spark, config.inputPath, group, rules, config.sampleRatio, timestamp)
      results ++= groupResults
      errors ++= groupErrors
      logDebug(
        "group_scan_recorded",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "schema" -> group.schemaSignature,
        "result_rows" -> groupResults.size,
        "error_rows" -> groupErrors.size
      )
    }

    logDebug("report_write_start", "results" -> results.size, "errors" -> errors.size, "output_root" -> config.outputPath)
    writeReports(spark, config.outputPath, results.toSeq, errors.toSeq)
    logDebug("report_write_complete", "results" -> results.size, "errors" -> errors.size, "output_root" -> config.outputPath)

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
      val (resolvedGroup, probeErrors) = resolveGroupSchema(spark, datasetPath, timestamp, group)
      resolvedGroup.foreach(schemaAwareGroups += _)
      errors ++= probeErrors
      logDebug(
        "scan_directory_group_schema_probe",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "input_files" -> group.filePaths.size,
        "resolved_group" -> resolvedGroup.isDefined,
        "probe_errors" -> probeErrors.size
      )
      if (probeErrors.nonEmpty) {
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
      val finalizedGroup = group.copy(useDirectoryIdentifier = useDirectoryIdentifier)
      logDebug(
        "scan_group_planned",
        "directory" -> finalizedGroup.directoryPath,
        "format" -> finalizedGroup.format,
        "schema" -> finalizedGroup.schemaSignature,
        "files" -> finalizedGroup.filePaths.size,
        "use_directory_identifier" -> finalizedGroup.useDirectoryIdentifier
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

  private def resolveGroupSchema(
    spark: SparkSession,
    datasetPath: String,
    timestamp: String,
    group: ScanGroup
  ): (Option[ScanGroup], Seq[ScanError]) = {
    logDebug(
      "scan_group_schema_probe_start",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "files" -> group.filePaths.size
    )
    val errors = ArrayBuffer.empty[ScanError]
    var resolvedGroup: Option[ScanGroup] = None
    var index = 0

    while (index < group.filePaths.size && resolvedGroup.isEmpty) {
      val filePath = group.filePaths(index)
      inferSchemaMetadata(spark, group.format, filePath) match {
        case Right((schemaSignature, expectedSchema)) =>
          resolvedGroup = Some(
            group.copy(
              schemaSignature = schemaSignature,
              filePaths = group.filePaths.drop(index),
              expectedSchema = Some(expectedSchema)
            )
          )
          logDebug(
            "group_schema_probe_resolved",
            "directory" -> group.directoryPath,
            "file" -> filePath,
            "format" -> group.format,
            "schema" -> schemaSignature
          )
        case Left(errorMessage) =>
          logDebug(
            "group_schema_probe_failed",
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
      index += 1
    }

    logDebug(
      "scan_group_schema_probe_complete",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "resolved_group" -> resolvedGroup.isDefined,
      "errors" -> errors.size
    )
    (resolvedGroup, errors.toSeq)
  }

  private def inferSchemaMetadata(
    spark: SparkSession,
    format: String,
    filePath: String
  ): Either[String, (String, StructType)] = {
    try {
      val schema = readSchemaSource(spark, format, filePath).schema
      Right(schemaSignatureForSchema(format, schema) -> schema)
    } catch {
      case NonFatal(e) =>
        Left(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
    }
  }

  private def schemaSignatureForSchema(format: String, schema: StructType): String = {
    val normalizedFieldNames = schema.fieldNames.map(_.toLowerCase)
    if (format == "csv") {
      // CSV는 헤더 순서가 데이터 매핑에 직접 영향을 주므로 순서를 유지한다.
      normalizedFieldNames.mkString("|")
    } else {
      normalizedFieldNames.sorted.mkString("|")
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
    logDebug("read_schema_source_start", "format" -> format, "file" -> filePath)
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

  private def verifyBatchSchemaConsistency(spark: SparkSession, group: ScanGroup): Unit = {
    if (group.format == "csv" || group.filePaths.size <= 1) {
      return
    }

    logDebug(
      "group_scan_batch_schema_verify_start",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "files_to_verify" -> (group.filePaths.size - 1)
    )

    group.filePaths.tail.foreach { filePath =>
      inferSchemaMetadata(spark, group.format, filePath) match {
        case Right((schemaSignature, _)) if schemaSignature != group.schemaSignature =>
          throw new IllegalStateException(
            s"Schema mismatch detected for $filePath: expected=${group.schemaSignature} actual=$schemaSignature"
          )
        case Left(errorMessage) =>
          throw new IllegalStateException(s"Schema verification failed for $filePath: $errorMessage")
        case _ =>
      }
    }

    logDebug(
      "group_scan_batch_schema_verify_complete",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "files_verified" -> (group.filePaths.size - 1)
    )
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
    logDebug(
      "group_scan_start",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "files" -> group.filePaths.size,
      "sample_ratio" -> sampleRatio,
      "use_directory_identifier" -> group.useDirectoryIdentifier
    )
    if (group.filePaths.size > maxFilesPerGroupBatchScan) {
      logDriver(
        s"group_scan_fallback directory=${group.directoryPath} format=${group.format} files=${group.filePaths.size} reason=group_size_limit_exceeded($maxFilesPerGroupBatchScan)"
      )
      logDebug(
        "group_scan_fallback_requested",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "schema" -> group.schemaSignature,
        "files" -> group.filePaths.size,
        "reason" -> s"group_size_limit_exceeded($maxFilesPerGroupBatchScan)"
      )
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
      return fallbackResult
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
    logDebug(
      "group_scan_fallback_execute",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "files" -> group.filePaths.size,
      "use_directory_identifier" -> group.useDirectoryIdentifier
    )
    val successfulFileMetrics = ArrayBuffer.empty[FileScanMetrics]
    val fallbackErrors = ArrayBuffer.empty[ScanError]
    group.filePaths.foreach { filePath =>
      logDebug("group_scan_fallback_file_start", "file" -> filePath, "directory" -> group.directoryPath)
      scanFileMetrics(spark, datasetPath, filePath, rules, sampleRatio, timestamp) match {
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

    val preserveDirectoryIdentifier =
      group.useDirectoryIdentifier &&
        fallbackErrors.isEmpty &&
        successfulFileMetrics.nonEmpty &&
        successfulFileMetrics.forall(_.schemaSignature == group.schemaSignature)

    val fallbackResults = if (preserveDirectoryIdentifier) {
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
      } else if (group.useDirectoryIdentifier && successfulFileMetrics.nonEmpty) {
        val actualSchemas = successfulFileMetrics.map(_.schemaSignature).distinct.sorted.mkString(",")
        logDriver(
          s"group_scan_schema_divergence directory=${group.directoryPath} format=${group.format} expected_schema=${group.schemaSignature} actual_schemas=$actualSchemas mode=file_identifier_preserved"
        )
        logDebug(
          "group_scan_schema_divergence",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "expected_schema" -> group.schemaSignature,
          "actual_schemas" -> actualSchemas
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
      "result_rows" -> fallbackResults.size,
      "directory_identifier_preserved" -> preserveDirectoryIdentifier
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
    val baseDf = readSource(spark, group.format, group.filePaths, group.expectedSchema)
    verifyBatchSchemaConsistency(spark, group)
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
    logDebug("scan_file_start", "file" -> filePath, "file_identifier" -> fileIdentifier, "sample_ratio" -> sampleRatio)

    try {
      val format = FormatDetector.infer(filePath).getOrElse {
        logDebug("scan_file_error", "file" -> filePath, "file_identifier" -> fileIdentifier, "reason" -> "Unsupported file format")
        return Left(ScanError(datasetPath, timestamp, fileIdentifier, s"Unsupported file format: $filePath"))
      }

      val sourceDf = readSource(spark, format, Seq(filePath))
      val schemaSignature = schemaSignatureForSchema(format, sourceDf.schema)
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
          Right(FileScanMetrics(fileIdentifier, sampledRowCount, Seq.empty, schemaSignature))
        } else {
          val matchCounts = DetectionAggregator.aggregate(sampledDf, rules)
          logDebug(
            "scan_file_complete",
            "file" -> filePath,
            "file_identifier" -> fileIdentifier,
            "matches" -> matchCounts.size
          )
          Right(FileScanMetrics(fileIdentifier, sampledRowCount, matchCounts, schemaSignature))
        }
      } finally {
        sampledDf.unpersist(blocking = true)
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
    expectedSchema: Option[StructType] = None
  ): DataFrame = {
    require(filePaths.nonEmpty, "filePaths must not be empty")
    logDebug("read_source_start", "format" -> format, "files" -> filePaths.size, "first_file" -> filePaths.head)

    format match {
      case "csv" =>
        val reader = spark.read
          .option("header", "true")
          .option("mode", "PERMISSIVE")

        expectedSchema match {
          case Some(schema) =>
            reader
              .option("inferSchema", "false")
              .option("enforceSchema", "false")
              .schema(schema)
              .csv(filePaths: _*)
          case None =>
            reader
              .option("inferSchema", "true")
              .csv(filePaths: _*)
        }
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
    logDebug("write_reports_materialize", "output_root" -> root, "results" -> results.size, "errors" -> errors.size)
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
