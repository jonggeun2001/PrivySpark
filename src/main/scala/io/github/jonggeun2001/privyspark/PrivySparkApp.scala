package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.cli.{Cli, CliCommand, CliConfig, PathValidator, ReviewApplyCliConfig, ReviewCollectCliConfig}
import io.github.jonggeun2001.privyspark.config.{IgnoreMatcher, RulesetLoader, SuppressionSet}
import io.github.jonggeun2001.privyspark.format.ExcelReadConfig
import io.github.jonggeun2001.privyspark.fsio.ManagedPaths.cleanupStagingPaths
import io.github.jonggeun2001.privyspark.hive.{HiveMetastoreJdbcConfig, HiveTableLookup}
import io.github.jonggeun2001.privyspark.model.{ProgressRun, ScanReadOptions, Suppression}
import io.github.jonggeun2001.privyspark.progress.ProgressIO.persistProgressRecords
import io.github.jonggeun2001.privyspark.progress.ProgressRunManager._
import io.github.jonggeun2001.privyspark.review.{AllowlistMatcher, ReviewApplyCommand, ReviewCollectCommand, ReviewHtmlWriter}
import io.github.jonggeun2001.privyspark.scan.{CsvHeadCache, DirectoryScanner, GroupScanner, ParseOkCache, SchemaSignatureCache}
import io.github.jonggeun2001.privyspark.util.ParallelismConfig.{renderConfiguredParallelism, resolveCliParallelism}
import io.github.jonggeun2001.privyspark.util.{DriverLogLevel, DriverLogger}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkEnv, SparkFiles}
import org.apache.spark.sql.SparkSession

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.Instant
import java.util.concurrent.ScheduledExecutorService
import scala.collection.mutable.ArrayBuffer
import scala.util.control.ControlThrowable
import scala.util.control.NonFatal

object PrivySparkApp {
  private[privyspark] final case class ParsedSuppression(suppression: Suppression, source: String)

  private[privyspark] def resetDebugCache(): Unit = {
    DriverLogger.resetCache()
  }

  def main(args: Array[String]): Unit = {
    runMain(args)
  }

  private[privyspark] def runMain(
    args: Array[String],
    createSparkSession: () => SparkSession = () => buildDefaultSparkSession(),
    exitWith: Int => Unit = code => System.exit(code),
    runScanCommand: (SparkSession, CliConfig) => Unit = runScan,
    runReviewApplyCommand: (SparkSession, ReviewApplyCliConfig) => Unit = ReviewApplyCommand.run,
    runReviewCollectCommand: (SparkSession, ReviewCollectCliConfig) => Unit = ReviewCollectCommand.run
  ): Unit = {
    val parseResult = Cli.parseWithErrors(args)
    val command = parseResult.command.getOrElse {
      DriverLogger.emitAlways(
        DriverLogLevel.Error,
        "cli_argument_invalid",
        "errors" -> parseResult.errors.mkString(" | "),
        "args" -> args.mkString(" ")
      )
      exitWith(2)
      return
    }

    command match {
      case CliCommand.Scan(config) =>
        if (!validateAbsoluteArgument("--path", config.inputPath, exitWith)) {
          return
        }
        if (!validateAbsoluteArgument("--output", config.outputPath, exitWith)) {
          return
        }
        if (config.allowlist.exists(path => !PathValidator.isAbsolute(path))) {
          emitAbsolutePathError("--allowlist", config.allowlist.get)
          exitWith(2)
          return
        }
        if (config.reviewStateRoot.exists(path => !PathValidator.isAbsolute(path))) {
          emitAbsolutePathError("--review-state-root", config.reviewStateRoot.get)
          exitWith(2)
          return
        }
        if (config.reviewHtmlDir.exists(path => !PathValidator.isAbsolute(path))) {
          emitAbsolutePathError("--review-html-dir", config.reviewHtmlDir.get)
          exitWith(2)
          return
        }
        if (config.hiveMetastorePasswordFile.exists(path => !PathValidator.isAbsolute(path))) {
          emitAbsolutePathError("--hive-metastore-password-file", config.hiveMetastorePasswordFile.get)
          exitWith(2)
          return
        }
      case CliCommand.ReviewApply(config) =>
        if (!validateAbsoluteArgument("--scan-results", config.scanResultsPath, exitWith)) {
          return
        }
        if (!validateAbsoluteArgument("--input-root", config.inputRoot, exitWith)) {
          return
        }
        if (!validateAbsoluteArgument("--allowlist", config.allowlistPath, exitWith)) {
          return
        }
      case CliCommand.ReviewCollect(config) =>
        if (!validateAbsoluteArgument("--scan-results", config.scanResultsPath, exitWith)) {
          return
        }
        if (!validateAbsoluteArgument("--review-state-root", config.reviewStateRoot, exitWith)) {
          return
        }
    }

    var spark: Option[SparkSession] = None

    try {
      val session = createSparkSession()
      spark = Some(session)
      session.sparkContext.setLogLevel("WARN")
      command match {
        case CliCommand.Scan(config) =>
          runScanCommand(session, config)
        case CliCommand.ReviewApply(config) =>
          runReviewApplyCommand(session, config)
        case CliCommand.ReviewCollect(config) =>
          runReviewCollectCommand(session, config)
      }
    } catch {
      case control: ControlThrowable =>
        throw control
      case NonFatal(e) =>
        DriverLogger.emitAlways(
          DriverLogLevel.Error,
          "scan_failed",
          "exception" -> e.getClass.getSimpleName,
          "reason" -> Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        )
        exitWith(1)
    } finally {
      spark.foreach(_.stop())
    }
  }

  private def validateAbsoluteArgument(argument: String, value: String, exitWith: Int => Unit): Boolean = {
    if (PathValidator.isAbsolute(value)) {
      true
    } else {
      emitAbsolutePathError(argument, value)
      exitWith(2)
      false
    }
  }

  private def emitAbsolutePathError(argument: String, value: String): Unit = {
    DriverLogger.emitAlways(
      DriverLogLevel.Error,
      "cli_argument_invalid",
      "argument" -> argument,
      "reason" -> "must_be_absolute_path_or_uri",
      "value" -> value
    )
  }

  private[privyspark] def buildDefaultSparkSession(): SparkSession = {
    SparkSession.builder().appName("PrivySpark").getOrCreate()
  }

  private def runScan(spark: SparkSession, config: CliConfig): Unit = {
    val (preScanParallelism, groupParallelism, fileParallelism) =
      resolveCliParallelism(config.preScanParallelism, config.groupParallelism, config.fileParallelism)
    val byteArrayMaxOverride = ExcelReadConfig.resolveByteArrayMaxOverride(
      spark.sparkContext.getConf,
      config.excelByteArrayMaxOverride
    )
    ExcelReadConfig.applyByteArrayMaxOverride(byteArrayMaxOverride)
    spark.conf.set(ExcelReadConfig.ByteArrayMaxOverrideConfKey, byteArrayMaxOverride.toString)
    spark.sparkContext.hadoopConfiguration.set(ExcelReadConfig.ByteArrayMaxOverrideConfKey, byteArrayMaxOverride.toString)
    warnUnusedExcelMaxRowsInMemory(config.excelMaxRowsInMemory)
    val outputFormats = config.effectiveOutputFormats
    val csvHeadCache = new CsvHeadCache()
    val schemaSigCache = new SchemaSignatureCache()
    val parseOkCache = new ParseOkCache()
    val ignoreMatcher = IgnoreMatcher.fromSources(
      spark.sparkContext.hadoopConfiguration,
      config.ignorePatterns,
      config.ignoreFile
    )
    val reviewStateAllowlist = config.reviewStateRoot.map(root => s"${root.stripSuffix("/")}/current/allowlist.jsonl")
    val allowlistMatcher = AllowlistMatcher.combine(Seq(
      config.allowlist
        .map(path => AllowlistMatcher.load(spark.sparkContext.hadoopConfiguration, path))
        .getOrElse(AllowlistMatcher.empty),
      reviewStateAllowlist
        .map(path => AllowlistMatcher.loadExisting(spark.sparkContext.hadoopConfiguration, path))
        .getOrElse(AllowlistMatcher.empty)
    ))

    DriverLogger.info(
      "scan_start",
      "input_path" -> config.inputPath,
      "output_path" -> config.outputPath,
      "ruleset" -> config.ruleset,
      "sample_ratio" -> config.sampleRatio,
      "file_sample_ratio" -> config.fileSampleRatio.getOrElse("none"),
      "file_sample_min_files" -> config.fileSampleMinFiles,
      "configured_pre_scan_parallelism" -> renderConfiguredParallelism(config.preScanParallelism),
      "configured_group_parallelism" -> renderConfiguredParallelism(config.groupParallelism),
      "configured_file_parallelism" -> renderConfiguredParallelism(config.fileParallelism),
      "configured_excel_max_rows_in_memory" -> ExcelReadConfig.renderConfiguredMaxRowsInMemory(config.excelMaxRowsInMemory),
      "configured_excel_byte_array_max_override" -> ExcelReadConfig.renderConfiguredByteArrayMaxOverride(config.excelByteArrayMaxOverride),
      "output_formats" -> outputFormats.mkString(","),
      "ignore_patterns" -> config.ignorePatterns.size,
      "ignore_file" -> config.ignoreFile.getOrElse("none"),
      "allowlist" -> config.allowlist.getOrElse("none"),
      "review_state_root" -> config.reviewStateRoot.getOrElse("none"),
      "review_html_dir" -> config.reviewHtmlDir.getOrElse("default"),
      "review_sample_mode" -> config.reviewSampleMode,
      "allowlist_entries" -> allowlistMatcher.size,
      "suppressions" -> config.suppressions.size,
      "suppression_file" -> config.suppressionFile.getOrElse("none"),
      "hive_metastore_jdbc_lookup" -> (if (config.hiveMetastoreJdbcUrl.nonEmpty) "configured" else "none"),
      "driver_log_level" -> DriverLogger.currentLogLevel.label.toLowerCase
    )

    val bundle = RulesetLoader.loadBundle(config.ruleset)
    val parsedCliSuppressions = parseCliSuppressionEntries(
      spark.sparkContext.hadoopConfiguration,
      config.suppressions,
      config.suppressionFile
    )
    warnUnknownSuppressions(parsedCliSuppressions, bundle.rules.map(_.piiType).toSet)
    val cliSuppressions = parsedCliSuppressions.map(_.suppression)
    val suppressions = SuppressionSet.from(bundle.suppressions).merge(SuppressionSet.from(cliSuppressions))
    val rules = bundle.rules
    val hiveMetastoreConfig = for {
      jdbcUrl <- config.hiveMetastoreJdbcUrl
      user <- config.hiveMetastoreUser
      passwordFile <- config.hiveMetastorePasswordFile
    } yield HiveMetastoreJdbcConfig(jdbcUrl, user, passwordFile)
    val hiveLookupBroadcast = HiveTableLookup.buildAndBroadcast(spark, hiveMetastoreConfig)
    DriverLogger.debug(
      "ruleset_loaded",
      "rules" -> rules.size,
      "ruleset_suppressions" -> bundle.suppressions.size,
      "cli_suppressions" -> cliSuppressions.size,
      "effective_suppressions" -> suppressions.size,
      "ruleset" -> config.ruleset
    )

    val timestamp = Instant.now().toString
    val scanPlan = DirectoryScanner.scanDirectoryStructure(
      spark,
      config.inputPath,
      config.inputPath,
      timestamp,
      preScanParallelism,
      ignoreMatcher = ignoreMatcher,
      csvHeadCache = csvHeadCache,
      schemaSigCache = schemaSigCache,
      parseOkCache = parseOkCache,
      readOptions = ScanReadOptions(
        excelMaxRowsInMemory = config.excelMaxRowsInMemory,
        excelByteArrayMaxOverride = Some(byteArrayMaxOverride)
      )
    )

    var progressRun: Option[ProgressRun] = None
    var heartbeatExecutor: Option[ScheduledExecutorService] = None

    try {
      DriverLogger.info(
        "scan_plan_ready",
        "groups" -> scanPlan.groups.size,
        "plan_errors" -> scanPlan.errors.size,
        "total_files" -> scanPlan.totalFiles,
        "directories" -> scanPlan.directoryCount,
        "ignored_files" -> scanPlan.ignoredFiles
      )

      val preparedProgressRun = prepareProgressRun(
        spark.sparkContext.hadoopConfiguration,
        config.outputPath,
        config.inputPath,
        timestamp
      )
      progressRun = Some(preparedProgressRun)
      heartbeatExecutor = Some(startProgressHeartbeat(spark.sparkContext.hadoopConfiguration, preparedProgressRun))

      if (scanPlan.errors.nonEmpty) {
        persistProgressRecords(
          spark.sparkContext.hadoopConfiguration,
          preparedProgressRun,
          "plan",
          config.inputPath,
          Seq.empty,
          scanPlan.errors
        )
      }

      GroupScanner.scanGroups(
        spark,
        config.inputPath,
        scanPlan.groups,
        rules,
        config.sampleRatio,
        timestamp,
        groupParallelism,
        fileParallelism,
        config.fileSampleRatio,
        config.fileSampleMinFiles,
        suppressions,
        allowlistMatcher,
        Some(config.inputPath),
        Some(preparedProgressRun),
        retainPayloads = false,
        csvHeadCache = csvHeadCache,
        hiveLookup = Some(hiveLookupBroadcast)
      )

      DriverLogger.debug(
        "report_write_start",
        "output_root" -> config.outputPath,
        "progress_run" -> preparedProgressRun.runId,
        "output_formats" -> outputFormats.mkString(",")
      )
      val (resultCount, errorCount) = mergeProgressReports(
        spark,
        config.outputPath,
        preparedProgressRun,
        outputFormats,
        resultDf => {
          if (config.reviewStateRoot.nonEmpty) {
            ReviewHtmlWriter.write(
              spark.sparkContext.hadoopConfiguration,
              config.outputPath,
              config.inputPath,
              resultDf,
              config.reviewSampleMode,
              config.reviewHtmlDir
            )
          }
        }
      )
      DriverLogger.debug(
        "report_write_complete",
        "results" -> resultCount,
        "errors" -> errorCount,
        "output_root" -> config.outputPath,
        "output_formats" -> outputFormats.mkString(",")
      )
      DriverLogger.info(
        "scan_complete",
        "scanned_files" -> scanPlan.totalFiles,
        "ignored_files" -> scanPlan.ignoredFiles,
        "grouped_dirs" -> scanPlan.directoryCount,
        "groups" -> scanPlan.groups.size,
        "detections" -> resultCount,
        "errors" -> errorCount,
        "output_root" -> config.outputPath,
        "output_formats" -> outputFormats.mkString(",")
      )

      println(
        s"[PrivySpark] scanned_files=${scanPlan.totalFiles}, ignored_files=${scanPlan.ignoredFiles}, grouped_dirs=${scanPlan.directoryCount}, groups=${scanPlan.groups.size}, detections=$resultCount, errors=$errorCount"
      )
    } catch {
      case NonFatal(e) =>
        heartbeatExecutor.foreach(stopProgressHeartbeat)
        progressRun.foreach { run =>
          markProgressRunFailed(
            spark.sparkContext.hadoopConfiguration,
            run,
            Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
          )
        }
        throw e
    } finally {
      heartbeatExecutor.foreach(stopProgressHeartbeat)
      csvHeadCache.clear()
      schemaSigCache.clear()
      parseOkCache.clear()
      cleanupStagingPaths(spark.sparkContext.hadoopConfiguration, scanPlan.stagingPaths)
    }
  }

  private[privyspark] def warnUnusedExcelMaxRowsInMemory(configured: Option[Int]): Unit = {
    configured.foreach { value =>
      DriverLogger.warn(
        "excel_max_rows_in_memory_unused",
        "argument" -> "--excel-max-rows-in-memory",
        "value" -> value,
        "reason" -> "executor_side_xlsx_scan"
      )
    }
  }

  private[privyspark] def parseCliSuppressions(
    conf: Configuration,
    inline: Seq[String],
    file: Option[String]
  ): Seq[Suppression] = {
    parseCliSuppressionEntries(conf, inline, file).map(_.suppression)
  }

  private def parseCliSuppressionEntries(
    conf: Configuration,
    inline: Seq[String],
    file: Option[String]
  ): Seq[ParsedSuppression] = {
    inline.zipWithIndex.map {
      case (rawValue, index) => parseSuppressionSpec(rawValue, s"cli:${index + 1}")
    } ++ file.toSeq.flatMap(loadSuppressionFile(conf, _))
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

  private[privyspark] def warnUnknownSuppressions(
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
}
