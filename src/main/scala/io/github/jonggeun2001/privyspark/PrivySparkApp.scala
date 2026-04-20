package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.cli.{Cli, CliCommand, CliConfig, PathValidator, ReviewApplyCliConfig}
import io.github.jonggeun2001.privyspark.fsio.ManagedPaths.cleanupStagingPaths
import io.github.jonggeun2001.privyspark.scan.{CsvHeadCache, DirectoryScanner, GroupScanner, ParseOkCache, SchemaSignatureCache}
import io.github.jonggeun2001.privyspark.util.ParallelismConfig.{renderConfiguredParallelism, resolveCliParallelism}
import io.github.jonggeun2001.privyspark.util.{DriverLogLevel, DriverLogger}
import io.github.jonggeun2001.privyspark.config.IgnoreMatcher
import io.github.jonggeun2001.privyspark.config.RulesetLoader
import io.github.jonggeun2001.privyspark.model.ProgressRun
import io.github.jonggeun2001.privyspark.progress.ProgressIO.persistProgressRecords
import io.github.jonggeun2001.privyspark.progress.ProgressRunManager._
import io.github.jonggeun2001.privyspark.review.{AllowlistMatcher, ReviewApplyCommand}
import org.apache.spark.sql.SparkSession

import java.time.Instant
import java.util.concurrent.ScheduledExecutorService
import scala.util.control.ControlThrowable
import scala.util.control.NonFatal

object PrivySparkApp {
  private[privyspark] def resetDebugCache(): Unit = {
    DriverLogger.resetCache()
  }

  def main(args: Array[String]): Unit = {
    runMain(args)
  }

  private[privyspark] def runMain(
    args: Array[String],
    createSparkSession: () => SparkSession = () => SparkSession.builder().appName("PrivySpark").getOrCreate(),
    exitWith: Int => Unit = code => System.exit(code),
    runScanCommand: (SparkSession, CliConfig) => Unit = runScan,
    runReviewApplyCommand: (SparkSession, ReviewApplyCliConfig) => Unit = ReviewApplyCommand.run
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

  private def runScan(spark: SparkSession, config: CliConfig): Unit = {
    val (preScanParallelism, groupParallelism, fileParallelism) =
      resolveCliParallelism(config.preScanParallelism, config.groupParallelism, config.fileParallelism)
    val outputFormats = config.effectiveOutputFormats
    val csvHeadCache = new CsvHeadCache()
    val schemaSigCache = new SchemaSignatureCache()
    val parseOkCache = new ParseOkCache()
    val ignoreMatcher = IgnoreMatcher.fromSources(
      spark.sparkContext.hadoopConfiguration,
      config.ignorePatterns,
      config.ignoreFile
    )
    val allowlistMatcher = config.allowlist
      .map(path => AllowlistMatcher.load(spark.sparkContext.hadoopConfiguration, path))
      .getOrElse(AllowlistMatcher.empty)

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
      "output_formats" -> outputFormats.mkString(","),
      "ignore_patterns" -> config.ignorePatterns.size,
      "ignore_file" -> config.ignoreFile.getOrElse("none"),
      "allowlist" -> config.allowlist.getOrElse("none"),
      "allowlist_entries" -> allowlistMatcher.size,
      "driver_log_level" -> DriverLogger.currentLogLevel.label.toLowerCase
    )

    val rules = RulesetLoader.load(config.ruleset)
    DriverLogger.debug("ruleset_loaded", "rules" -> rules.size, "ruleset" -> config.ruleset)

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
      parseOkCache = parseOkCache
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
        allowlistMatcher,
        Some(config.inputPath),
        Some(preparedProgressRun),
        retainPayloads = false,
        csvHeadCache = csvHeadCache
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
        outputFormats
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
}
