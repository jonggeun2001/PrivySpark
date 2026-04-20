package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.cli.{Cli, CliConfig, PathValidator}
import io.github.jonggeun2001.privyspark.config.{IgnoreMatcher, RulesetLoader, SuppressionSet}
import io.github.jonggeun2001.privyspark.fsio.ManagedPaths.cleanupStagingPaths
import io.github.jonggeun2001.privyspark.model.{ProgressRun, Suppression}
import io.github.jonggeun2001.privyspark.scan.{CsvHeadCache, DirectoryScanner, GroupScanner, ParseOkCache, SchemaSignatureCache}
import io.github.jonggeun2001.privyspark.util.ParallelismConfig.{renderConfiguredParallelism, resolveCliParallelism}
import io.github.jonggeun2001.privyspark.util.{DriverLogLevel, DriverLogger}
import io.github.jonggeun2001.privyspark.progress.ProgressIO.persistProgressRecords
import io.github.jonggeun2001.privyspark.progress.ProgressRunManager._
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
  private[privyspark] def resetDebugCache(): Unit = {
    DriverLogger.resetCache()
  }

  def main(args: Array[String]): Unit = {
    runMain(args)
  }

  private[privyspark] def runMain(
    args: Array[String],
    createSparkSession: () => SparkSession = () => SparkSession.builder().appName("PrivySpark").getOrCreate(),
    exitWith: Int => Unit = code => System.exit(code)
  ): Unit = {
    val normalizedArgs = if (args.headOption.contains("scan")) args.drop(1) else args

    val parseResult = Cli.parseWithErrors(normalizedArgs)
    val config = parseResult.config.getOrElse {
      DriverLogger.emitAlways(
        DriverLogLevel.Error,
        "cli_argument_invalid",
        "errors" -> parseResult.errors.mkString(" | "),
        "args" -> normalizedArgs.mkString(" ")
      )
      exitWith(2)
      return
    }

    if (!PathValidator.isAbsolute(config.inputPath)) {
      DriverLogger.emitAlways(
        DriverLogLevel.Error,
        "cli_argument_invalid",
        "argument" -> "--path",
        "reason" -> "must_be_absolute_path_or_uri",
        "value" -> config.inputPath
      )
      exitWith(2)
      return
    }

    if (!PathValidator.isAbsolute(config.outputPath)) {
      DriverLogger.emitAlways(
        DriverLogLevel.Error,
        "cli_argument_invalid",
        "argument" -> "--output",
        "reason" -> "must_be_absolute_path_or_uri",
        "value" -> config.outputPath
      )
      exitWith(2)
      return
    }

    var spark: Option[SparkSession] = None

    try {
      val session = createSparkSession()
      spark = Some(session)
      session.sparkContext.setLogLevel("WARN")
      runScan(session, config)
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
      "suppressions" -> config.suppressions.size,
      "suppression_file" -> config.suppressionFile.getOrElse("none"),
      "driver_log_level" -> DriverLogger.currentLogLevel.label.toLowerCase
    )

    val bundle = RulesetLoader.loadBundle(config.ruleset)
    val cliSuppressions = parseCliSuppressions(
      spark.sparkContext.hadoopConfiguration,
      config.suppressions,
      config.suppressionFile
    )
    val suppressions = SuppressionSet.from(bundle.suppressions).merge(SuppressionSet.from(cliSuppressions))
    val rules = bundle.rules
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
        suppressions,
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

  private[privyspark] def parseCliSuppressions(
    conf: Configuration,
    inline: Seq[String],
    file: Option[String]
  ): Seq[Suppression] = {
    inline.map(rawValue => parseSuppressionSpec(rawValue, "cli")) ++ file.toSeq.flatMap(loadSuppressionFile(conf, _))
  }

  private def loadSuppressionFile(conf: Configuration, path: String): Seq[Suppression] = {
    val normalizedPath = Option(path).map(_.trim).getOrElse("")
    if (normalizedPath.isEmpty) {
      Seq.empty
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

  private def readSuppressions(reader: BufferedReader, source: String): Seq[Suppression] = {
    val suppressions = ArrayBuffer.empty[Suppression]

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

  private def parseSuppressionSpec(rawValue: String, source: String): Suppression = {
    val parts = Option(rawValue).map(_.split(":", 2)).getOrElse(Array.empty[String])
    val columnName = if (parts.length == 2) parts(0).trim else ""
    val piiType = if (parts.length == 2) parts(1).trim else ""

    if (columnName.isEmpty || piiType.isEmpty) {
      throw new IllegalArgumentException(s"Invalid suppression entry ($source): $rawValue")
    }

    Suppression(columnName, piiType)
  }
}
