package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.cli.CliConfig
import io.github.jonggeun2001.privyspark.config.{IgnoreMatcher, RulesetLoader, SuppressionParser, SuppressionSet}
import io.github.jonggeun2001.privyspark.format.ExcelReadConfig
import io.github.jonggeun2001.privyspark.fsio.ManagedPaths.cleanupStagingPaths
import io.github.jonggeun2001.privyspark.hive.{HiveMetastoreJdbcConfig, HiveTableLookup}
import io.github.jonggeun2001.privyspark.model.{ProgressRun, ScanReadOptions}
import io.github.jonggeun2001.privyspark.progress.ProgressIO.persistProgressRecords
import io.github.jonggeun2001.privyspark.progress.ProgressRunManager._
import io.github.jonggeun2001.privyspark.review.AllowlistMatcher
import io.github.jonggeun2001.privyspark.util.ParallelismConfig.{renderConfiguredParallelism, resolveCliParallelism}
import io.github.jonggeun2001.privyspark.util.DriverLogger
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.Instant
import java.util.concurrent.ScheduledExecutorService
import scala.util.control.NonFatal

private[privyspark] object ScanPipeline {
  final case class ScanSummary(
    scannedFiles: Int,
    ignoredFiles: Int,
    groupedDirectories: Int,
    groups: Int,
    detections: Long,
    errors: Long,
    outputRoot: String,
    outputFormats: Seq[String]
  ) {
    def consoleLine: String =
      s"[PrivySpark] scanned_files=$scannedFiles, ignored_files=$ignoredFiles, grouped_dirs=$groupedDirectories, groups=$groups, detections=$detections, errors=$errors"
  }

  final case class Hooks(
    warnUnusedExcelMaxRowsInMemory: Option[Int] => Unit = _ => (),
    writeReviewHtml: (Configuration, String, String, DataFrame, String, Option[String], Option[String]) => Unit =
      (_, _, _, _, _, _, _) => ()
  )

  def run(spark: SparkSession, config: CliConfig, hooks: Hooks): ScanSummary = {
    val (preScanParallelism, groupParallelism, fileParallelism) =
      resolveCliParallelism(config.preScanParallelism, config.groupParallelism, config.fileParallelism)
    val byteArrayMaxOverride = ExcelReadConfig.resolveByteArrayMaxOverride(
      spark.sparkContext.getConf,
      config.excelByteArrayMaxOverride
    )
    ExcelReadConfig.applyByteArrayMaxOverride(byteArrayMaxOverride)
    spark.conf.set(ExcelReadConfig.ByteArrayMaxOverrideConfKey, byteArrayMaxOverride.toString)
    spark.sparkContext.hadoopConfiguration.set(ExcelReadConfig.ByteArrayMaxOverrideConfKey, byteArrayMaxOverride.toString)
    hooks.warnUnusedExcelMaxRowsInMemory(config.excelMaxRowsInMemory)
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
    val parsedCliSuppressions = SuppressionParser.parseCliSuppressionEntries(
      spark.sparkContext.hadoopConfiguration,
      config.suppressions,
      config.suppressionFile
    )
    SuppressionParser.warnUnknownSuppressions(parsedCliSuppressions, bundle.rules.map(_.piiType).toSet)
    val cliSuppressions = parsedCliSuppressions.map(_.suppression)
    val suppressions = SuppressionSet.from(bundle.suppressions).merge(SuppressionSet.from(cliSuppressions))
    val rules = bundle.rules
    val hiveMetastoreConfig = for {
      jdbcUrl <- config.hiveMetastoreJdbcUrl
      user <- config.hiveMetastoreUser
      passwordFile <- config.hiveMetastorePasswordFile
    } yield HiveMetastoreJdbcConfig(
      jdbcUrl,
      user,
      passwordFile,
      HiveMetastoreJdbcConfig.resolveDriverClass(spark.sparkContext.getConf, config.hiveMetastoreJdbcDriverClass)
    )
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
      ),
      hiveLookupIndex = Some(hiveLookupBroadcast.value)
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

      GroupScanCoordinator.scanGroups(
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
        schemaSigCache = schemaSigCache,
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
            hooks.writeReviewHtml(
              spark.sparkContext.hadoopConfiguration,
              config.outputPath,
              config.inputPath,
              resultDf,
              config.reviewSampleMode,
              config.reviewHtmlDir,
              config.reviewStateRoot
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

      ScanSummary(
        scanPlan.totalFiles,
        scanPlan.ignoredFiles,
        scanPlan.directoryCount,
        scanPlan.groups.size,
        resultCount,
        errorCount,
        config.outputPath,
        outputFormats
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
