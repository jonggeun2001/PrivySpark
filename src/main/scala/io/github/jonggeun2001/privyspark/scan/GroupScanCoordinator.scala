package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.config.SuppressionSet
import io.github.jonggeun2001.privyspark.hive.HiveTableLookupIndex
import io.github.jonggeun2001.privyspark.model.{PiiRule, ProgressRun, ScanError, ScanGroup, ScanResult}
import io.github.jonggeun2001.privyspark.progress.InFlightMarker
import io.github.jonggeun2001.privyspark.progress.ProgressIO.persistProgressRecords
import io.github.jonggeun2001.privyspark.review.AllowlistMatcher
import io.github.jonggeun2001.privyspark.scan.GroupScanRoute.{BatchScan, FileScan, SampledExact}
import io.github.jonggeun2001.privyspark.util.{DriverLogger, DriverTcpConnectionLogger, RpcGate}
import io.github.jonggeun2001.privyspark.util.ParallelismConfig._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import scala.util.control.NonFatal

private[privyspark] object GroupScanCoordinator {
  def scanGroups(
    spark: SparkSession,
    datasetPath: String,
    groups: Seq[ScanGroup],
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    groupParallelism: Int = -1,
    fileParallelism: Int = -1,
    fileSampleRatio: Option[Double] = None,
    fileSampleMinFiles: Int = 10,
    suppressions: SuppressionSet = SuppressionSet.empty,
    allowlistMatcher: AllowlistMatcher = AllowlistMatcher.empty,
    allowlistInputRoot: Option[String] = None,
    progressRun: Option[ProgressRun] = None,
    retainPayloads: Boolean = true,
    csvHeadCache: CsvHeadCache = new CsvHeadCache(),
    hiveLookup: Option[Broadcast[HiveTableLookupIndex]] = None
  ): Seq[(ScanGroup, Seq[ScanResult], Seq[ScanError])] = {
    if (groups.isEmpty) {
      return Seq.empty
    }

    val parallelism = if (groupParallelism > 0) {
      resolveParallelism(groups.size, groupParallelism)
    } else {
      resolveGroupParallelism(spark, groups.size)
    }
    DriverLogger.debug("group_scan_parallelism", "groups" -> groups.size, "parallelism" -> parallelism)
    val rpcGate = RpcGate.driverGate(spark)
    val groupDispatchGate = rpcGate.filter(_.permits > parallelism)
    if (rpcGate.nonEmpty && groupDispatchGate.isEmpty) {
      DriverLogger.warn(
        "group_scan_rpc_gate_outer_skipped",
        "groups" -> groups.size,
        "parallelism" -> parallelism,
        "driver_rpc_concurrency" -> rpcGate.map(_.permits).getOrElse(0),
        "reason" -> "outer_parallelism_not_below_gate"
      )
    }
    DriverTcpConnectionLogger.debugSnapshot(
      "group_scan_tcp_snapshot",
      "phase" -> "groups_start",
      "groups" -> groups.size,
      "parallelism" -> parallelism
    )

    executeInParallel(parallelism, groups.map { group =>
      () => {
        DriverLogger.debug(
          "group_scan_dispatch",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "files" -> group.filePaths.size,
          "use_directory_identifier" -> group.useDirectoryIdentifier,
          "parallelism" -> parallelism
        )
        DriverTcpConnectionLogger.debugSnapshot(
          "group_scan_tcp_snapshot",
          "phase" -> "group_dispatch",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "files" -> group.filePaths.size,
          "use_directory_identifier" -> group.useDirectoryIdentifier,
          "parallelism" -> parallelism
        )
        def scanCurrentGroup(): (Seq[ScanResult], Seq[ScanError]) =
          scanGroup(
            spark,
            datasetPath,
            group,
            rules,
            sampleRatio,
            timestamp,
            fileParallelism,
            fileSampleRatio,
            fileSampleMinFiles,
            suppressions,
            allowlistMatcher,
            allowlistInputRoot,
            progressRun,
            csvHeadCache,
            hiveLookup = hiveLookup
          )
        val (groupResults, groupErrors) = progressRun match {
          case Some(run) =>
            InFlightMarker.run(
              spark.sparkContext.hadoopConfiguration,
              run.inFlightPath,
              "group",
              group.directoryPath,
              Map("format" -> group.format, "schemaSignature" -> group.schemaSignature),
              preserveOnFailure = true
            ) {
              scanCurrentGroup()
            }
          case None =>
            scanCurrentGroup()
        }
        DriverLogger.debug(
          "group_scan_recorded",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "result_rows" -> groupResults.size,
          "error_rows" -> groupErrors.size
        )
        DriverTcpConnectionLogger.debugSnapshot(
          "group_scan_tcp_snapshot",
          "phase" -> "group_recorded",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "files" -> group.filePaths.size,
          "result_rows" -> groupResults.size,
          "error_rows" -> groupErrors.size
        )
        if (retainPayloads) {
          (group, groupResults, groupErrors)
        } else {
          (group, Seq.empty, Seq.empty)
        }
      }
    }, gate = groupDispatchGate)
  }

  def scanGroup(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    fileParallelism: Int = -1,
    fileSampleRatio: Option[Double] = None,
    fileSampleMinFiles: Int = 10,
    suppressions: SuppressionSet = SuppressionSet.empty,
    allowlistMatcher: AllowlistMatcher = AllowlistMatcher.empty,
    allowlistInputRoot: Option[String] = None,
    progressRun: Option[ProgressRun] = None,
    csvHeadCache: CsvHeadCache = new CsvHeadCache(),
    selectedSourceKeys: Option[Seq[String]] = None,
    hiveLookup: Option[Broadcast[HiveTableLookupIndex]] = None
  ): (Seq[ScanResult], Seq[ScanError]) = {
    DriverLogger.debug(
      "group_scan_start",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "files" -> group.filePaths.size,
      "sample_ratio" -> sampleRatio,
      "file_sample_ratio" -> fileSampleRatio.getOrElse("none"),
      "file_sample_min_files" -> fileSampleMinFiles,
      "use_directory_identifier" -> group.useDirectoryIdentifier,
      "schema_sampled" -> group.schemaSampled,
      "csv_has_header" -> group.csvHasHeader
    )
    def rescanSampledGroupWithExactSplit(mode: String): (Seq[ScanResult], Seq[ScanError]) =
      AllowlistApplier.rescanSampledGroupWithExactSplit(
        spark,
        datasetPath,
        group,
        rules,
        sampleRatio,
        timestamp,
        mode,
        fileParallelism,
        fileSampleRatio,
        fileSampleMinFiles,
        suppressions,
        allowlistMatcher,
        allowlistInputRoot,
        progressRun,
        csvHeadCache,
        hiveLookup
      )

    def scanCurrentGroupByFile(selectedKeys: Seq[String]): (Seq[ScanResult], Seq[ScanError]) =
      GroupFileScanner.scanGroupByFile(
        spark,
        datasetPath,
        group,
        rules,
        sampleRatio,
        timestamp,
        fileParallelism,
        suppressions,
        allowlistMatcher,
        allowlistInputRoot,
        progressRun,
        csvHeadCache,
        fileSampleRatio,
        fileSampleMinFiles,
        selectedSourceKeys = Some(selectedKeys),
        hiveLookup = hiveLookup
      )

    GroupScanRouter.routeOf(group) match {
      case SampledExact =>
        val exactSplitResult = rescanSampledGroupWithExactSplit("sampled_exact_split")
        DriverLogger.debug(
          "group_scan_complete",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "result_rows" -> exactSplitResult._1.size,
          "error_rows" -> exactSplitResult._2.size,
          "mode" -> "sampled_exact_split"
        )
        exactSplitResult

      case FileScan =>
        val effectiveSelectedSourceKeys =
          selectedSourceKeys.getOrElse(FileMetricsScanner.resolveSelectedFileKeys(group, sampleRatio, fileSampleRatio, fileSampleMinFiles))
        val fallbackResult = scanCurrentGroupByFile(effectiveSelectedSourceKeys)
        DriverLogger.debug(
          "group_scan_complete",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "result_rows" -> fallbackResult._1.size,
          "error_rows" -> fallbackResult._2.size,
          "mode" -> (if (group.useDirectoryIdentifier) "directory_file_scan" else "direct_file_scan")
        )
        fallbackResult

      case BatchScan =>
        val effectiveSelectedSourceKeys =
          selectedSourceKeys.getOrElse(FileMetricsScanner.resolveSelectedFileKeys(group, sampleRatio, fileSampleRatio, fileSampleMinFiles))
        try {
          val results = scanGroupBatch(
            spark,
            datasetPath,
            group,
            rules,
            sampleRatio,
            timestamp,
            fileSampleRatio,
            fileSampleMinFiles,
            suppressions,
            allowlistMatcher,
            allowlistInputRoot,
            selectedSourceKeys = Some(effectiveSelectedSourceKeys),
            progressRun = progressRun,
            hiveLookup = hiveLookup
          )
          progressRun.foreach { run =>
            persistProgressRecords(
              spark.sparkContext.hadoopConfiguration,
              run,
              "group",
              group.directoryPath,
              results,
              Seq.empty
            )
          }
          DriverLogger.debug(
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
            GroupScanFallbackPolicy.fallback(
              group,
              e,
              () => rescanSampledGroupWithExactSplit("fallback_schema_resplit"),
              () => scanCurrentGroupByFile(effectiveSelectedSourceKeys)
            )
        }
    }
  }

  def scanGroupByFile(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    fileParallelism: Int = -1,
    suppressions: SuppressionSet = SuppressionSet.empty,
    allowlistMatcher: AllowlistMatcher = AllowlistMatcher.empty,
    allowlistInputRoot: Option[String] = None,
    progressRun: Option[ProgressRun] = None,
    csvHeadCache: CsvHeadCache = new CsvHeadCache(),
    fileSampleRatio: Option[Double] = None,
    fileSampleMinFiles: Int = 10,
    selectedSourceKeys: Option[Seq[String]] = None,
    hiveLookup: Option[Broadcast[HiveTableLookupIndex]] = None
  ): (Seq[ScanResult], Seq[ScanError]) = {
    GroupFileScanner.scanGroupByFile(
      spark,
      datasetPath,
      group,
      rules,
      sampleRatio,
      timestamp,
      fileParallelism,
      suppressions,
      allowlistMatcher,
      allowlistInputRoot,
      progressRun,
      csvHeadCache,
      fileSampleRatio,
      fileSampleMinFiles,
      selectedSourceKeys,
      hiveLookup
    )
  }

  def scanGroupBatch(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    fileSampleRatio: Option[Double] = None,
    fileSampleMinFiles: Int = 10,
    suppressions: SuppressionSet = SuppressionSet.empty,
    allowlistMatcher: AllowlistMatcher = AllowlistMatcher.empty,
    allowlistInputRoot: Option[String] = None,
    selectedSourceKeys: Option[Seq[String]] = None,
    progressRun: Option[ProgressRun] = None,
    hiveLookup: Option[Broadcast[HiveTableLookupIndex]] = None
  ): Seq[ScanResult] = {
    GroupBatchScanner.scanGroupBatch(
      spark,
      datasetPath,
      group,
      rules,
      sampleRatio,
      timestamp,
      fileSampleRatio,
      fileSampleMinFiles,
      suppressions,
      allowlistMatcher,
      allowlistInputRoot,
      selectedSourceKeys,
      progressRun,
      hiveLookup
    )
  }
}
