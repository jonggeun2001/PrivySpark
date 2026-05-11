package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.scan.archive.ArchiveStaging.ArchiveFormats
import io.github.jonggeun2001.privyspark.format.CsvInference.XlsxFormat
import io.github.jonggeun2001.privyspark.fsio.ManagedPaths.cleanupStagingPaths
import io.github.jonggeun2001.privyspark.scan.discovery.{DirectoryDiscovery, DiscoveredFile, PreScanExecutor, SchemaGroupSplitter}
import io.github.jonggeun2001.privyspark.util.ParallelismConfig._
import io.github.jonggeun2001.privyspark.util.{DriverLogger, RpcGate}
import io.github.jonggeun2001.privyspark.config.IgnoreMatcher
import io.github.jonggeun2001.privyspark.model.{DirectoryScanPlan, ScanError, ScanFileEntry, ScanGroup, ScanReadOptions}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import java.io.FileNotFoundException
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

private[privyspark] object DirectoryScanner {
  private def elapsedMillis(startNanos: Long): Long = {
    (System.nanoTime() - startNanos) / 1000000L
  }

  def scanDirectoryStructure(
    spark: SparkSession,
    inputPath: String,
    datasetPath: String,
    timestamp: String,
    preScanParallelism: Int = -1,
    ignoreMatcher: IgnoreMatcher = IgnoreMatcher.empty,
    csvHeadCache: CsvHeadCache = new CsvHeadCache(),
    schemaSigCache: SchemaSignatureCache = new SchemaSignatureCache(),
    parseOkCache: ParseOkCache = new ParseOkCache(),
    readOptions: ScanReadOptions = ScanReadOptions()
  ): DirectoryScanPlan = {
    DriverLogger.debug("scan_directory_structure_start", "input_path" -> inputPath, "dataset_path" -> datasetPath)
    val conf = spark.sparkContext.hadoopConfiguration
    val path = new Path(inputPath)
    val fs = path.getFileSystem(conf)
    val stagingPaths = ArrayBuffer.empty[String]
    val fileDiscoveryStartedAt = System.nanoTime()
    val preScanRpcGate = RpcGate.preScanGate(spark)

    try {
      val inputStatus =
        try {
          fs.getFileStatus(path)
        } catch {
          case _: FileNotFoundException =>
            throw new IllegalArgumentException(s"Input path not found: $inputPath")
        }
      val inputPathIsFile = inputStatus.isFile
      val resolvedDiscoveryParallelism = if (inputPathIsFile) {
        1
      } else {
        resolveDiscoveryParallelism(spark, preScanParallelism)
      }

      val (files, ignoredFiles) = if (inputPathIsFile) {
        ignoreMatcher.matched(path.toString, inputPath) match {
          case Some(pattern) =>
            (Seq.empty[DiscoveredFile], Seq(path.toString -> pattern))
          case None =>
            (Seq(DiscoveredFile(path.toString, inputStatus.getLen, inputStatus.getModificationTime)), Seq.empty[(String, String)])
        }
      } else {
        DriverLogger.debug(
          "scan_directory_file_discovery_parallelism",
          "input_path" -> inputPath,
          "parallelism" -> resolvedDiscoveryParallelism,
          "driver_rpc_concurrency" -> preScanRpcGate.map(_.permits).getOrElse("disabled")
        )
        DirectoryDiscovery.discover(fs, path, inputPath, ignoreMatcher, resolvedDiscoveryParallelism, rpcGate = preScanRpcGate)
      }
      ignoredFiles.foreach {
        case (filePath, pattern) =>
          DriverLogger.debug(
            "scan_directory_file_ignored",
            "input_path" -> inputPath,
            "file" -> filePath,
            "pattern" -> pattern
          )
      }
      DriverLogger.debug(
        "scan_directory_files_discovered",
        "input_path" -> inputPath,
        "files" -> files.size,
        "ignored_files" -> ignoredFiles.size,
        "duration_ms" -> elapsedMillis(fileDiscoveryStartedAt)
      )

      val supportedFiles = ArrayBuffer.empty[ScanFileEntry]
      val errors = ArrayBuffer.empty[ScanError]
      val directoriesWithPreScanErrors = scala.collection.mutable.Set.empty[String]
      val resolvedPreScanParallelism = if (preScanParallelism > 0) {
        resolveConfiguredPreScanParallelism(files.size, preScanParallelism, "--pre-scan-parallelism")
      } else {
        resolvePreScanParallelism(spark, files.size)
      }

      DriverLogger.debug(
        "scan_directory_pre_scan_parallelism",
        "input_path" -> inputPath,
        "files" -> files.size,
        "parallelism" -> resolvedPreScanParallelism,
        "driver_rpc_concurrency" -> preScanRpcGate.map(_.permits).getOrElse("disabled")
      )

      val preScanStartedAt = System.nanoTime()
      val preScanOutcomes = PreScanExecutor.runPreScan(
        spark,
        files,
        datasetPath,
        inputPath,
        timestamp,
        resolvedPreScanParallelism,
        readOptions,
        ignoreMatcher,
        csvHeadCache,
        preScanRpcGate
      )
      val ignoredArchiveEntryCount = preScanOutcomes.map(_.ignoredEntries).sum
      val totalIgnoredCount = ignoredFiles.size + ignoredArchiveEntryCount

      DriverLogger.debug(
        "scan_directory_pre_scan_execute_complete",
        "input_path" -> inputPath,
        "files" -> files.size,
        "parallelism" -> resolvedPreScanParallelism,
        "duration_ms" -> elapsedMillis(preScanStartedAt),
        "ignored_files" -> totalIgnoredCount,
        "completed_files" -> preScanOutcomes.size,
        "skipped_files" -> preScanOutcomes.count(_.skipped),
        "expanded_entries" -> preScanOutcomes.map(_.expandedEntries.size.toLong).sum,
        "error_entries" -> preScanOutcomes.map(_.expandedErrors.size.toLong).sum,
        "failure_files" -> preScanOutcomes.count(_.failure.isDefined),
        "probe_candidates" -> preScanOutcomes.count(_.probeRequired),
        "archive_candidates" -> preScanOutcomes.count(_.pathInferredFormat.exists(ArchiveFormats.contains)),
        "xlsx_candidates" -> preScanOutcomes.count(_.pathInferredFormat.contains(XlsxFormat))
      )

      val preScanCollectStartedAt = System.nanoTime()
      DriverLogger.debug("scan_directory_pre_scan_collect_start", "input_path" -> inputPath, "outcomes" -> preScanOutcomes.size)
      preScanOutcomes.foreach(outcome => stagingPaths ++= outcome.stagingPaths)
      preScanOutcomes.flatMap(_.failure).headOption.foreach { failure =>
        throw failure
      }

      preScanOutcomes.foreach { outcome =>
        supportedFiles ++= outcome.expandedEntries
        errors ++= outcome.expandedErrors

        if (outcome.skipped) {
          DriverLogger.debug(
            "scan_directory_file_skipped",
            "file" -> outcome.filePath,
            "directory" -> outcome.groupingDirectoryPath,
            "reason" -> "zero_byte"
          )
        } else if (outcome.expandedEntries.nonEmpty) {
          DriverLogger.debug(
            "scan_directory_file_supported",
            "file" -> outcome.filePath,
            "expanded_entries" -> outcome.expandedEntries.size,
            "formats" -> outcome.expandedEntries.map(_.format).distinct.sorted.mkString(","),
            "directory" -> outcome.groupingDirectoryPath
          )
        }
        if (outcome.expandedErrors.nonEmpty) {
          directoriesWithPreScanErrors += outcome.preScanErrorScope
          DriverLogger.debug(
            "scan_directory_file_unsupported",
            "file" -> outcome.filePath,
            "directory" -> outcome.preScanErrorScope,
            "errors" -> outcome.expandedErrors.size
          )
        }
      }

      DriverLogger.debug(
        "scan_directory_pre_scan_collect_complete",
        "input_path" -> inputPath,
        "duration_ms" -> elapsedMillis(preScanCollectStartedAt),
        "supported_files" -> supportedFiles.size,
        "errors" -> errors.size,
        "directories_with_pre_scan_errors" -> directoriesWithPreScanErrors.size
      )

      val groupBuildStartedAt = System.nanoTime()
      DriverLogger.debug("scan_directory_group_build_start", "input_path" -> inputPath, "supported_files" -> supportedFiles.size)
      val groupedByDirectoryAndFormat = supportedFiles
        .groupBy(file => (file.directoryPath, file.format))
        .toSeq
        .sortBy { case ((directoryPath, format), _) => (directoryPath, format) }
        .map {
          case ((directoryPath, format), groupedFiles) =>
            val sortedFiles = groupedFiles.sortBy(_.sourceKey)
            ScanGroup(
              directoryPath = directoryPath,
              format = format,
              schemaSignature = "",
              filePaths = sortedFiles.map(_.sourceKey),
              physicalPathsByKey = sortedFiles.map(file => file.sourceKey -> file.physicalPath).toMap,
              logicalIdentifiersByKey = sortedFiles.map(file => file.sourceKey -> file.logicalIdentifier).toMap,
              fileSizesByKey = sortedFiles.map(file => file.sourceKey -> file.fileSize).toMap,
              fileMtimesByKey = sortedFiles.map(file => file.sourceKey -> file.fileMtimeEpochMs).toMap,
              readOptionsByKey = sortedFiles.collect {
                case file if file.readOptions != ScanReadOptions() => file.sourceKey -> file.readOptions
              }.toMap,
              allowDirectoryIdentifier = sortedFiles.forall(_.allowDirectoryIdentifier)
            )
        }
      DriverLogger.debug(
        "scan_directory_initial_groups_ready",
        "groups" -> groupedByDirectoryAndFormat.size,
        "supported_files" -> supportedFiles.size,
        "duration_ms" -> elapsedMillis(groupBuildStartedAt)
      )

      val splitAndFinalizeResult = SchemaGroupSplitter.splitAndFinalize(
        spark,
        datasetPath,
        inputPath,
        timestamp,
        inputPathIsFile,
        groupedByDirectoryAndFormat,
        directoriesWithPreScanErrors.toSet,
        resolvedPreScanParallelism,
        csvHeadCache,
        schemaSigCache,
        parseOkCache,
        preScanRpcGate
      )
      errors ++= splitAndFinalizeResult.errors
      directoriesWithPreScanErrors ++= splitAndFinalizeResult.directoriesWithPreScanErrors
      val finalizedGroups = splitAndFinalizeResult.groups

      val nonSkippedPreScanOutcomes = preScanOutcomes.filterNot(_.skipped)
      val directoryCount = nonSkippedPreScanOutcomes
        .map(_.groupingDirectoryPath)
        .distinct
        .size
      val totalFiles = nonSkippedPreScanOutcomes.size
      val plannedGroups = finalizedGroups.toSeq.sortBy(group => (group.directoryPath, group.format, group.schemaSignature))

      DriverLogger.debug(
        "scan_directory_structure_complete",
        "input_path" -> inputPath,
        "total_files" -> totalFiles,
        "ignored_files" -> totalIgnoredCount,
        "supported_files" -> supportedFiles.size,
        "groups" -> plannedGroups.size,
        "errors" -> errors.size,
        "directories" -> directoryCount
      )

      DirectoryScanPlan(
        groups = plannedGroups,
        errors = errors.toSeq,
        totalFiles = totalFiles,
        directoryCount = directoryCount,
        ignoredFiles = totalIgnoredCount,
        stagingPaths = stagingPaths.toSeq
      )
    } catch {
      case NonFatal(e) =>
        cleanupStagingPaths(conf, stagingPaths.toSeq)
        throw e
    }
  }

  def splitGroupBySchemaFast(
    spark: SparkSession,
    datasetPath: String,
    timestamp: String,
    group: ScanGroup,
    csvHeadCache: CsvHeadCache = new CsvHeadCache(),
    schemaSigCache: SchemaSignatureCache = new SchemaSignatureCache(),
    parseOkCache: ParseOkCache = new ParseOkCache()
  ): (Seq[ScanGroup], Seq[ScanError]) = {
    SchemaGroupSplitter.splitGroupBySchemaFast(
      spark,
      datasetPath,
      timestamp,
      group,
      csvHeadCache,
      schemaSigCache,
      parseOkCache
    )
  }

  def splitGroupBySchema(
    spark: SparkSession,
    datasetPath: String,
    timestamp: String,
    group: ScanGroup,
    csvHeadCache: CsvHeadCache = new CsvHeadCache(),
    schemaSigCache: SchemaSignatureCache = new SchemaSignatureCache()
  ): (Seq[ScanGroup], Seq[ScanError]) = {
    SchemaGroupSplitter.splitGroupBySchema(
      spark,
      datasetPath,
      timestamp,
      group,
      csvHeadCache,
      schemaSigCache
    )
  }
}
