package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.scan.ArchiveStaging.ArchiveFormats
import io.github.jonggeun2001.privyspark.format.ByteProbe.{isZeroBytePhysicalFile, shouldProbeForFormat}
import io.github.jonggeun2001.privyspark.format.CsvInference._
import io.github.jonggeun2001.privyspark.format.FormatDetector
import io.github.jonggeun2001.privyspark.fsio.ManagedPaths.cleanupStagingPaths
import io.github.jonggeun2001.privyspark.util.PathIdentifiers._
import io.github.jonggeun2001.privyspark.util.ParallelismConfig._
import io.github.jonggeun2001.privyspark.util.DriverLogger
import io.github.jonggeun2001.privyspark.fsio.RetryIO.withFileReadRetry
import io.github.jonggeun2001.privyspark.scan.SourceExpansion.expandPhysicalSource
import io.github.jonggeun2001.privyspark.config.IgnoreMatcher
import io.github.jonggeun2001.privyspark.model.{DirectoryScanPlan, PreScanFileOutcome, ScanError, ScanFileEntry, ScanGroup, ScanReadOptions}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

private[privyspark] object DirectoryScanner {
  private val PreScanProgressLogInterval = 10000

  private def elapsedMillis(startNanos: Long): Long = {
    (System.nanoTime() - startNanos) / 1000000L
  }

  def resolvePreScanProgressInterval(fileCount: Int): Int = {
    if (fileCount <= 0) 1 else math.min(fileCount, PreScanProgressLogInterval)
  }

  private def discoverPhysicalFiles(
    fs: org.apache.hadoop.fs.FileSystem,
    rootPath: Path,
    inputPath: String,
    ignoreMatcher: IgnoreMatcher,
    parallelism: Int
  ): (Seq[String], Seq[(String, String)]) = {
    val discoveredFiles = ArrayBuffer.empty[String]
    val ignoredPaths = ArrayBuffer.empty[(String, String)]

    var currentLevelDirectories = Seq(rootPath)
    while (currentLevelDirectories.nonEmpty) {
      val nextLevelDirectories = ArrayBuffer.empty[Path]

      currentLevelDirectories
        .sortBy(_.toString)
        .grouped(math.max(1, parallelism))
        .foreach { directoryBatch =>
          val listedDirectories = executeInParallel(
            parallelism,
            directoryBatch.map { directory =>
              () => Option(fs.listStatus(directory)).getOrElse(Array.empty).sortBy(_.getPath.toString)
            }
          )

          listedDirectories.foreach { children =>
            children.foreach { status =>
              val childPath = status.getPath.toString
              if (status.isDirectory) {
                ignoreMatcher.matched(childPath, inputPath, isDirectory = true) match {
                  case Some(pattern) =>
                    ignoredPaths += ((childPath, pattern))
                  case None =>
                    nextLevelDirectories += status.getPath
                }
              } else if (status.isFile) {
                ignoreMatcher.matched(childPath, inputPath) match {
                  case Some(pattern) =>
                    ignoredPaths += ((childPath, pattern))
                  case None =>
                    discoveredFiles += childPath
                }
              }
            }
          }
        }

      currentLevelDirectories = nextLevelDirectories.toSeq
    }

    (discoveredFiles.toSeq.sorted, ignoredPaths.toSeq)
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
    parseOkCache: ParseOkCache = new ParseOkCache()
  ): DirectoryScanPlan = {
    DriverLogger.debug("scan_directory_structure_start", "input_path" -> inputPath, "dataset_path" -> datasetPath)
    val conf = spark.sparkContext.hadoopConfiguration
    val path = new Path(inputPath)
    val fs = path.getFileSystem(conf)
    val stagingPaths = ArrayBuffer.empty[String]
    val fileDiscoveryStartedAt = System.nanoTime()

    try {
      if (!fs.exists(path)) {
        throw new IllegalArgumentException(s"Input path not found: $inputPath")
      }
      val inputPathIsFile = fs.getFileStatus(path).isFile
      val resolvedDiscoveryParallelism = if (inputPathIsFile) {
        1
      } else {
        resolveDiscoveryParallelism(spark, preScanParallelism)
      }

      val (files, ignoredFiles) = if (inputPathIsFile) {
        ignoreMatcher.matched(path.toString, inputPath) match {
          case Some(pattern) =>
            (Seq.empty[String], Seq(path.toString -> pattern))
          case None =>
            (Seq(path.toString), Seq.empty[(String, String)])
        }
      } else {
        DriverLogger.debug(
          "scan_directory_file_discovery_parallelism",
          "input_path" -> inputPath,
          "parallelism" -> resolvedDiscoveryParallelism
        )
        discoverPhysicalFiles(fs, path, inputPath, ignoreMatcher, resolvedDiscoveryParallelism)
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
        "parallelism" -> resolvedPreScanParallelism
      )

      val preScanStartedAt = System.nanoTime()
      val preScanProgressInterval = resolvePreScanProgressInterval(files.size)
      val completedPreScanFiles = new AtomicInteger(0)
      DriverLogger.debug(
        "scan_directory_pre_scan_execute_start",
        "input_path" -> inputPath,
        "files" -> files.size,
        "parallelism" -> resolvedPreScanParallelism,
        "progress_interval" -> preScanProgressInterval
      )

      val preScanOutcomes = executeInParallel(resolvedPreScanParallelism, files.map { filePath =>
        () => {
          val parentDirectory = Option(new Path(filePath).getParent).map(_.toString).getOrElse(filePath)
          val logicalIdentifier = resolveRelativeIdentifier(datasetPath, filePath)
          val pathInferredFormat = FormatDetector.infer(filePath)
          val probeRequired = shouldProbeForFormat(filePath, pathInferredFormat)
          val preScanErrorScope = pathInferredFormat match {
            case Some(format) if ArchiveFormats.contains(format) || format == XlsxFormat => logicalIdentifier
            case _ => parentDirectory
          }
          val localStagingPaths = ArrayBuffer.empty[String]

          val outcome =
            try {
              val zeroByteStatus = try {
                Right(isZeroBytePhysicalFile(conf, filePath))
              } catch {
                case NonFatal(e) => Left(e)
              }

              zeroByteStatus match {
                case Left(e) =>
                  PreScanFileOutcome(
                    filePath = filePath,
                    groupingDirectoryPath = parentDirectory,
                    preScanErrorScope = preScanErrorScope,
                    expandedEntries = Seq.empty,
                    expandedErrors = Seq(ScanError(datasetPath, timestamp, logicalIdentifier, Option(e.getMessage).getOrElse(e.getClass.getSimpleName))),
                    ignoredEntries = 0,
                    stagingPaths = localStagingPaths.toSeq,
                    pathInferredFormat = pathInferredFormat,
                    probeRequired = probeRequired
                  )
                case Right(true) =>
                  PreScanFileOutcome(
                    filePath = filePath,
                    groupingDirectoryPath = parentDirectory,
                    preScanErrorScope = preScanErrorScope,
                    expandedEntries = Seq.empty,
                    expandedErrors = Seq.empty,
                    ignoredEntries = 0,
                    stagingPaths = localStagingPaths.toSeq,
                    pathInferredFormat = pathInferredFormat,
                    probeRequired = probeRequired,
                    skipped = true
                  )
                case Right(false) =>
                  val (expandedEntries, expandedErrors, ignoredEntries) =
                    expandPhysicalSource(
                      conf,
                      datasetPath,
                      timestamp,
                      filePath,
                      logicalIdentifier,
                      parentDirectory,
                      localStagingPaths,
                      ignoreMatcher = ignoreMatcher
                    )
                  PreScanFileOutcome(
                    filePath = filePath,
                    groupingDirectoryPath = parentDirectory,
                    preScanErrorScope = preScanErrorScope,
                    expandedEntries = expandedEntries,
                    expandedErrors = expandedErrors,
                    ignoredEntries = ignoredEntries,
                    stagingPaths = localStagingPaths.toSeq,
                    pathInferredFormat = pathInferredFormat,
                    probeRequired = probeRequired
                  )
              }
            } catch {
              case NonFatal(e) =>
                PreScanFileOutcome(
                  filePath = filePath,
                  groupingDirectoryPath = parentDirectory,
                  preScanErrorScope = preScanErrorScope,
                  expandedEntries = Seq.empty,
                  expandedErrors = Seq.empty,
                  ignoredEntries = 0,
                  stagingPaths = localStagingPaths.toSeq,
                  pathInferredFormat = pathInferredFormat,
                  probeRequired = probeRequired,
                  failure = Some(e)
                )
            }

          val completedFiles = completedPreScanFiles.incrementAndGet()
          if (completedFiles == files.size || completedFiles % preScanProgressInterval == 0) {
            DriverLogger.debug(
              "scan_directory_pre_scan_progress",
              "input_path" -> inputPath,
              "completed_files" -> completedFiles,
              "total_files" -> files.size,
              "elapsed_ms" -> elapsedMillis(preScanStartedAt)
            )
          }
          outcome
        }
      })
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

      val schemaAwareGroups = ArrayBuffer.empty[ScanGroup]
      val schemaSplitParallelism = resolveParallelism(groupedByDirectoryAndFormat.size, resolvedPreScanParallelism)
      DriverLogger.debug(
        "scan_directory_schema_split_parallelism",
        "groups" -> groupedByDirectoryAndFormat.size,
        "parallelism" -> schemaSplitParallelism
      )
      val schemaSplitOutcomes = executeInParallel(schemaSplitParallelism, groupedByDirectoryAndFormat.map { group =>
        () =>
          val (splitGroups, splitErrors) = splitGroupBySchemaFast(
            spark,
            datasetPath,
            timestamp,
            group,
            csvHeadCache,
            schemaSigCache,
            parseOkCache
          )
          (group, splitGroups, splitErrors)
      })

      schemaSplitOutcomes.foreach {
        case (group, splitGroups, splitErrors) =>
          schemaAwareGroups ++= splitGroups
          errors ++= splitErrors
          DriverLogger.debug(
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
        val isInputRootGroup = comparableGroupingPath(group.directoryPath) == comparableGroupingPath(inputPath)
        val directoryIdentifierEligible =
          !inputPathIsFile &&
          group.allowDirectoryIdentifier &&
            groupsPerDirectory.getOrElse(group.directoryPath, 0) == 1 &&
            (group.filePaths.size > 1 || !isInputRootGroup) &&
            !directoriesWithPreScanErrors.contains(group.directoryPath)
        val finalizedGroup = group.copy(
          useDirectoryIdentifier = directoryIdentifierEligible && !group.schemaSampled,
          directoryIdentifierEligible = directoryIdentifierEligible
        )
        DriverLogger.debug(
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

      val nonSkippedPreScanOutcomes = preScanOutcomes.filterNot(_.skipped)
      val directoryCount = nonSkippedPreScanOutcomes
        .map(outcome => Option(new Path(outcome.filePath).getParent).map(_.toString).getOrElse(outcome.filePath))
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
    if (group.filePaths.size <= 1) {
      splitGroupBySchema(spark, datasetPath, timestamp, group, csvHeadCache, schemaSigCache)
    } else {
      DriverLogger.debug(
        "scan_group_schema_sample_start",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "files" -> group.filePaths.size
      )

      val sampledSourceKey = group.filePaths.head
      val sampledPhysicalPath = resolvePhysicalPath(group, sampledSourceKey)
      val sampledReadOptions = resolveReadOptions(group, sampledSourceKey)
      val sampledSchemaResult = if (group.format == "csv") {
        inferCsvSchemaSignature(spark, sampledPhysicalPath, csvHeadCache, schemaSigCache)
      } else {
        inferSchemaSignature(spark, group.format, sampledPhysicalPath, sampledReadOptions, schemaSigCache)
          .map(signature => (signature, true))
      }

      sampledSchemaResult match {
        case Right((schemaSignature, csvHasHeader)) =>
          val (validatedFilePaths, validationErrors) =
            if (group.format == "json") {
              validateSampledJsonFiles(spark, datasetPath, timestamp, group, parseOkCache)
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
          DriverLogger.debug(
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
          DriverLogger.debug(
            "scan_group_schema_sample_fallback",
            "directory" -> group.directoryPath,
            "format" -> group.format,
            "files" -> group.filePaths.size,
            "reason" -> errorMessage
          )
          splitGroupBySchema(spark, datasetPath, timestamp, group, csvHeadCache, schemaSigCache)
      }
    }
  }

  private def validateSampledJsonFiles(
    spark: SparkSession,
    datasetPath: String,
    timestamp: String,
    group: ScanGroup,
    parseOkCache: ParseOkCache = new ParseOkCache()
  ): (Seq[String], Seq[ScanError]) = {
    val validFilePaths = ArrayBuffer.empty[String]
    val errors = ArrayBuffer.empty[ScanError]

    group.filePaths.foreach { sourceKey =>
      val physicalPath = resolvePhysicalPath(group, sourceKey)
      val logicalIdentifier = resolveLogicalIdentifier(group, datasetPath, sourceKey)
      if (parseOkCache.isOk(physicalPath)) {
        validFilePaths += sourceKey
      } else {
        try {
          withFileReadRetry(spark, Seq(physicalPath), "schema_detection") {
            readSchemaSource(spark, group.format, physicalPath, group.csvHasHeader)
            ()
          }
          parseOkCache.markOk(physicalPath)
          validFilePaths += sourceKey
        } catch {
          case NonFatal(e) =>
            val errorMessage = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
            DriverLogger.debug(
              "group_schema_signature_failed",
              "directory" -> group.directoryPath,
              "file" -> physicalPath,
              "format" -> group.format,
              "reason" -> errorMessage
            )
            errors += ScanError(
              datasetPath,
              timestamp,
              logicalIdentifier,
              s"Schema detection failed: $errorMessage"
            )
        }
      }
    }

    (validFilePaths.toSeq, errors.toSeq)
  }

  def splitGroupBySchema(
    spark: SparkSession,
    datasetPath: String,
    timestamp: String,
    group: ScanGroup,
    csvHeadCache: CsvHeadCache = new CsvHeadCache(),
    schemaSigCache: SchemaSignatureCache = new SchemaSignatureCache()
  ): (Seq[ScanGroup], Seq[ScanError]) = {
    DriverLogger.debug(
      "scan_group_schema_split_start",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "files" -> group.filePaths.size
    )
    val filesBySchema = scala.collection.mutable.Map.empty[(String, Boolean), ArrayBuffer[String]]
    val errors = ArrayBuffer.empty[ScanError]

    group.filePaths.foreach { sourceKey =>
      val physicalPath = resolvePhysicalPath(group, sourceKey)
      val readOptions = resolveReadOptions(group, sourceKey)
      val schemaResult = if (group.format == "csv") {
        inferCsvSchemaSignature(spark, physicalPath, csvHeadCache, schemaSigCache)
      } else {
        inferSchemaSignature(spark, group.format, physicalPath, readOptions, schemaSigCache)
          .map(signature => (signature, true))
      }

      schemaResult match {
        case Right((schemaSignature, csvHasHeader)) =>
          val groupedFiles = filesBySchema.getOrElseUpdate((schemaSignature, csvHasHeader), ArrayBuffer.empty[String])
          groupedFiles += sourceKey
          DriverLogger.debug(
            "group_schema_signature_detected",
            "directory" -> group.directoryPath,
            "file" -> physicalPath,
            "format" -> group.format,
            "schema" -> schemaSignature,
            "csv_has_header" -> csvHasHeader
          )
        case Left(errorMessage) =>
          DriverLogger.debug(
            "group_schema_signature_failed",
            "directory" -> group.directoryPath,
            "file" -> physicalPath,
            "format" -> group.format,
            "reason" -> errorMessage
          )
          errors += ScanError(
            datasetPath,
            timestamp,
            resolveLogicalIdentifier(group, datasetPath, sourceKey),
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

    DriverLogger.debug(
      "scan_group_schema_split_complete",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema_groups" -> groups.size,
      "errors" -> errors.size
    )
    (groups, errors.toSeq)
  }
}
