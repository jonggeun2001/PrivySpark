package io.github.jonggeun2001.privyspark.scan.discovery

import io.github.jonggeun2001.privyspark.config.IgnoreMatcher
import io.github.jonggeun2001.privyspark.format.ByteProbe.{isZeroBytePhysicalFile, shouldProbeForFormat}
import io.github.jonggeun2001.privyspark.format.CsvDialectDetector
import io.github.jonggeun2001.privyspark.format.CsvInference.XlsxFormat
import io.github.jonggeun2001.privyspark.format.FormatDetector
import io.github.jonggeun2001.privyspark.model.{PreScanFileOutcome, ScanError, ScanFileEntry, ScanReadOptions}
import io.github.jonggeun2001.privyspark.scan.SourceExpansion.expandPhysicalSource
import io.github.jonggeun2001.privyspark.scan.archive.ArchiveStaging.ArchiveFormats
import io.github.jonggeun2001.privyspark.scan.CsvHeadCache
import io.github.jonggeun2001.privyspark.util.DriverLogger
import io.github.jonggeun2001.privyspark.util.ParallelismConfig.executeInParallel
import io.github.jonggeun2001.privyspark.util.PathIdentifiers.{normalizeHiveLayoutGroupingPath, resolveRelativeIdentifier}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

private[privyspark] object PreScanExecutor {
  private def elapsedMillis(startNanos: Long): Long = {
    (System.nanoTime() - startNanos) / 1000000L
  }

  private def refineCsvLikeEntries(
    spark: SparkSession,
    entries: Seq[ScanFileEntry],
    csvHeadCache: CsvHeadCache
  ): Seq[ScanFileEntry] = {
    entries.map { entry =>
      val (format, readOptions) = CsvDialectDetector.refineDetectedFormat(
        spark,
        entry.physicalPath,
        entry.format,
        entry.readOptions,
        csvHeadCache
      )
      if (format == entry.format && readOptions == entry.readOptions) {
        entry
      } else {
        entry.copy(format = format, readOptions = readOptions)
      }
    }
  }

  def runPreScan(
    spark: SparkSession,
    files: Seq[String],
    datasetPath: String,
    inputPath: String,
    timestamp: String,
    parallelism: Int,
    readOptions: ScanReadOptions,
    ignoreMatcher: IgnoreMatcher,
    csvHeadCache: CsvHeadCache
  ): Seq[PreScanFileOutcome] = {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = new Path(inputPath).getFileSystem(conf)
    val preScanStartedAt = System.nanoTime()
    val preScanProgressInterval = DirectoryDiscovery.resolvePreScanProgressInterval(files.size)
    val completedPreScanFiles = new AtomicInteger(0)

    DriverLogger.debug(
      "scan_directory_pre_scan_execute_start",
      "input_path" -> inputPath,
      "files" -> files.size,
      "parallelism" -> parallelism,
      "progress_interval" -> preScanProgressInterval
    )

    executeInParallel(parallelism, files.map { filePath =>
      () => {
        val parentDirectory = Option(new Path(filePath).getParent).map(_.toString).getOrElse(filePath)
        val groupingDirectory = normalizeHiveLayoutGroupingPath(parentDirectory, inputPath)
        val logicalIdentifier = resolveRelativeIdentifier(datasetPath, filePath)
        val pathInferredFormat = FormatDetector.infer(filePath)
        val probeRequired = shouldProbeForFormat(filePath, pathInferredFormat)
        val preScanErrorScope = pathInferredFormat match {
          case Some(format) if ArchiveFormats.contains(format) || format == XlsxFormat => logicalIdentifier
          case _ => groupingDirectory
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
                  groupingDirectoryPath = groupingDirectory,
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
                  groupingDirectoryPath = groupingDirectory,
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
                val fileStatus = fs.getFileStatus(new Path(filePath))
                val (expandedEntries, expandedErrors, ignoredEntries) =
                  expandPhysicalSource(
                    conf,
                    datasetPath,
                    timestamp,
                    filePath,
                    logicalIdentifier,
                    groupingDirectory,
                    localStagingPaths,
                    fileSize = fileStatus.getLen,
                    fileMtimeEpochMs = fileStatus.getModificationTime,
                    readOptions = readOptions,
                    ignoreMatcher = ignoreMatcher
                  )
                val refinedEntries = refineCsvLikeEntries(spark, expandedEntries, csvHeadCache)
                PreScanFileOutcome(
                  filePath = filePath,
                  groupingDirectoryPath = groupingDirectory,
                  preScanErrorScope = preScanErrorScope,
                  expandedEntries = refinedEntries,
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
                groupingDirectoryPath = groupingDirectory,
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
  }
}
