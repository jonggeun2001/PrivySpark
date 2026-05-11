package io.github.jonggeun2001.privyspark.scan.discovery

import io.github.jonggeun2001.privyspark.config.IgnoreMatcher
import io.github.jonggeun2001.privyspark.format.ByteProbe.shouldProbeForFormat
import io.github.jonggeun2001.privyspark.format.CsvDialectDetector
import io.github.jonggeun2001.privyspark.format.CsvInference.XlsxFormat
import io.github.jonggeun2001.privyspark.format.FormatDetector
import io.github.jonggeun2001.privyspark.model.{PreScanFileOutcome, ScanError, ScanFileEntry, ScanReadOptions}
import io.github.jonggeun2001.privyspark.scan.SourceExpansion.expandPhysicalSource
import io.github.jonggeun2001.privyspark.scan.archive.ArchiveStaging.ArchiveFormats
import io.github.jonggeun2001.privyspark.scan.CsvHeadCache
import io.github.jonggeun2001.privyspark.util.{DriverLogger, RpcGate}
import io.github.jonggeun2001.privyspark.util.ParallelismConfig.executeInParallel
import io.github.jonggeun2001.privyspark.util.PathIdentifiers.{normalizeHiveLayoutGroupingPath, resolveRelativeIdentifier}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

private[privyspark] object PreScanExecutor {
  private val ProgressLogMinIntervalNanos = 5000000000L

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
    files: Seq[DiscoveredFile],
    datasetPath: String,
    inputPath: String,
    timestamp: String,
    parallelism: Int,
    readOptions: ScanReadOptions,
    ignoreMatcher: IgnoreMatcher,
    csvHeadCache: CsvHeadCache,
    rpcGate: Option[RpcGate] = None
  ): Seq[PreScanFileOutcome] = {
    val conf = spark.sparkContext.hadoopConfiguration
    val preScanStartedAt = System.nanoTime()
    val preScanProgressInterval = DirectoryDiscovery.resolvePreScanProgressInterval(files.size)
    val completedPreScanFiles = new AtomicInteger(0)
    val lastProgressNanos = new AtomicLong(preScanStartedAt)

    DriverLogger.debug(
      "scan_directory_pre_scan_execute_start",
      "input_path" -> inputPath,
      "files" -> files.size,
      "parallelism" -> parallelism,
      "progress_interval" -> preScanProgressInterval
    )

    executeInParallel(parallelism, files.map { discovered =>
      () => {
        val filePath = discovered.path
        val sourcePath = new Path(filePath)
        val parentDirectory = Option(sourcePath.getParent).map(_.toString).getOrElse(filePath)
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
            if (discovered.size == 0L) {
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
            } else {
              val (expandedEntries, expandedErrors, ignoredEntries) =
                expandPhysicalSource(
                  conf,
                  datasetPath,
                  timestamp,
                  filePath,
                  logicalIdentifier,
                  groupingDirectory,
                  localStagingPaths,
                  fileSize = discovered.size,
                  fileMtimeEpochMs = discovered.mtimeEpochMs,
                  readOptions = readOptions,
                  ignoreMatcher = ignoreMatcher,
                  precomputedSize = Some(discovered.size)
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
        val nowNanos = System.nanoTime()
        val shouldLogByCount = completedFiles == files.size || completedFiles % preScanProgressInterval == 0
        val shouldLogByTime = !shouldLogByCount && {
          val previousNanos = lastProgressNanos.get()
          nowNanos - previousNanos >= ProgressLogMinIntervalNanos &&
            lastProgressNanos.compareAndSet(previousNanos, nowNanos)
        }
        if (shouldLogByCount || shouldLogByTime) {
          if (shouldLogByCount) {
            lastProgressNanos.set(nowNanos)
          }
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
    }, gate = rpcGate)
  }
}
