package io.github.jonggeun2001.privyspark.progress

import io.github.jonggeun2001.privyspark.report.JsonCodec.{activeRunMetadataJson, progressRunMetadataJson}
import io.github.jonggeun2001.privyspark.report.{OutputFormats, ReportWriter, WriteReportsRequest}
import io.github.jonggeun2001.privyspark.model.{ProgressRun, ScanError}
import io.github.jonggeun2001.privyspark.util.DriverLogger
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Encoders, SparkSession}

import java.util.UUID
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.util.control.NonFatal

private[privyspark] object ProgressRunManager {
  private val ProgressDirectoryName = "_progress"
  private val ActiveRunHeartbeatIntervalMillis = 15000L
  private val ActiveRunStaleThresholdMillis = 3L * 60L * 1000L
  private val PreparingRunStaleThresholdMillis = 30000L
  private val ActiveRunMarkerLock = new AnyRef

  def prepareProgressRun(
    conf: org.apache.hadoop.conf.Configuration,
    outputRoot: String,
    datasetPath: String,
    timestamp: String
  ): ProgressRun = {
    val rootPath = s"${outputRoot.stripSuffix("/")}/$ProgressDirectoryName"
    val root = new Path(rootPath)
    val fs = root.getFileSystem(conf)
    val activeRunPath = s"$rootPath/active-run.json"
    val preparingRunPath = s"${outputRoot.stripSuffix("/")}/${ProgressDirectoryName}-preparing.json"
    cleanupProgressRoot(conf, rootPath, activeRunPath, preparingRunPath)

    val runId = s"${timestamp.replaceAll("[:.]", "-")}-${UUID.randomUUID().toString}"
    val runPath = s"$rootPath/$runId"
    val resultsPath = s"$runPath/results"
    val errorsPath = s"$runPath/errors"
    val metaPath = s"$runPath/meta"
    val completionsPath = s"$metaPath/completions"
    val inFlightPath = s"$runPath/in-flight"
    val progressRun = ProgressRun(
      runId,
      rootPath,
      runPath,
      activeRunPath,
      datasetPath,
      outputRoot,
      timestamp,
      resultsPath,
      errorsPath,
      metaPath,
      completionsPath,
      inFlightPath
    )

    try {
      writePreparingRunMarker(conf, progressRun, preparingRunPath, overwrite = false)
      fs.mkdirs(root)
      writeActiveRunMarker(conf, progressRun, state = "RUNNING", overwrite = false)
      Seq(runPath, resultsPath, errorsPath, metaPath, completionsPath, inFlightPath).foreach(path => fs.mkdirs(new Path(path)))
      ProgressIO.writeJsonFile(
        conf,
        s"$metaPath/run.json",
        progressRunMetadataJson(progressRun, state = "RUNNING", errorMessage = None)
      )
      deleteOwnedPreparingRunMarker(conf, preparingRunPath, progressRun.runId)

      DriverLogger.debug(
        "progress_run_prepared",
        "run_id" -> progressRun.runId,
        "root_path" -> progressRun.rootPath,
        "run_path" -> progressRun.runPath
      )
      progressRun
    } catch {
      case _: org.apache.hadoop.fs.FileAlreadyExistsException =>
        deleteOwnedPreparingRunMarker(conf, preparingRunPath, progressRun.runId)
        throw new IllegalStateException(s"Active progress run already exists under output root: $rootPath")
      case NonFatal(e) =>
        deleteOwnedPreparingRunMarker(conf, preparingRunPath, progressRun.runId)
        deleteOwnedActiveRunMarker(conf, progressRun)
        fs.delete(new Path(runPath), true)
        ProgressIO.deleteEmptyProgressRoot(fs, root)
        throw e
    }
  }

  def mergeProgressReports(
    spark: SparkSession,
    outputRoot: String,
    progressRun: ProgressRun
  ): (Long, Long) = mergeProgressReports(spark, outputRoot, progressRun, OutputFormats.Default)

  def mergeProgressReports(
    spark: SparkSession,
    outputRoot: String,
    progressRun: ProgressRun,
    outputFormats: Seq[String]
  ): (Long, Long) = mergeProgressReports(spark, outputRoot, progressRun, outputFormats, _ => ())

  def mergeProgressReports(
    spark: SparkSession,
    outputRoot: String,
    progressRun: ProgressRun,
    outputFormats: Seq[String],
    afterReportWrite: org.apache.spark.sql.DataFrame => Unit
  ): (Long, Long) = {
    val normalizedOutputFormats = OutputFormats.requireSupported(outputFormats)
    DriverLogger.debug(
      "progress_merge_start",
      "run_id" -> progressRun.runId,
      "results_path" -> progressRun.resultsPath,
      "errors_path" -> progressRun.errorsPath,
      "output_formats" -> normalizedOutputFormats.mkString(",")
    )
    val resultDf = ProgressIO.readProgressScanResults(spark, progressRun.resultsPath)
    val errorDf = ProgressIO.readProgressRecords(spark, progressRun.errorsPath, Encoders.product[ScanError].schema)
    val resultCount = resultDf.count()
    val errorCount = errorDf.count()
    ReportWriter.writeReports(
      spark,
      WriteReportsRequest(
        outputRoot,
        resultDf,
        errorDf,
        normalizedOutputFormats
      )
    )
    afterReportWrite(resultDf)
    ProgressIO.deleteProgressRun(spark.sparkContext.hadoopConfiguration, progressRun)
    DriverLogger.debug(
      "progress_merge_complete",
      "run_id" -> progressRun.runId,
      "results" -> resultCount,
      "errors" -> errorCount,
      "output_formats" -> normalizedOutputFormats.mkString(",")
    )
    (resultCount, errorCount)
  }

  def markProgressRunFailed(
    conf: org.apache.hadoop.conf.Configuration,
    progressRun: ProgressRun,
    errorMessage: String
  ): Unit = {
    ProgressIO.writeJsonFile(
      conf,
      s"${progressRun.metaPath}/run.json",
      progressRunMetadataJson(progressRun, state = "FAILED", errorMessage = Some(errorMessage))
    )
    updateActiveRunMarker(conf, progressRun, state = "FAILED", errorMessage = Some(errorMessage))
  }

  def updateActiveRunHeartbeat(
    conf: org.apache.hadoop.conf.Configuration,
    progressRun: ProgressRun
  ): Unit = updateActiveRunMarker(conf, progressRun, state = "RUNNING", errorMessage = None)

  def startProgressHeartbeat(
    conf: org.apache.hadoop.conf.Configuration,
    progressRun: ProgressRun
  ): ScheduledExecutorService = {
    val executor = Executors.newSingleThreadScheduledExecutor()
    executor.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = {
          try {
            updateActiveRunHeartbeat(conf, progressRun)
          } catch {
            case NonFatal(_) =>
          }
        }
      },
      ActiveRunHeartbeatIntervalMillis,
      ActiveRunHeartbeatIntervalMillis,
      TimeUnit.MILLISECONDS
    )
    executor
  }

  def stopProgressHeartbeat(executor: ScheduledExecutorService): Unit = {
    executor.shutdownNow()
    executor.awaitTermination(5L, TimeUnit.SECONDS)
  }

  private def cleanupProgressRoot(
    conf: org.apache.hadoop.conf.Configuration,
    rootPath: String,
    activeRunPath: String,
    preparingRunPath: String
  ): Unit = {
    val root = new Path(rootPath)
    val fs = root.getFileSystem(conf)
    val preparingMarkerPath = new Path(preparingRunPath)
    if (fs.exists(preparingMarkerPath)) {
      val preparingModifiedAt = fs.getFileStatus(preparingMarkerPath).getModificationTime
      if (System.currentTimeMillis() - preparingModifiedAt > PreparingRunStaleThresholdMillis) {
        DriverLogger.warn(
          "progress_cleanup_stale",
          "path" -> rootPath,
          "reason" -> "stale_preparing_run_marker"
        )
        fs.delete(preparingMarkerPath, false)
      } else {
        throw new IllegalStateException(s"Progress root is being prepared under output root: $rootPath")
      }
    }

    if (!fs.exists(root)) {
      return
    }

    val activeMarkerPath = new Path(activeRunPath)
    if (!fs.exists(activeMarkerPath)) {
      if (!ProgressIO.progressRootHasRunMetadata(fs, root)) {
        DriverLogger.warn(
          "progress_cleanup_stale",
          "path" -> rootPath,
          "reason" -> "missing_active_run_marker_without_run_metadata"
        )
        fs.delete(root, true)
      } else {
        val rootModifiedAt = fs.getFileStatus(root).getModificationTime
        if (System.currentTimeMillis() - rootModifiedAt > ActiveRunStaleThresholdMillis) {
          DriverLogger.warn(
            "progress_cleanup_stale",
            "path" -> rootPath,
            "reason" -> "missing_active_run_marker"
          )
          fs.delete(root, true)
        } else {
          throw new IllegalStateException(s"Progress root is being prepared under output root: $rootPath")
        }
      }
      return
    }

    ProgressIO.readActiveRunMarker(conf, activeRunPath) match {
      case Some(marker) if marker.state == "FAILED" || isStaleActiveRun(marker.lastHeartbeatEpochMillis) =>
        DriverLogger.warn(
          "progress_cleanup_stale",
          "path" -> rootPath,
          "run_id" -> marker.runId,
          "state" -> marker.state,
          "last_heartbeat_epoch_ms" -> marker.lastHeartbeatEpochMillis
        )
        fs.delete(root, true)
      case Some(marker) =>
        throw new IllegalStateException(s"Active progress run already exists under output root: $rootPath (run_id=${marker.runId})")
      case None =>
        if (ProgressIO.progressRootHasFailedRunMetadata(conf, root)) {
          DriverLogger.warn(
            "progress_cleanup_stale",
            "path" -> rootPath,
            "reason" -> "failed_run_metadata_with_unreadable_active_run_marker"
          )
          fs.delete(root, true)
        } else {
          val markerModifiedAt = fs.getFileStatus(activeMarkerPath).getModificationTime
          if (System.currentTimeMillis() - markerModifiedAt > ActiveRunStaleThresholdMillis) {
            DriverLogger.warn(
              "progress_cleanup_stale",
              "path" -> rootPath,
              "reason" -> "stale_unreadable_active_run_marker"
            )
            fs.delete(root, true)
          } else {
            throw new IllegalStateException(s"Active progress marker is unreadable under output root: $rootPath")
          }
        }
    }
  }

  private def updateActiveRunMarker(
    conf: org.apache.hadoop.conf.Configuration,
    progressRun: ProgressRun,
    state: String,
    errorMessage: Option[String]
  ): Unit = {
    ActiveRunMarkerLock.synchronized {
      val runMetadata = ProgressIO.readProgressRunMetadata(conf, progressRun)
      val failedRunMetadata = runMetadata.exists(metadata => metadata.runId == progressRun.runId && metadata.state == "FAILED")
      ProgressIO.readActiveRunMarker(conf, progressRun.activeRunPath) match {
        case Some(marker) if marker.runId == progressRun.runId && marker.state == "FAILED" && state == "RUNNING" =>
        case Some(marker) if marker.runId == progressRun.runId && failedRunMetadata && state == "RUNNING" =>
        case Some(marker) if marker.runId == progressRun.runId =>
          writeActiveRunMarker(conf, progressRun, state, overwrite = true, errorMessage)
        case None if failedRunMetadata && state == "RUNNING" =>
        case None if runMetadata.exists(_.runId == progressRun.runId) =>
          DriverLogger.warn(
            "progress_active_run_marker_self_healed",
            "run_id" -> progressRun.runId,
            "path" -> progressRun.activeRunPath,
            "state" -> state
          )
          writeActiveRunMarker(conf, progressRun, state, overwrite = true, errorMessage)
        case _ =>
      }
    }
  }

  private def writeActiveRunMarker(
    conf: org.apache.hadoop.conf.Configuration,
    progressRun: ProgressRun,
    state: String,
    overwrite: Boolean,
    errorMessage: Option[String] = None
  ): Unit = {
    ProgressIO.writeJsonFile(
      conf,
      progressRun.activeRunPath,
      activeRunMetadataJson(progressRun, state, System.currentTimeMillis(), errorMessage),
      overwrite = overwrite
    )
  }

  private def writePreparingRunMarker(
    conf: org.apache.hadoop.conf.Configuration,
    progressRun: ProgressRun,
    preparingRunPath: String,
    overwrite: Boolean
  ): Unit = {
    ProgressIO.writeJsonFile(
      conf,
      preparingRunPath,
      activeRunMetadataJson(progressRun, state = "PREPARING", System.currentTimeMillis(), errorMessage = None),
      overwrite = overwrite
    )
  }

  private[privyspark] def deleteOwnedActiveRunMarker(
    conf: org.apache.hadoop.conf.Configuration,
    progressRun: ProgressRun
  ): Unit = deleteOwnedRunMarker(conf, progressRun.activeRunPath, progressRun.runId)

  private def deleteOwnedPreparingRunMarker(
    conf: org.apache.hadoop.conf.Configuration,
    preparingRunPath: String,
    runId: String
  ): Unit = deleteOwnedRunMarker(conf, preparingRunPath, runId)

  private def deleteOwnedRunMarker(
    conf: org.apache.hadoop.conf.Configuration,
    markerPath: String,
    runId: String
  ): Unit = {
    ActiveRunMarkerLock.synchronized {
      ProgressIO.readActiveRunMarker(conf, markerPath) match {
        case Some(marker) if marker.runId == runId =>
          val path = new Path(markerPath)
          val fs = path.getFileSystem(conf)
          if (fs.exists(path)) {
            fs.delete(path, false)
          }
        case _ =>
      }
    }
  }

  private def isStaleActiveRun(lastHeartbeatEpochMillis: Long): Boolean =
    System.currentTimeMillis() - lastHeartbeatEpochMillis > ActiveRunStaleThresholdMillis
}
