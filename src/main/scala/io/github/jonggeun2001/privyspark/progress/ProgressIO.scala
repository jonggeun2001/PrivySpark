package io.github.jonggeun2001.privyspark.progress

import io.github.jonggeun2001.privyspark.report.JsonCodec._
import io.github.jonggeun2001.privyspark.model.{ActiveRunMarker, ProgressRun, ProgressRunMetadata, ScanError, ScanResult}
import io.github.jonggeun2001.privyspark.report.ReportWriter
import io.github.jonggeun2001.privyspark.util.DriverLogger
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col}

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.util.control.NonFatal

private[privyspark] object ProgressIO {
  def persistProgressRecords(
    conf: org.apache.hadoop.conf.Configuration,
    progressRun: ProgressRun,
    scope: String,
    identifier: String,
    results: Seq[ScanResult],
    errors: Seq[ScanError]
  ): Unit = {
    if (results.nonEmpty) {
      writeProgressLines(conf, progressRun.resultsPath, scope, results.map(scanResultToJson))
    }
    if (errors.nonEmpty) {
      writeProgressLines(conf, progressRun.errorsPath, scope, errors.map(scanErrorToJson))
    }
    writeProgressLines(
      conf,
      progressRun.completionsPath,
      scope,
      Seq(progressCompletionToJson(scope, identifier, results.size, errors.size))
    )
    ProgressRunManager.updateActiveRunHeartbeat(conf, progressRun)
    DriverLogger.debug(
      "progress_write_complete",
      "run_id" -> progressRun.runId,
      "scope" -> scope,
      "identifier" -> identifier,
      "results" -> results.size,
      "errors" -> errors.size
    )
  }

  def readProgressRecords(
    spark: SparkSession,
    directoryPath: String,
    schema: StructType
  ): DataFrame = {
    val conf = spark.sparkContext.hadoopConfiguration
    val directory = new Path(directoryPath)
    val fs = directory.getFileSystem(conf)
    val jsonPattern = new Path(s"${directoryPath.stripSuffix("/")}/*.jsonl")
    val files = Option(fs.globStatus(jsonPattern)).getOrElse(Array.empty)
    if (files.isEmpty) {
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    } else {
      spark.read.schema(schema).json(jsonPattern.toString)
    }
  }

  def readProgressScanResults(
    spark: SparkSession,
    directoryPath: String
  ): DataFrame = {
    val resultSchema = Encoders.product[ScanResult].schema
    val legacyCompatibleSchema = resultSchema.add("non_null_match_ratio", "double")

    readProgressRecords(spark, directoryPath, legacyCompatibleSchema)
      .withColumn(
        "non_empty_match_ratio",
        coalesce(col("non_empty_match_ratio"), col("non_null_match_ratio"))
      )
      .select(resultSchema.fieldNames.map(col): _*)
  }

  def writeProgressLines(
    conf: org.apache.hadoop.conf.Configuration,
    directoryPath: String,
    scope: String,
    lines: Seq[String]
  ): Unit = {
    if (lines.isEmpty) {
      return
    }

    val filePath = new Path(s"${directoryPath.stripSuffix("/")}/$scope-${UUID.randomUUID().toString}.jsonl")
    val fs = filePath.getFileSystem(conf)
    val writer = new BufferedWriter(new OutputStreamWriter(fs.create(filePath, false), StandardCharsets.UTF_8))
    try {
      lines.foreach { line =>
        writer.write(line)
        writer.newLine()
      }
    } finally {
      writer.close()
    }
  }

  def writeJsonFile(
    conf: org.apache.hadoop.conf.Configuration,
    filePath: String,
    line: String,
    overwrite: Boolean = true
  ): Unit = {
    val path = new Path(filePath)
    val fs = path.getFileSystem(conf)
    val writer = new BufferedWriter(new OutputStreamWriter(fs.create(path, overwrite), StandardCharsets.UTF_8))
    try {
      writer.write(line)
      writer.newLine()
    } finally {
      writer.close()
    }
  }

  def deleteProgressRun(conf: org.apache.hadoop.conf.Configuration, progressRun: ProgressRun): Unit = {
    val runPath = new Path(progressRun.runPath)
    val fs = runPath.getFileSystem(conf)
    if (fs.exists(runPath)) {
      fs.delete(runPath, true)
    }

    ProgressRunManager.deleteOwnedActiveRunMarker(conf, progressRun)

    val rootPath = new Path(progressRun.rootPath)
    deleteEmptyProgressRoot(fs, rootPath)
  }

  def progressRootHasRunMetadata(
    fs: org.apache.hadoop.fs.FileSystem,
    root: Path
  ): Boolean =
    Option(fs.listStatus(root)).getOrElse(Array.empty).exists { status =>
      status.isDirectory && fs.exists(new Path(status.getPath, "meta/run.json"))
    }

  def progressRootHasFailedRunMetadata(
    conf: org.apache.hadoop.conf.Configuration,
    root: Path
  ): Boolean = {
    val fs = root.getFileSystem(conf)
    Option(fs.listStatus(root)).getOrElse(Array.empty).exists { status =>
      status.isDirectory && readRunMetadataFile(conf, new Path(status.getPath, "meta/run.json")).exists(_.state == "FAILED")
    }
  }

  def readProgressRunMetadata(
    conf: org.apache.hadoop.conf.Configuration,
    progressRun: ProgressRun
  ): Option[ProgressRunMetadata] =
    readRunMetadataFile(conf, new Path(s"${progressRun.metaPath}/run.json"))

  def readRunMetadataFile(
    conf: org.apache.hadoop.conf.Configuration,
    path: Path
  ): Option[ProgressRunMetadata] = {
    val fs = path.getFileSystem(conf)
    if (!fs.exists(path)) {
      return None
    }

    val reader = new BufferedReader(new InputStreamReader(fs.open(path), StandardCharsets.UTF_8))
    try {
      Option(reader.readLine())
        .flatMap { line =>
          for {
            runId <- extractJsonStringField(line, "run_id")
            state <- extractJsonStringField(line, "state")
          } yield ProgressRunMetadata(runId, state)
        }
    } finally {
      reader.close()
    }
  }

  def deleteEmptyProgressRoot(
    fs: org.apache.hadoop.fs.FileSystem,
    root: Path
  ): Unit = {
    if (fs.exists(root) && Option(fs.listStatus(root)).getOrElse(Array.empty).isEmpty) {
      fs.delete(root, true)
    }
  }

  def readActiveRunMarker(
    conf: org.apache.hadoop.conf.Configuration,
    activeRunPath: String
  ): Option[ActiveRunMarker] = {
    val path = new Path(activeRunPath)
    val fs = path.getFileSystem(conf)
    if (!fs.exists(path)) {
      return None
    }

    try {
      val reader = new BufferedReader(new InputStreamReader(fs.open(path), StandardCharsets.UTF_8))
      try {
        val line = Option(reader.readLine()).getOrElse("")
        for {
          runId <- extractJsonStringField(line, "run_id")
          state <- extractJsonStringField(line, "state")
          heartbeat <- extractJsonLongField(line, "last_heartbeat_epoch_ms")
        } yield ActiveRunMarker(runId, state, heartbeat)
      } finally {
        reader.close()
      }
    } catch {
      case NonFatal(_) => None
    }
  }
}
