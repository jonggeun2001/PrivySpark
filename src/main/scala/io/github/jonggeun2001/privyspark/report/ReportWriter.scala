package io.github.jonggeun2001.privyspark.report

import io.github.jonggeun2001.privyspark.fsio.ManagedPaths._
import io.github.jonggeun2001.privyspark.format.WorkbookHelpers.workbookDataAddress
import io.github.jonggeun2001.privyspark.model.{ReportFormatPaths, ScanError, ScanResult}
import io.github.jonggeun2001.privyspark.util.DriverLogger
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.NonFatal

private[privyspark] object ReportWriter {
  private val ReportStagingDirectoryName = "_report_staging"

  def writeReports(
    spark: SparkSession,
    outputRoot: String,
    results: Seq[ScanResult],
    errors: Seq[ScanError]
  ): Unit = writeReports(spark, outputRoot, results, errors, OutputFormats.Default)

  def writeReports(
    spark: SparkSession,
    outputRoot: String,
    results: Seq[ScanResult],
    errors: Seq[ScanError],
    outputFormats: Seq[String]
  ): Unit = writeReports(spark, outputRoot, results, errors, outputFormats, () => ())

  def writeReports(
    spark: SparkSession,
    outputRoot: String,
    results: Seq[ScanResult],
    errors: Seq[ScanError],
    outputFormats: Seq[String],
    beforePromote: () => Unit
  ): Unit = {
    import spark.implicits._
    writeReports(
      spark,
      outputRoot,
      spark.createDataset(results).toDF(),
      spark.createDataset(errors).toDF(),
      outputFormats,
      beforePromote
    )
  }

  def writeReports(
    spark: SparkSession,
    outputRoot: String,
    resultsDf: DataFrame,
    errorsDf: DataFrame,
    outputFormats: Seq[String],
    beforePromote: () => Unit
  ): Unit = {
    val root = outputRoot.stripSuffix("/")
    val conf = spark.sparkContext.hadoopConfiguration
    val normalizedOutputFormats = OutputFormats.requireSupported(outputFormats)
    DriverLogger.debug(
      "write_reports_materialize",
      "output_root" -> root,
      "output_formats" -> normalizedOutputFormats.mkString(",")
    )
    val resultDf = resultsDf.coalesce(1)
    val errorDf = errorsDf.coalesce(1)
    val selectedFinalPaths = normalizedOutputFormats.map(format => reportFormatPaths(root, format))
    val stagingBaseRoot = s"$root/$ReportStagingDirectoryName"
    val stagingRoot = s"$stagingBaseRoot/${UUID.randomUUID().toString}"
    val backupRoot = s"$stagingRoot/backups"
    val stagedPaths = normalizedOutputFormats.map(format => reportFormatPaths(stagingRoot, format))

    val movedBackups = ArrayBuffer.empty[(ReportFormatPaths, ReportFormatPaths)]
    val promotedRoots = ArrayBuffer.empty[String]

    try {
      stagedPaths.foreach(paths => writeReportFormat(resultDf, errorDf, paths))

      OutputFormats.All.foreach { format =>
        val finalPaths = reportFormatPaths(root, format)
        if (pathExists(conf, finalPaths.rootPath)) {
          val backupPaths = reportFormatPaths(backupRoot, format)
          renameManagedPath(conf, finalPaths.rootPath, backupPaths.rootPath)
          movedBackups += ((finalPaths, backupPaths))
        }
      }

      beforePromote()

      stagedPaths.foreach { stagePaths =>
        val finalPaths = reportFormatPaths(root, stagePaths.format)
        renameManagedPath(conf, stagePaths.rootPath, finalPaths.rootPath)
        promotedRoots += finalPaths.rootPath
      }
    } catch {
      case NonFatal(e) =>
        val rollbackFailure = Try {
          restoreBackedUpReportOutputs(conf, promotedRoots.toSeq, movedBackups.toSeq)
          deleteStagingPath(conf, stagingRoot)
        }.failed.toOption

        rollbackFailure match {
          case Some(restoreError) =>
            DriverLogger.warn(
              "report_output_rollback_failed",
              "output_root" -> root,
              "staging_root" -> stagingRoot,
              "reason" -> Option(restoreError.getMessage).getOrElse(restoreError.getClass.getSimpleName)
            )

            val rollbackException = new IllegalStateException(
              s"Report output rollback failed; preserved backup staging at $stagingRoot",
              restoreError
            )
            rollbackException.addSuppressed(e)
            throw rollbackException
          case None =>
            throw e
        }
    }

    val resultPathsByFormat = selectedFinalPaths.map(paths => s"${paths.format}:${paths.resultPath}").mkString(",")
    val errorPathsByFormat = selectedFinalPaths.map(paths => s"${paths.format}:${paths.errorPath}").mkString(",")

    DriverLogger.debug(
      "write_reports_complete",
      "output_root" -> root,
      "output_formats" -> normalizedOutputFormats.mkString(","),
      "result_paths" -> resultPathsByFormat,
      "error_paths" -> errorPathsByFormat
    )

    deleteStagingPath(conf, stagingBaseRoot)
  }

  private def writeExcelReport(df: DataFrame, path: String, sheetName: String): Unit = {
    df.write
      .format("com.crealytics.spark.excel")
      .option("header", "true")
      .option("dataAddress", workbookDataAddress(sheetName))
      .mode("overwrite")
      .save(path)
  }

  private def writeReportFormat(resultDf: DataFrame, errorDf: DataFrame, paths: ReportFormatPaths): Unit = {
    paths.format match {
      case OutputFormats.Parquet =>
        resultDf.write.mode("overwrite").parquet(paths.resultPath)
        errorDf.write.mode("overwrite").parquet(paths.errorPath)
      case OutputFormats.Csv =>
        resultDf.write
          .option("header", "true")
          .mode("overwrite")
          .csv(paths.resultPath)

        errorDf.write
          .option("header", "true")
          .mode("overwrite")
          .csv(paths.errorPath)
      case OutputFormats.Excel =>
        writeExcelReport(resultDf, paths.resultPath, "scan_results")
        writeExcelReport(errorDf, paths.errorPath, "scan_errors")
      case unsupported =>
        throw new IllegalArgumentException(s"Unsupported output format: $unsupported")
    }
  }

  private def reportFormatPaths(root: String, format: String): ReportFormatPaths = {
    format match {
      case OutputFormats.Parquet =>
        ReportFormatPaths(format, s"$root/${OutputFormats.Parquet}", s"$root/${OutputFormats.Parquet}/scan_results", s"$root/${OutputFormats.Parquet}/scan_errors")
      case OutputFormats.Csv =>
        ReportFormatPaths(format, s"$root/${OutputFormats.Csv}", s"$root/${OutputFormats.Csv}/scan_results", s"$root/${OutputFormats.Csv}/scan_errors")
      case OutputFormats.Excel =>
        ReportFormatPaths(format, s"$root/${OutputFormats.Excel}", s"$root/${OutputFormats.Excel}/scan_results.xlsx", s"$root/${OutputFormats.Excel}/scan_errors.xlsx")
      case unsupported =>
        throw new IllegalArgumentException(s"Unsupported output format: $unsupported")
    }
  }

  private[privyspark] def restoreBackedUpReportOutputs(
    conf: org.apache.hadoop.conf.Configuration,
    promotedRoots: Seq[String],
    movedBackups: Seq[(ReportFormatPaths, ReportFormatPaths)]
  ): Unit = {
    promotedRoots.foreach(path => deleteManagedPath(conf, path))
    movedBackups.reverse.foreach {
      case (finalPaths, backupPaths) =>
        if (pathExists(conf, backupPaths.rootPath)) {
          renameManagedPath(conf, backupPaths.rootPath, finalPaths.rootPath)
        }
    }
  }
}
