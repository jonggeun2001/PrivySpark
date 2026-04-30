package io.github.jonggeun2001.privyspark.report

import io.github.jonggeun2001.privyspark.PrivySparkSpecFixtures
import io.github.jonggeun2001.privyspark.model.{ScanError, ScanResult}
import org.apache.spark.sql.DataFrame
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.file.Files

@RunWith(classOf[JUnitRunner])
class ReportWriterSpec extends AnyFunSuite with PrivySparkSpecFixtures {
  test("writeReports default overload promotes parquet reports") {
    val outputDir = Files.createTempDirectory("privyspark-report-default-")

    try {
      ReportWriter.writeReports(spark, outputDir.toString, Seq(scanResult("part-0001.csv")), Seq(scanError("broken.csv")))

      val resultsDf = spark.read.parquet(s"${outputDir.toString}/parquet/scan_results")
      val errorsDf = spark.read.parquet(s"${outputDir.toString}/parquet/scan_errors")

      assert(resultsDf.count() == 1L)
      assert(errorsDf.count() == 1L)
      assert(resultsDf.select("file_identifier").collect().map(_.getString(0)).toSeq == Seq("part-0001.csv"))
      assert(errorsDf.select("file_identifier").collect().map(_.getString(0)).toSeq == Seq("broken.csv"))
      assert(!Files.exists(outputDir.resolve("csv")))
      assert(!Files.exists(outputDir.resolve("excel")))
      assert(!Files.exists(outputDir.resolve("_report_staging")))
    } finally {
      deleteRecursively(outputDir)
    }
  }

  test("writeReports format overload promotes selected csv and excel reports") {
    val outputDir = Files.createTempDirectory("privyspark-report-formats-")

    try {
      ReportWriter.writeReports(spark, outputDir.toString, Seq(scanResult("stale.csv")), Seq.empty[ScanError])
      assert(Files.exists(outputDir.resolve("parquet/scan_results")))

      ReportWriter.writeReports(
        spark,
        outputDir.toString,
        Seq(scanResult("part-0001.csv")),
        Seq(scanError("broken.csv")),
        Seq("csv", "excel")
      )

      val resultCsvDf = spark.read.option("header", "true").csv(s"${outputDir.toString}/csv/scan_results")
      val errorCsvDf = spark.read.option("header", "true").csv(s"${outputDir.toString}/csv/scan_errors")
      val resultWorkbookRows = readWorkbookRows(outputDir.resolve("excel/scan_results.xlsx"), "scan_results")
      val errorWorkbookRows = readWorkbookRows(outputDir.resolve("excel/scan_errors.xlsx"), "scan_errors")

      assert(resultCsvDf.select("file_identifier").collect().map(_.getString(0)).toSeq == Seq("part-0001.csv"))
      assert(errorCsvDf.select("file_identifier").collect().map(_.getString(0)).toSeq == Seq("broken.csv"))
      assert(resultWorkbookRows.head.contains("file_identifier"))
      assert(resultWorkbookRows(1).contains("part-0001.csv"))
      assert(errorWorkbookRows.head.contains("error_message"))
      assert(errorWorkbookRows(1).contains("broken.csv"))
      assert(!Files.exists(outputDir.resolve("parquet")))
      assert(!Files.exists(outputDir.resolve("_report_staging")))
    } finally {
      deleteRecursively(outputDir)
    }
  }

  test("writeReports sequence guard overload restores previous outputs when promotion fails") {
    val outputDir = Files.createTempDirectory("privyspark-report-seq-rollback-")

    try {
      ReportWriter.writeReports(spark, outputDir.toString, Seq(scanResult("part-0001.csv")), Seq.empty[ScanError])

      val error = intercept[RuntimeException] {
        ReportWriter.writeReports(
          spark,
          outputDir.toString,
          Seq(scanResult("replacement.csv")),
          Seq.empty[ScanError],
          Seq("csv"),
          () => throw new RuntimeException("promote guard failed")
        )
      }

      val preservedDf = spark.read.parquet(s"${outputDir.toString}/parquet/scan_results")
      assert(error.getMessage.contains("promote guard failed"))
      assert(preservedDf.select("file_identifier").collect().map(_.getString(0)).toSeq == Seq("part-0001.csv"))
      assert(!Files.exists(outputDir.resolve("csv")))
    } finally {
      deleteRecursively(outputDir)
    }
  }

  test("writeReports dataframe overload promotes and rolls back with the same staging contract") {
    val outputDir = Files.createTempDirectory("privyspark-report-df-")

    try {
      val (initialResultsDf, emptyErrorsDf) = reportDataFrames(Seq(scanResult("part-0001.csv")), Seq.empty[ScanError])
      ReportWriter.writeReports(
        spark,
        outputDir.toString,
        initialResultsDf,
        emptyErrorsDf,
        Seq("parquet"),
        () => ()
      )

      val (replacementResultsDf, replacementErrorsDf) =
        reportDataFrames(Seq(scanResult("replacement.csv")), Seq(scanError("replacement-broken.csv")))
      val error = intercept[RuntimeException] {
        ReportWriter.writeReports(
          spark,
          outputDir.toString,
          replacementResultsDf,
          replacementErrorsDf,
          Seq("csv"),
          () => throw new RuntimeException("dataframe promote guard failed")
        )
      }

      val preservedDf = spark.read.parquet(s"${outputDir.toString}/parquet/scan_results")
      assert(error.getMessage.contains("dataframe promote guard failed"))
      assert(preservedDf.select("file_identifier").collect().map(_.getString(0)).toSeq == Seq("part-0001.csv"))
      assert(!Files.exists(outputDir.resolve("csv")))
    } finally {
      deleteRecursively(outputDir)
    }
  }

  private def reportDataFrames(results: Seq[ScanResult], errors: Seq[ScanError]): (DataFrame, DataFrame) = {
    import spark.implicits._
    (spark.createDataset(results).toDF(), spark.createDataset(errors).toDF())
  }

  private def scanResult(fileIdentifier: String): ScanResult =
    ScanResult(
      dataset_path = "/data/input",
      scan_timestamp = "2026-04-30T00:00:00Z",
      file_identifier = fileIdentifier,
      column_name = "email",
      pii_type = "email",
      match_count = 1L,
      sampled_row_count = 1L,
      match_ratio = 1.0,
      non_empty_match_ratio = 1.0,
      confidence = 1.0,
      sample_raw_value = "alice@example.com",
      sample_matched_fragment = "alice@example.com"
    )

  private def scanError(fileIdentifier: String): ScanError =
    ScanError(
      dataset_path = "/data/input",
      scan_timestamp = "2026-04-30T00:00:00Z",
      file_identifier = fileIdentifier,
      error_message = "Unsupported file format"
    )
}
