package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.model.{PiiRule, ScanError, ScanResult}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Comparator

@RunWith(classOf[JUnitRunner])
class PrivySparkAppSpec extends AnyFunSuite with BeforeAndAfterAll {
  private val spark = SparkSession.builder()
    .appName("PrivySparkAppSpec")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("scanDirectoryStructure splits same directory files by schema signature") {
    val inputDir = Files.createTempDirectory("privyspark-schema-plan-")

    try {
      writeText(inputDir.resolve("users_email.csv"),
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(inputDir.resolve("users_phone.csv"),
        "name,phone\n" +
          "bob,010-1234-5678\n")

      val plan = PrivySparkApp.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        "2026-03-05T00:00:00Z"
      )

      val csvGroups = plan.groups.filter(_.format == "csv")
      assert(plan.totalFiles == 2)
      assert(plan.errors.isEmpty)
      assert(csvGroups.size == 2)
      assert(csvGroups.forall(_.filePaths.size == 1))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanDirectoryStructure splits CSV files when header order differs") {
    val inputDir = Files.createTempDirectory("privyspark-schema-order-")

    try {
      writeText(inputDir.resolve("ordered_a.csv"),
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(inputDir.resolve("ordered_b.csv"),
        "email,name\n" +
          "bob@example.com,bob\n")

      val plan = PrivySparkApp.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        "2026-03-05T00:00:00Z"
      )

      val csvGroups = plan.groups.filter(_.format == "csv")
      assert(csvGroups.size == 2)
      assert(csvGroups.forall(_.filePaths.size == 1))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanDirectoryStructure throws when input path does not exist") {
    val missingPath = s"/tmp/privyspark-missing-${System.nanoTime()}"

    val exception = intercept[IllegalArgumentException] {
      PrivySparkApp.scanDirectoryStructure(
        spark,
        missingPath,
        missingPath,
        "2026-03-05T00:00:00Z"
      )
    }

    assert(exception.getMessage.contains("Input path not found"))
  }

  test("scanDirectoryStructure records unsupported files as errors and keeps supported groups") {
    val inputDir = Files.createTempDirectory("privyspark-unsupported-format-")

    try {
      writeText(inputDir.resolve("supported.csv"),
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(inputDir.resolve("unsupported.xlsx"), "binary-placeholder")

      val plan = PrivySparkApp.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        "2026-03-05T00:00:00Z"
      )

      assert(plan.totalFiles == 2)
      assert(plan.groups.size == 1)
      assert(plan.groups.head.filePaths.map(path => new java.io.File(path).getName) == Seq("supported.csv"))
      assert(plan.errors.size == 1)
      assert(plan.errors.head.file_identifier == "unsupported.xlsx")
      assert(plan.errors.head.error_message.contains("Unsupported file format"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroupBatch returns file-level detections for grouped files") {
    val inputDir = Files.createTempDirectory("privyspark-group-batch-")

    try {
      val file1 = inputDir.resolve("part-0001.csv")
      val file2 = inputDir.resolve("part-0002.csv")

      writeText(file1,
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(file2,
        "name,email\n" +
          "bob,bob@example.com\n")

      val group = PrivySparkApp.ScanGroup(
        directoryPath = inputDir.toString,
        format = "csv",
        schemaSignature = "email|name",
        filePaths = Seq(file1.toString, file2.toString)
      )

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val results = PrivySparkApp.scanGroupBatch(
        spark,
        inputDir.toString,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = "2026-03-05T00:00:00Z"
      )

      assert(results.nonEmpty)
      assert(results.map(_.file_identifier).toSet == Set("part-0001.csv", "part-0002.csv"))
      assert(results.forall(_.pii_type == "email"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroup falls back to file scan when group file count exceeds limit") {
    val inputDir = Files.createTempDirectory("privyspark-group-fallback-")

    try {
      val file1 = inputDir.resolve("part-a.csv")
      val file2 = inputDir.resolve("part-b.csv")

      writeText(file1,
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(file2,
        "name,email\n" +
          "bob,bob@example.com\n")

      val group = PrivySparkApp.ScanGroup(
        directoryPath = inputDir.toString,
        format = "csv",
        schemaSignature = "email|name",
        filePaths = Seq(file1.toString, file2.toString)
      )

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))

      val (results, errors) = PrivySparkApp.scanGroup(
        spark,
        inputDir.toString,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = "2026-03-05T00:00:00Z",
        maxFilesPerGroupBatchScan = 1
      )

      assert(errors.isEmpty)
      assert(results.map(_.file_identifier).toSet == Set("part-a.csv", "part-b.csv"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroupBatch keeps scanning source column even when internal identifier column exists") {
    val inputDir = Files.createTempDirectory("privyspark-file-id-column-")

    try {
      val file = inputDir.resolve("part-with-internal-name.csv")
      writeText(file,
        "__privyspark_file_identifier,email\n" +
          "alpha@example.com,beta@example.com\n")

      val group = PrivySparkApp.ScanGroup(
        directoryPath = inputDir.toString,
        format = "csv",
        schemaSignature = "__privyspark_file_identifier|email",
        filePaths = Seq(file.toString)
      )

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val results = PrivySparkApp.scanGroupBatch(
        spark,
        inputDir.toString,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = "2026-03-05T00:00:00Z"
      )

      assert(results.exists(_.column_name == "__privyspark_file_identifier"))
      assert(results.exists(_.column_name == "email"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("writeReports stores scan results and errors in csv output paths") {
    val outputDir = Files.createTempDirectory("privyspark-write-reports-")

    try {
      val results = Seq(
        ScanResult(
          dataset_path = "/data/input",
          scan_timestamp = "2026-03-05T00:00:00Z",
          file_identifier = "part-0001.csv",
          column_name = "email",
          pii_type = "email",
          match_count = 3L,
          match_ratio = 0.6,
          confidence = 0.6
        ),
        ScanResult(
          dataset_path = "/data/input",
          scan_timestamp = "2026-03-05T00:00:00Z",
          file_identifier = "part-0002.csv",
          column_name = "phone",
          pii_type = "phone",
          match_count = 1L,
          match_ratio = 0.2,
          confidence = 0.2
        )
      )

      val errors = Seq(
        ScanError(
          dataset_path = "/data/input",
          scan_timestamp = "2026-03-05T00:00:00Z",
          file_identifier = "broken.csv",
          error_message = "Unsupported file format"
        )
      )

      PrivySparkApp.writeReports(spark, outputDir.toString, results, errors)

      val resultCsvDf = spark.read.option("header", "true").csv(s"${outputDir.toString}/csv/scan_results")
      val errorCsvDf = spark.read.option("header", "true").csv(s"${outputDir.toString}/csv/scan_errors")

      assert(resultCsvDf.count() == 2L)
      assert(errorCsvDf.count() == 1L)
      assert(resultCsvDf.columns.toSet.contains("file_identifier"))
      assert(errorCsvDf.columns.toSet.contains("error_message"))
    } finally {
      deleteRecursively(outputDir)
    }
  }

  private def writeText(path: Path, content: String): Unit = {
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      val stream = Files.walk(path)
      try {
        stream.sorted(Comparator.reverseOrder()).forEach(pathToDelete => Files.deleteIfExists(pathToDelete))
      } finally {
        stream.close()
      }
    }
  }
}
