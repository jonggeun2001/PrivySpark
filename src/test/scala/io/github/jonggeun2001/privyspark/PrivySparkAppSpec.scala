package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.model.PiiRule
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
