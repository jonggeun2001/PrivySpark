package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.model.{PiiRule, ScanError, ScanResult}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.Comparator
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable.ArrayBuffer

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

  test("scanDirectoryStructure groups same directory CSV files when schema signature matches") {
    val inputDir = Files.createTempDirectory("privyspark-schema-group-")

    try {
      writeText(inputDir.resolve("users_a.csv"),
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(inputDir.resolve("users_b.csv"),
        "name,email\n" +
          "bob,bob@example.com\n")

      val plan = PrivySparkApp.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        "2026-03-12T00:00:00Z"
      )

      val csvGroups = plan.groups.filter(_.format == "csv")
      assert(plan.errors.isEmpty)
      assert(csvGroups.size == 1)
      assert(csvGroups.head.filePaths.map(path => new java.io.File(path).getName) == Seq("users_a.csv", "users_b.csv"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanDirectoryStructure emits debug logs for planning lifecycle") {
    val inputDir = Files.createTempDirectory("privyspark-debug-plan-")

    try {
      writeText(inputDir.resolve("users_a.csv"),
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(inputDir.resolve("users_b.csv"),
        "name,email\n" +
          "bob,bob@example.com\n")

      val logs = captureStderr {
        withDebugLoggingEnabled {
          PrivySparkApp.scanDirectoryStructure(
            spark,
            inputDir.toString,
            inputDir.toString,
            "2026-03-12T00:00:00Z"
          )
        }
      }

      assert(logs.contains("[PrivySpark][DEBUG] scan_directory_structure_start"))
      assert(logs.contains("[PrivySpark][DEBUG] scan_group_schema_split_start"))
      assert(logs.contains("[PrivySpark][DEBUG] scan_group_planned"))
      assert(logs.contains("[PrivySpark][DEBUG] scan_directory_structure_complete"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanDirectoryStructure marks a multi-file directory group to use directory identifier") {
    val inputDir = Files.createTempDirectory("privyspark-directory-identifier-plan-")
    val groupedDir = Files.createDirectories(inputDir.resolve("users"))

    try {
      writeText(groupedDir.resolve("part-0001.csv"),
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(groupedDir.resolve("part-0002.csv"),
        "name,email\n" +
          "bob,bob@example.com\n")

      val plan = PrivySparkApp.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        "2026-03-12T00:00:00Z"
      )

      val csvGroups = plan.groups.filter(_.format == "csv")
      assert(csvGroups.size == 1)
      assert(csvGroups.head.useDirectoryIdentifier)
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

  test("parseHeaderFields handles quoted commas and escaped quotes") {
    val fields = PrivySparkApp.parseHeaderFields("\"Last, First\",\"He said \"\"Hello\"\"\",email")

    assert(fields == Seq("Last, First", "He said \"Hello\"", "email"))
  }

  test("inferCsvHeaderSignature matches Spark schema signature for quoted CSV headers") {
    val inputDir = Files.createTempDirectory("privyspark-quoted-header-")

    try {
      val file = inputDir.resolve("quoted.csv")
      writeText(file,
        "\"Last, First\",\"He said \"\"Hello\"\"\",email\n" +
          "\"Alice, Kim\",greeting,alice@example.com\n")

      val fastPathSignature = PrivySparkApp.inferCsvHeaderSignature(spark, file.toString)
      val sparkSignature = PrivySparkApp.inferSchemaSignature(spark, "csv", file.toString)

      assert(fastPathSignature == sparkSignature)
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanDirectoryStructure preserves quoted CSV header whitespace in schema signatures") {
    val inputDir = Files.createTempDirectory("privyspark-quoted-header-whitespace-")

    try {
      val spacedFile = inputDir.resolve("spaced.csv")
      val compactFile = inputDir.resolve("compact.csv")

      writeText(spacedFile,
        "\" account \",email\n" +
          "alice,alice@example.com\n")
      writeText(compactFile,
        "\"account\",email\n" +
          "bob,bob@example.com\n")

      val spacedFastPath = PrivySparkApp.inferCsvHeaderSignature(spark, spacedFile.toString)
      val spacedSparkSignature = PrivySparkApp.inferSchemaSignature(spark, "csv", spacedFile.toString)
      val compactFastPath = PrivySparkApp.inferCsvHeaderSignature(spark, compactFile.toString)
      val compactSparkSignature = PrivySparkApp.inferSchemaSignature(spark, "csv", compactFile.toString)
      val plan = PrivySparkApp.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        "2026-03-13T00:00:00Z"
      )

      assert(spacedFastPath == spacedSparkSignature)
      assert(compactFastPath == compactSparkSignature)
      assert(spacedFastPath != compactFastPath)
      assert(plan.groups.filter(_.format == "csv").size == 2)
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("inferCsvHeaderSignature skips leading blank lines like Spark schema detection") {
    val inputDir = Files.createTempDirectory("privyspark-leading-blank-header-")

    try {
      val file = inputDir.resolve("blank-prefix.csv")
      writeText(file,
        "\n\nname,email\n" +
          "alice,alice@example.com\n")

      val fastPathSignature = PrivySparkApp.inferCsvHeaderSignature(spark, file.toString)
      val sparkSignature = PrivySparkApp.inferSchemaSignature(spark, "csv", file.toString)
      val plan = PrivySparkApp.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        "2026-03-13T00:00:00Z"
      )

      assert(fastPathSignature == sparkSignature)
      assert(plan.errors.isEmpty)
      assert(plan.groups.filter(_.format == "csv").size == 1)
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

  test("withFileReadRetry retries transient missing file failures") {
    var attempts = 0

    val result = PrivySparkApp.withFileReadRetry(
      spark,
      Seq("/tmp/privyspark-transient.csv"),
      operationName = "test_retry",
      maxAttempts = 2,
      retryDelayMs = 0L
    ) {
      attempts += 1
      if (attempts == 1) {
        throw new RuntimeException("Path does not exist: /tmp/privyspark-transient.csv")
      }
      "ok"
    }

    assert(result == "ok")
    assert(attempts == 2)
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

  test("scanGroupBatch retries when a transiently missing file becomes readable") {
    val inputDir = Files.createTempDirectory("privyspark-group-batch-retry-")
    val file = inputDir.resolve("part-0001.csv")
    val writerError = new AtomicReference[Throwable]()

    val writer = new Thread(new Runnable {
      override def run(): Unit = {
        try {
          Thread.sleep(50L)
          writeText(file,
            "name,email\n" +
              "alice,alice@example.com\n")
        } catch {
          case error: Throwable =>
            writerError.set(error)
        }
      }
    })

    writer.start()

    try {
      val group = PrivySparkApp.ScanGroup(
        directoryPath = inputDir.toString,
        format = "csv",
        schemaSignature = "name|email",
        filePaths = Seq(file.toString)
      )

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val results = PrivySparkApp.scanGroupBatch(
        spark,
        inputDir.toString,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = "2026-03-13T00:00:00Z"
      )

      writer.join(1000L)
      assert(writerError.get() == null, Option(writerError.get()).map(_.getMessage).getOrElse("unexpected writer error"))
      assert(results.map(_.file_identifier).toSet == Set("part-0001.csv"))
      assert(results.map(result => (result.column_name, result.match_count)).toSet == Set(("email", 1L)))
    } finally {
      writer.join(1000L)
      deleteRecursively(inputDir)
    }
  }

  test("scanWithRules uses relative file path for nested single file results") {
    val inputDir = Files.createTempDirectory("privyspark-relative-file-id-")
    val nestedDir = Files.createDirectories(inputDir.resolve("daily"))
    val timestamp = "2026-03-12T00:00:00Z"

    try {
      writeText(nestedDir.resolve("customers.csv"),
        "name,email\n" +
          "alice,alice@example.com\n")

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(errors.isEmpty)
      assert(results.map(_.file_identifier).toSet == Set("daily/customers.csv"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanWithRules uses directory identifier when a directory is a single grouped dataset") {
    val inputDir = Files.createTempDirectory("privyspark-directory-group-result-")
    val groupedDir = Files.createDirectories(inputDir.resolve("users"))
    val timestamp = "2026-03-12T00:00:00Z"

    try {
      writeText(groupedDir.resolve("part-0001.csv"),
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(groupedDir.resolve("part-0002.csv"),
        "name,email\n" +
          "bob,bob@example.com\n")

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(errors.isEmpty)
      assert(results.map(_.file_identifier).toSet == Set("users"))
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet == Set(("users", "email", 2L)))
      assert(results.forall(_.match_ratio == 1.0))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanWithRules rounds probability fields to two decimal places") {
    val inputDir = Files.createTempDirectory("privyspark-rounded-probability-")
    val timestamp = "2026-03-13T00:00:00Z"

    try {
      writeText(inputDir.resolve("customers.csv"),
        "name,email\n" +
          "alice,alice@example.com\n" +
          "bob,not-an-email\n" +
          "carol,carol@example.com\n")

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(errors.isEmpty)
      assert(results.map(_.match_ratio).toSet == Set(0.67))
      assert(results.map(_.confidence).toSet == Set(0.67))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanWithRules uses dot for the root directory group identifier to avoid nested collisions") {
    val parentDir = Files.createTempDirectory("privyspark-root-directory-group-")
    val datasetDir = Files.createDirectories(parentDir.resolve("users"))
    val nestedDir = Files.createDirectories(datasetDir.resolve("users"))
    val timestamp = "2026-03-12T00:00:00Z"

    try {
      writeText(datasetDir.resolve("root-0001.csv"),
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(datasetDir.resolve("root-0002.csv"),
        "name,email\n" +
          "bob,bob@example.com\n")
      writeText(nestedDir.resolve("nested-0001.csv"),
        "name,email\n" +
          "carol,carol@example.com\n")
      writeText(nestedDir.resolve("nested-0002.csv"),
        "name,email\n" +
          "dave,dave@example.com\n")

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = scanWithRules(datasetDir.toString, datasetDir.toString, rules, timestamp)

      assert(errors.isEmpty)
      assert(results.map(_.file_identifier).toSet == Set(".", "users"))
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set((".", "email", 2L), ("users", "email", 2L)))
    } finally {
      deleteRecursively(parentDir)
    }
  }

  test("scanWithRules keeps file identifiers when grouped directory has pre-scan errors") {
    val inputDir = Files.createTempDirectory("privyspark-directory-group-prescan-error-")
    val groupedDir = Files.createDirectories(inputDir.resolve("users"))
    val timestamp = "2026-03-12T00:00:00Z"

    try {
      writeText(groupedDir.resolve("part-0001.csv"),
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(groupedDir.resolve("part-0002.csv"),
        "name,email\n" +
          "bob,bob@example.com\n")
      writeText(groupedDir.resolve("unsupported.xlsx"), "binary-placeholder")

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val plan = PrivySparkApp.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        timestamp
      )
      val csvGroups = plan.groups.filter(_.format == "csv")

      assert(csvGroups.size == 1)
      assert(!csvGroups.head.useDirectoryIdentifier)
      assert(plan.errors.map(_.file_identifier).toSet == Set("users/unsupported.xlsx"))

      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(errors.size == 1)
      assert(results.map(_.file_identifier).toSet == Set("users/part-0001.csv", "users/part-0002.csv"))
      assert(!results.exists(_.file_identifier == "users"))
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

  test("scanGroup fallback keeps directory identifier for a grouped directory") {
    val inputDir = Files.createTempDirectory("privyspark-directory-group-fallback-")
    val groupedDir = Files.createDirectories(inputDir.resolve("users"))

    try {
      writeText(groupedDir.resolve("part-a.csv"),
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(groupedDir.resolve("part-b.csv"),
        "name,email\n" +
          "bob,bob@example.com\n")

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val plan = PrivySparkApp.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        "2026-03-12T00:00:00Z"
      )
      val group = plan.groups.head

      val (results, errors) = PrivySparkApp.scanGroup(
        spark,
        inputDir.toString,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = "2026-03-12T00:00:00Z",
        maxFilesPerGroupBatchScan = 1
      )

      assert(errors.isEmpty)
      assert(results.map(_.file_identifier).toSet == Set("users"))
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet == Set(("users", "email", 2L)))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroup fallback preserves file identifiers when a grouped directory has partial file errors") {
    val inputDir = Files.createTempDirectory("privyspark-directory-group-partial-fallback-")
    val groupedDir = Files.createDirectories(inputDir.resolve("users"))

    try {
      val existingFile = groupedDir.resolve("part-a.csv")
      val missingFile = groupedDir.resolve("part-missing.csv")

      writeText(existingFile,
        "name,email\n" +
          "alice,alice@example.com\n")

      val group = PrivySparkApp.ScanGroup(
        directoryPath = groupedDir.toString,
        format = "csv",
        schemaSignature = "name|email",
        filePaths = Seq(existingFile.toString, missingFile.toString),
        useDirectoryIdentifier = true
      )

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = PrivySparkApp.scanGroup(
        spark,
        inputDir.toString,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = "2026-03-12T00:00:00Z",
        maxFilesPerGroupBatchScan = 1
      )

      assert(errors.size == 1)
      assert(results.map(_.file_identifier).toSet == Set("users/part-a.csv"))
      assert(!results.exists(_.file_identifier == "users"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroup emits driver fallback logs when switching to file scan") {
    val inputDir = Files.createTempDirectory("privyspark-group-fallback-log-")

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
        schemaSignature = "name|email",
        filePaths = Seq(file1.toString, file2.toString)
      )

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))

      val logs = captureStderr {
        PrivySparkApp.scanGroup(
          spark,
          inputDir.toString,
          group,
          rules,
          sampleRatio = 1.0,
          timestamp = "2026-03-12T00:00:00Z",
          maxFilesPerGroupBatchScan = 1
        )
      }

      assert(logs.contains("group_scan_fallback"))
      assert(logs.contains("group_scan_fallback_execute"))
      assert(logs.contains("mode=file_scan"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroupByFile parallel should match sequential fallback results") {
    val inputDir = Files.createTempDirectory("privyspark-file-fallback-parallel-")
    val groupedDir = Files.createDirectories(inputDir.resolve("users"))
    val timestamp = "2026-03-13T00:00:00Z"

    try {
      val file1 = groupedDir.resolve("part-a.csv")
      val file2 = groupedDir.resolve("part-b.csv")
      writeText(file1,
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(file2,
        "name,email\n" +
          "bob,bob@example.com\n")

      val group = PrivySparkApp.ScanGroup(
        directoryPath = groupedDir.toString,
        format = "csv",
        schemaSignature = "name|email",
        filePaths = Seq(file1.toString, file2.toString),
        useDirectoryIdentifier = true
      )
      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))

      val sequential = PrivySparkApp.scanGroupByFile(
        spark,
        inputDir.toString,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp,
        fileParallelism = 1
      )
      val parallel = PrivySparkApp.scanGroupByFile(
        spark,
        inputDir.toString,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp,
        fileParallelism = 2
      )

      assert(normalizeResults(sequential._1) == normalizeResults(parallel._1))
      assert(normalizeErrors(sequential._2) == normalizeErrors(parallel._2))
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

  test("scanGroupBatch emits debug logs for batch scan lifecycle") {
    val inputDir = Files.createTempDirectory("privyspark-group-batch-debug-")

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
        schemaSignature = "name|email",
        filePaths = Seq(file1.toString, file2.toString)
      )

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val logs = captureStderr {
        withDebugLoggingEnabled {
          PrivySparkApp.scanGroupBatch(
            spark,
            inputDir.toString,
            group,
            rules,
            sampleRatio = 1.0,
            timestamp = "2026-03-12T00:00:00Z"
          )
        }
      }

      assert(logs.contains("[PrivySpark][DEBUG] group_scan_batch_start"))
      assert(logs.contains("[PrivySpark][DEBUG] read_source_start"))
      assert(logs.contains("[PrivySpark][DEBUG] group_scan_batch_source_ready"))
      assert(logs.contains("[PrivySpark][DEBUG] group_scan_batch_complete"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroups parallel should match sequential results") {
    val inputDir = Files.createTempDirectory("privyspark-group-parallel-scan-")
    val customersDir = Files.createDirectories(inputDir.resolve("customers"))
    val membersDir = Files.createDirectories(inputDir.resolve("members"))
    val contactsDir = Files.createDirectories(inputDir.resolve("contacts"))
    val timestamp = "2026-03-13T00:00:00Z"

    try {
      writeText(customersDir.resolve("customers.csv"),
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(membersDir.resolve("members.csv"),
        "name,phone\n" +
          "bob,010-1234-5678\n")
      writeText(contactsDir.resolve("contacts.jsonl"),
        "{\"email\":\"carol@example.com\",\"phone\":\"031-555-7777\"}\n")

      val rules = Seq(
        PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
        PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
      )
      val plan = PrivySparkApp.scanDirectoryStructure(spark, inputDir.toString, inputDir.toString, timestamp)

      val sequential = PrivySparkApp.scanGroups(
        spark,
        inputDir.toString,
        plan.groups,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp,
        groupParallelism = 1
      )
      val parallel = PrivySparkApp.scanGroups(
        spark,
        inputDir.toString,
        plan.groups,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp,
        groupParallelism = 2
      )

      assert(normalizeOutcomeResults(sequential) == normalizeOutcomeResults(parallel))
      assert(normalizeOutcomeErrors(sequential) == normalizeOutcomeErrors(parallel))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanDirectoryStructure and scanGroup detect expected pii counts from bundled dataset") {
    val datasetDir = resolveResourcePath("datasets/pii-sample")
    val timestamp = "2026-03-05T00:00:00Z"

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
    )

    val plan = PrivySparkApp.scanDirectoryStructure(
      spark,
      datasetDir.toString,
      datasetDir.toString,
      timestamp
    )

    assert(plan.errors.isEmpty)
    assert(plan.totalFiles == 2)

    val results = ArrayBuffer.empty[ScanResult]
    val errors = ArrayBuffer.empty[ScanError] ++ plan.errors

    plan.groups.foreach { group =>
      val (groupResults, groupErrors) = PrivySparkApp.scanGroup(
        spark,
        datasetDir.toString,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp
      )
      results ++= groupResults
      errors ++= groupErrors
    }

    assert(errors.isEmpty)

    val actual = results
      .map(result => ((result.file_identifier, result.column_name, result.pii_type), result.match_count))
      .toMap
    val expected = Map(
      (("customers.csv", "email", "email"), 2L),
      (("customers.csv", "phone", "phone"), 2L),
      (("events.jsonl", "user_email", "email"), 2L),
      (("events.jsonl", "contact_phone", "phone"), 2L)
    )

    assert(actual == expected)
  }

  test("scanDirectoryStructure and scanGroup detect expected pii counts from parquet and orc files") {
    val outputDir = Files.createTempDirectory("privyspark-columnar-fixture-")
    val timestamp = "2026-03-05T00:00:00Z"

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
    )

    try {
      val parquetFilePath = createColumnarDataFile(outputDir, "parquet")
      val orcFilePath = createColumnarDataFile(outputDir, "orc")

      val (parquetResults, parquetErrors) = scanWithRules(parquetFilePath, parquetFilePath, rules, timestamp)
      val (orcResults, orcErrors) = scanWithRules(orcFilePath, orcFilePath, rules, timestamp)

      assert(parquetErrors.isEmpty)
      assert(orcErrors.isEmpty)

      assert(parquetResults.map(result => (result.column_name, result.pii_type)).toSet == Set(("email", "email"), ("phone", "phone")))
      assert(orcResults.map(result => (result.column_name, result.pii_type)).toSet == Set(("email", "email"), ("phone", "phone")))
      assert(parquetResults.forall(_.match_count == 2L))
      assert(orcResults.forall(_.match_count == 2L))
      assert(parquetResults.forall(_.file_identifier.toLowerCase.endsWith(".parquet")))
      assert(orcResults.forall(_.file_identifier.toLowerCase.endsWith(".orc")))
    } finally {
      deleteRecursively(outputDir)
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
      assert(countPartFiles(outputDir.resolve("csv/scan_results")) == 1L)
      assert(countPartFiles(outputDir.resolve("csv/scan_errors")) == 1L)
      assert(countPartFiles(outputDir.resolve("parquet/scan_results")) == 1L)
      assert(countPartFiles(outputDir.resolve("parquet/scan_errors")) == 1L)
    } finally {
      deleteRecursively(outputDir)
    }
  }

  private def writeText(path: Path, content: String): Unit = {
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
  }

  private def captureStderr[A](block: => A): String = {
    val output = new ByteArrayOutputStream()
    val originalErr = System.err
    val captureErr = new PrintStream(output, true, StandardCharsets.UTF_8.name())
    try {
      System.setErr(captureErr)
      block
    } finally {
      captureErr.flush()
      System.setErr(originalErr)
    }
    output.toString(StandardCharsets.UTF_8.name())
  }

  private def withDebugLoggingEnabled[A](block: => A): A = {
    val previous = sys.props.get("privyspark.debug")
    PrivySparkApp.resetDebugCache()
    System.setProperty("privyspark.debug", "true")
    try {
      block
    } finally {
      previous match {
        case Some(value) => System.setProperty("privyspark.debug", value)
        case None => System.clearProperty("privyspark.debug")
      }
      PrivySparkApp.resetDebugCache()
    }
  }

  private def scanWithRules(
    inputPath: String,
    datasetPath: String,
    rules: Seq[PiiRule],
    timestamp: String
  ): (Seq[ScanResult], Seq[ScanError]) = {
    val plan = PrivySparkApp.scanDirectoryStructure(
      spark,
      inputPath,
      datasetPath,
      timestamp
    )

    val results = ArrayBuffer.empty[ScanResult]
    val errors = ArrayBuffer.empty[ScanError] ++ plan.errors

    plan.groups.foreach { group =>
      val (groupResults, groupErrors) = PrivySparkApp.scanGroup(
        spark,
        datasetPath,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp
      )
      results ++= groupResults
      errors ++= groupErrors
    }

    (results.toSeq, errors.toSeq)
  }

  private def createColumnarDataFile(outputDir: Path, format: String): String = {
    import spark.implicits._

    val sourceDf = Seq(
      ("alpha@example.com", "010-1111-2222", "ok"),
      ("invalid-email", "not-phone", "skip"),
      ("beta@example.com", "031-555-7777", "ok")
    ).toDF("email", "phone", "message")

    val targetDir = outputDir.resolve(s"fixture-$format")
    format match {
      case "parquet" => sourceDf.coalesce(1).write.mode("overwrite").parquet(targetDir.toString)
      case "orc" => sourceDf.coalesce(1).write.mode("overwrite").orc(targetDir.toString)
      case _ => fail(s"Unsupported columnar fixture format: $format")
    }

    findDataFile(targetDir, s".$format")
      .map(_.toString)
      .getOrElse(fail(s"Failed to locate generated $format data file under $targetDir"))
  }

  private def findDataFile(root: Path, extension: String): Option[Path] = {
    val stream = Files.walk(root)
    try {
      val iter = stream.iterator()
      var found: Option[Path] = None
      while (iter.hasNext && found.isEmpty) {
        val candidate = iter.next()
        if (Files.isRegularFile(candidate) && candidate.getFileName.toString.toLowerCase.endsWith(extension)) {
          found = Some(candidate)
        }
      }
      found
    } finally {
      stream.close()
    }
  }

  private def countPartFiles(root: Path): Long = {
    if (!Files.exists(root)) {
      0L
    } else {
      val stream = Files.walk(root)
      try {
        val iter = stream.iterator()
        var count = 0L
        while (iter.hasNext) {
          val candidate = iter.next()
          if (Files.isRegularFile(candidate) && candidate.getFileName.toString.startsWith("part-")) {
            count += 1L
          }
        }
        count
      } finally {
        stream.close()
      }
    }
  }

  private def normalizeResults(results: Seq[ScanResult]): Seq[(String, String, String, Long, Double, Double)] = {
    results
      .map(result =>
        (
          result.file_identifier,
          result.column_name,
          result.pii_type,
          result.match_count,
          result.match_ratio,
          result.confidence
        )
      )
      .sortBy(identity)
  }

  private def normalizeErrors(errors: Seq[ScanError]): Seq[(String, String)] = {
    errors
      .map(error => (error.file_identifier, error.error_message))
      .sortBy(identity)
  }

  private def normalizeOutcomeResults(
    outcomes: Seq[(PrivySparkApp.ScanGroup, Seq[ScanResult], Seq[ScanError])]
  ): Seq[(String, String, String, Long, Double, Double)] = {
    normalizeResults(outcomes.flatMap(_._2))
  }

  private def normalizeOutcomeErrors(
    outcomes: Seq[(PrivySparkApp.ScanGroup, Seq[ScanResult], Seq[ScanError])]
  ): Seq[(String, String)] = {
    normalizeErrors(outcomes.flatMap(_._3))
  }

  private def resolveResourcePath(resource: String): Path = {
    val resourceUrl = Option(getClass.getClassLoader.getResource(resource))
      .getOrElse(fail(s"Missing test resource: $resource"))
    Paths.get(resourceUrl.toURI)
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
