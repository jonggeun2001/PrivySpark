package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.config.RulesetLoader
import io.github.jonggeun2001.privyspark.model.{PiiRule, PiiRuleMatchType, ScanError, ScanResult}
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.{ByteArrayOutputStream, PrintStream}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.nio.file.attribute.PosixFilePermissions
import java.util.Comparator
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.collection.mutable.ArrayBuffer
import scala.collection.concurrent.TrieMap
import scala.util.control.ControlThrowable

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

  test("executeInParallel preserves task order while allowing concurrent execution") {
    val currentRunning = new AtomicInteger(0)
    val maxLock = new AnyRef
    var maxRunning = 0

    def trackRunning(value: Int): Unit = maxLock.synchronized {
      if (value > maxRunning) {
        maxRunning = value
      }
    }

    val results = PrivySparkApp.executeInParallel(2, Seq(
      () => {
        val running = currentRunning.incrementAndGet()
        trackRunning(running)
        try {
          Thread.sleep(150L)
          "first"
        } finally {
          currentRunning.decrementAndGet()
        }
      },
      () => {
        val running = currentRunning.incrementAndGet()
        trackRunning(running)
        try {
          Thread.sleep(150L)
          "second"
        } finally {
          currentRunning.decrementAndGet()
        }
      },
      () => {
        val running = currentRunning.incrementAndGet()
        trackRunning(running)
        try {
          Thread.sleep(50L)
          "third"
        } finally {
          currentRunning.decrementAndGet()
        }
      }
    ))

    assert(results == Seq("first", "second", "third"))
    assert(maxRunning > 1)
  }

  test("renderConfiguredParallelism labels unset values as spark_conf_or_default") {
    assert(PrivySparkApp.renderConfiguredParallelism(None) == "spark_conf_or_default")
    assert(PrivySparkApp.renderConfiguredParallelism(Some(6)) == "6")
  }

  test("splitGroupBySchema exact mode splits same directory files by schema signature") {
    val inputDir = Files.createTempDirectory("privyspark-schema-plan-")

    try {
      val emailFile = inputDir.resolve("users_email.csv")
      val phoneFile = inputDir.resolve("users_phone.csv")
      writeText(emailFile,
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(phoneFile,
        "name,phone\n" +
          "bob,010-1234-5678\n")

      val group = PrivySparkApp.ScanGroup(
        directoryPath = inputDir.toString,
        format = "csv",
        schemaSignature = "",
        filePaths = Seq(emailFile.toString, phoneFile.toString)
      )
      val (splitGroups, splitErrors) = PrivySparkApp.splitGroupBySchema(
        spark,
        inputDir.toString,
        "2026-03-05T00:00:00Z",
        group
      )

      assert(splitErrors.isEmpty)
      assert(splitGroups.size == 2)
      assert(splitGroups.forall(_.filePaths.size == 1))
      assert(splitGroups.forall(!_.schemaSampled))
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
      assert(csvGroups.head.schemaSampled)
      assert(csvGroups.head.csvHasHeader)
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanDirectoryStructure skips zero-byte files when grouping supported files") {
    val inputDir = Files.createTempDirectory("privyspark-zero-byte-group-")

    try {
      writeText(inputDir.resolve("users_a.csv"),
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(inputDir.resolve("users_b.csv"),
        "name,email\n" +
          "bob,bob@example.com\n")
      writeBytes(inputDir.resolve("_SUCCESS"), Array.emptyByteArray)

      val plan = PrivySparkApp.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        "2026-04-10T00:00:00Z"
      )

      val csvGroups = plan.groups.filter(_.format == "csv")
      assert(plan.errors.isEmpty)
      assert(plan.totalFiles == 2)
      assert(plan.groups.size == 1)
      assert(csvGroups.size == 1)
      assert(csvGroups.head.filePaths.map(path => new java.io.File(path).getName) == Seq("users_a.csv", "users_b.csv"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanDirectoryStructure skips zero-byte files in parquet directories") {
    val inputDir = Files.createTempDirectory("privyspark-zero-byte-parquet-group-")
    val leftWriteDir = Files.createDirectory(inputDir.resolve("left-source"))
    val rightWriteDir = Files.createDirectory(inputDir.resolve("right-source"))
    val groupedDir = Files.createDirectories(inputDir.resolve("users"))

    try {
      import spark.implicits._

      Seq(("alice@example.com", "010-1234-5678"))
        .toDF("email", "phone")
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(leftWriteDir.toString)
      Seq(("bob@example.com", "031-555-7777"))
        .toDF("email", "phone")
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(rightWriteDir.toString)

      Files.move(findDataFile(leftWriteDir, ".parquet").get, groupedDir.resolve("part-a.parquet"))
      Files.move(findDataFile(rightWriteDir, ".parquet").get, groupedDir.resolve("part-b.parquet"))
      writeBytes(groupedDir.resolve("_SUCCESS"), Array.emptyByteArray)

      val plan = PrivySparkApp.scanDirectoryStructure(
        spark,
        groupedDir.toString,
        groupedDir.toString,
        "2026-04-10T00:00:00Z"
      )

      val parquetGroups = plan.groups.filter(_.format == "parquet")
      assert(plan.errors.isEmpty)
      assert(plan.totalFiles == 2)
      assert(plan.groups.size == 1)
      assert(parquetGroups.size == 1)
      assert(parquetGroups.head.filePaths.map(path => new java.io.File(path).getName) == Seq("part-a.parquet", "part-b.parquet"))
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

      assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[DEBUG\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] scan_directory_structure_start.*""")))
      assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[DEBUG\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] scan_directory_files_discovered.*duration_ms=\d+.*""")))
      assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[DEBUG\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] scan_directory_pre_scan_execute_start.*""")))
      assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[DEBUG\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] scan_directory_pre_scan_progress.*completed_files=\d+.*total_files=\d+.*""")))
      assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[DEBUG\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] scan_directory_pre_scan_execute_complete.*duration_ms=\d+.*""")))
      assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[DEBUG\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] scan_directory_pre_scan_collect_start.*""")))
      assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[DEBUG\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] scan_directory_pre_scan_collect_complete.*duration_ms=\d+.*""")))
      assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[DEBUG\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] scan_directory_group_build_start.*""")))
      assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[DEBUG\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] scan_directory_initial_groups_ready.*duration_ms=\d+.*""")))
      assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[DEBUG\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] scan_group_schema_sample_start.*""")))
      assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[DEBUG\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] scan_group_planned.*""")))
      assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[DEBUG\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] scan_directory_structure_complete.*""")))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanDirectoryStructure keeps the same logical plan when pre-scan file expansion runs in parallel") {
    val inputDir = Files.createTempDirectory("privyspark-prescan-parallel-plan-")

    try {
      writeText(inputDir.resolve("users.csv"),
        "name,email\n" +
          "alice,alice@example.com\n")
      createSpreadsheetFile(inputDir)
      createArchiveFile(
        inputDir.resolve("bundle.zip"),
        Seq(
          "nested/customers.csv" ->
            ("name,email\n" +
              "bob,bob@example.com\n"),
          "nested/notes.log" ->
            ("ignore me\n")
        )
      )

      val serialPlan = PrivySparkApp.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        "2026-04-10T00:00:00Z",
        preScanParallelism = 1
      )
      val parallelPlan = PrivySparkApp.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        "2026-04-10T00:00:00Z",
        preScanParallelism = 4
      )

      assert(normalizePlanGroups(serialPlan.groups) == normalizePlanGroups(parallelPlan.groups))
      assert(normalizeErrors(serialPlan.errors) == normalizeErrors(parallelPlan.errors))
      assert(serialPlan.totalFiles == parallelPlan.totalFiles)
      assert(serialPlan.directoryCount == parallelPlan.directoryCount)
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("resolveConfiguredPreScanParallelism caps large explicit values to the fixed safety ceiling") {
    val key = "spark.privyspark.preScanParallelism"
    assert(PrivySparkApp.resolveConfiguredPreScanParallelism(128, 128, key) == PrivySparkApp.maxSafePreScanParallelism)
  }

  test("scanDirectoryStructure keeps a sampled multi-file directory group on file identifiers until exact split confirms schema") {
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
      assert(!csvGroups.head.useDirectoryIdentifier)
      assert(csvGroups.head.schemaSampled)
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("splitGroupBySchema exact mode splits CSV files when header order differs") {
    val inputDir = Files.createTempDirectory("privyspark-schema-order-")

    try {
      val firstFile = inputDir.resolve("ordered_a.csv")
      val secondFile = inputDir.resolve("ordered_b.csv")
      writeText(firstFile,
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(secondFile,
        "email,name\n" +
          "bob@example.com,bob\n")

      val group = PrivySparkApp.ScanGroup(
        directoryPath = inputDir.toString,
        format = "csv",
        schemaSignature = "",
        filePaths = Seq(firstFile.toString, secondFile.toString)
      )
      val (splitGroups, splitErrors) = PrivySparkApp.splitGroupBySchema(
        spark,
        inputDir.toString,
        "2026-03-05T00:00:00Z",
        group
      )

      assert(splitErrors.isEmpty)
      assert(splitGroups.size == 2)
      assert(splitGroups.forall(_.filePaths.size == 1))
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

  test("inferCsvSchemaSignature returns header-based signature for CSV with header") {
    val inputDir = Files.createTempDirectory("privyspark-schema-signature-header-")

    try {
      val file = inputDir.resolve("header.csv")
      writeText(file,
        "name,email\n" +
          "alice,alice@example.com\n")

      assert(PrivySparkApp.inferCsvSchemaSignature(spark, file.toString) == Right(("name|email", true)))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("detectCsvHasHeader returns true for plain-text headers with common column names") {
    val inputDir = Files.createTempDirectory("privyspark-plain-text-header-csv-")

    try {
      val file = inputDir.resolve("header.csv")
      writeText(file,
        "name,city\n" +
          "alice,seoul\n")

      assert(PrivySparkApp.detectCsvHasHeader(spark, file.toString))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("detectCsvHasHeader returns true for generic plain-text headers when the first row is more header-like than the data row") {
    val inputDir = Files.createTempDirectory("privyspark-generic-header-csv-")

    try {
      val file = inputDir.resolve("header.csv")
      writeText(file,
        "foo,bar\n" +
          "alice,bob\n")

      assert(PrivySparkApp.detectCsvHasHeader(spark, file.toString))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("detectCsvHasHeader accepts header names that include trailing digits") {
    val inputDir = Files.createTempDirectory("privyspark-digit-header-csv-")

    try {
      val file = inputDir.resolve("header.csv")
      writeText(file,
        "address1,address2\n" +
          "home,office\n")

      assert(PrivySparkApp.detectCsvHasHeader(spark, file.toString))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("detectCsvHasHeader supports unicode header names") {
    val inputDir = Files.createTempDirectory("privyspark-unicode-header-csv-")

    try {
      val file = inputDir.resolve("header.csv")
      writeText(file,
        "이름,이메일\n" +
          "홍길동,hong@example.com\n")

      assert(PrivySparkApp.detectCsvHasHeader(spark, file.toString))
      assert(PrivySparkApp.inferCsvSchemaSignature(spark, file.toString) == Right(("이름|이메일", true)))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("detectCsvHasHeader returns true for plain-text headers backed by common header tokens") {
    val inputDir = Files.createTempDirectory("privyspark-common-header-token-csv-")

    try {
      val file = inputDir.resolve("header.csv")
      writeText(file,
        "maker,model\n" +
          "ford,focus\n")

      assert(PrivySparkApp.detectCsvHasHeader(spark, file.toString))
      assert(PrivySparkApp.inferCsvSchemaSignature(spark, file.toString) == Right(("maker|model", true)))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("detectCsvHasHeader returns true for unicode plain-text headers backed by common header tokens") {
    val inputDir = Files.createTempDirectory("privyspark-common-unicode-header-token-csv-")

    try {
      val file = inputDir.resolve("header.csv")
      writeText(file,
        "도시,국가\n" +
          "서울,한국\n")

      assert(PrivySparkApp.detectCsvHasHeader(spark, file.toString))
      assert(PrivySparkApp.inferCsvSchemaSignature(spark, file.toString) == Right(("도시|국가", true)))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("inferCsvSchemaSignature returns column-count signature for headerless CSV") {
    val inputDir = Files.createTempDirectory("privyspark-schema-signature-no-header-")

    try {
      val file = inputDir.resolve("headerless.csv")
      writeText(file,
        "alice,alice@example.com\n" +
          "bob,bob@example.com\n")

      assert(PrivySparkApp.inferCsvSchemaSignature(spark, file.toString) == Right(("cols:2", false)))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("detectCsvHasHeader defaults ambiguous plain-text two-row CSVs to header mode") {
    val inputDir = Files.createTempDirectory("privyspark-headerless-plain-text-csv-")

    try {
      val file = inputDir.resolve("headerless.csv")
      writeText(file,
        "alice,seoul\n" +
          "bob,busan\n")

      assert(PrivySparkApp.detectCsvHasHeader(spark, file.toString))
      assert(PrivySparkApp.inferCsvSchemaSignature(spark, file.toString) == Right(("alice|seoul", true)))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("detectCsvHasHeader defaults generic plain-text tie cases to header mode") {
    val inputDir = Files.createTempDirectory("privyspark-plain-text-tie-header-csv-")

    try {
      val file = inputDir.resolve("header.csv")
      writeText(file,
        "color,shape\n" +
          "green,round\n")

      assert(PrivySparkApp.detectCsvHasHeader(spark, file.toString))
      assert(PrivySparkApp.inferCsvSchemaSignature(spark, file.toString) == Right(("color|shape", true)))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("detectCsvHasHeader returns false for single-row headerless CSV") {
    val inputDir = Files.createTempDirectory("privyspark-single-row-headerless-csv-")

    try {
      val file = inputDir.resolve("headerless.csv")
      writeText(file, "alice,seoul\n")

      assert(!PrivySparkApp.detectCsvHasHeader(spark, file.toString))
      assert(PrivySparkApp.inferCsvSchemaSignature(spark, file.toString) == Right(("cols:2", false)))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("splitGroupBySchema exact mode preserves quoted CSV header whitespace in schema signatures") {
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
      val group = PrivySparkApp.ScanGroup(
        directoryPath = inputDir.toString,
        format = "csv",
        schemaSignature = "",
        filePaths = Seq(compactFile.toString, spacedFile.toString)
      )
      val (splitGroups, splitErrors) = PrivySparkApp.splitGroupBySchema(
        spark,
        inputDir.toString,
        "2026-03-13T00:00:00Z",
        group
      )

      assert(spacedFastPath == spacedSparkSignature)
      assert(compactFastPath == compactSparkSignature)
      assert(spacedFastPath != compactFastPath)
      assert(splitErrors.isEmpty)
      assert(splitGroups.size == 2)
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
      writeBytes(inputDir.resolve("unsupported.bin"), Array[Byte](0.toByte, 1.toByte, 2.toByte, 3.toByte))

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
      assert(plan.errors.head.file_identifier == "unsupported.bin")
      assert(plan.errors.head.error_message.contains("Unsupported file format"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanDirectoryStructure treats unsupported extension text files as text inputs") {
    val inputDir = Files.createTempDirectory("privyspark-text-fallback-plan-")

    try {
      writeText(inputDir.resolve("notes.log"),
        "alice@example.com\n" +
          "not-an-email\n" +
          "bob@example.com\n")

      val plan = PrivySparkApp.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        "2026-04-09T00:00:00Z"
      )

      assert(plan.errors.isEmpty)
      assert(plan.groups.size == 1)
      assert(plan.groups.head.format == "text")
      assert(!plan.groups.head.useDirectoryIdentifier)
      assert(plan.groups.head.filePaths.map(path => new java.io.File(path).getName) == Seq("notes.log"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanDirectoryStructure treats extensionless text files as text inputs when magic bytes do not match") {
    val inputDir = Files.createTempDirectory("privyspark-extensionless-unsupported-")

    try {
      writeText(inputDir.resolve("notes"),
        "alice@example.com\n" +
          "bob@example.com\n")

      val plan = PrivySparkApp.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        "2026-04-09T00:00:00Z"
      )

      assert(plan.errors.isEmpty)
      assert(plan.groups.size == 1)
      assert(plan.groups.head.format == "text")
      assert(!plan.groups.head.useDirectoryIdentifier)
      assert(plan.groups.head.filePaths.map(path => new java.io.File(path).getName) == Seq("notes"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanDirectoryStructure records extensionless probe read failures as file errors") {
    val inputDir = Files.createTempDirectory("privyspark-extensionless-probe-failure-")
    val unreadableFile = inputDir.resolve("locked")

    try {
      writeBytes(unreadableFile, Array[Byte](0x50.toByte, 0x41.toByte, 0x52.toByte, 0x31.toByte))
      Files.setPosixFilePermissions(unreadableFile, PosixFilePermissions.fromString("---------"))

      val plan = PrivySparkApp.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        "2026-04-09T00:00:00Z"
      )

      assert(plan.groups.isEmpty)
      assert(plan.errors.map(_.file_identifier) == Seq("locked"))
      assert(plan.errors.head.error_message.nonEmpty)
    } finally {
      if (Files.exists(unreadableFile)) {
        Files.setPosixFilePermissions(unreadableFile, PosixFilePermissions.fromString("rw-------"))
      }
      deleteRecursively(inputDir)
    }
  }

  test("scanDirectoryStructure detects extensionless parquet files when probe reads are short") {
    val outputDir = Files.createTempDirectory("privyspark-partial-read-parquet-")
    val partialPath = "partial:///fixture"

    try {
      val parquetBytes = Files.readAllBytes(Paths.get(createColumnarDataFile(outputDir, "parquet")))
      spark.sparkContext.hadoopConfiguration.set("fs.partial.impl", classOf[PartialReadFileSystem].getName)
      PartialReadFileSystem.register(partialPath, parquetBytes)

      val plan = PrivySparkApp.scanDirectoryStructure(
        spark,
        partialPath,
        partialPath,
        "2026-04-09T00:00:00Z"
      )

      assert(plan.errors.isEmpty)
      assert(plan.groups.map(_.format) == Seq("parquet"))
      assert(plan.groups.map(_.filePaths.map(path => new org.apache.hadoop.fs.Path(path).toUri.getPath)) == Seq(Seq("/fixture")))
    } finally {
      PartialReadFileSystem.clear()
      deleteRecursively(outputDir)
    }
  }

  test("scanDirectoryStructure records malformed json files without exposing Spark corrupt-record errors") {
    val inputDir = Files.createTempDirectory("privyspark-malformed-json-")

    try {
      writeText(inputDir.resolve("broken.json"),
        "{\"email\":\"alice@example.com\"\n")

      val plan = PrivySparkApp.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        "2026-04-09T00:00:00Z"
      )

      assert(plan.groups.isEmpty)
      assert(plan.errors.map(_.file_identifier) == Seq("broken.json"))
      assert(plan.errors.head.error_message.contains("Malformed json input contains only corrupt records"))
      assert(!plan.errors.head.error_message.contains("Since Spark 2.3"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("splitGroupBySchemaFast filters malformed json files from sampled groups") {
    val inputDir = Files.createTempDirectory("privyspark-fast-json-group-")

    try {
      val goodFile = inputDir.resolve("a-good.json")
      val brokenFile = inputDir.resolve("b-broken.json")
      writeText(goodFile,
        "{\"email\":\"alice@example.com\"}\n")
      writeText(brokenFile,
        "{\"email\":\"broken@example.com\"\n")

      val group = PrivySparkApp.ScanGroup(
        directoryPath = inputDir.toString,
        format = "json",
        schemaSignature = "",
        filePaths = Seq(goodFile.toString, brokenFile.toString)
      )

      val (groups, errors) = PrivySparkApp.splitGroupBySchemaFast(
        spark,
        inputDir.toString,
        "2026-04-09T00:00:00Z",
        group
      )

      assert(groups.map(_.filePaths) == Seq(Seq(goodFile.toString)))
      assert(errors.map(_.file_identifier) == Seq("b-broken.json"))
      assert(!errors.head.error_message.contains("Since Spark 2.3"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanWithRules preserves valid json column named corrupt record") {
    val inputDir = Files.createTempDirectory("privyspark-valid-corrupt-column-json-")
    val timestamp = "2026-04-09T00:00:00Z"

    try {
      writeText(inputDir.resolve("records.json"),
        "{\"_corrupt_record\":\"alice@example.com\"}\n")

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = scanWithRules(
        inputDir.toString,
        inputDir.toString,
        rules,
        timestamp
      )

      assert(errors.isEmpty)
      assert(results.exists(result =>
        result.file_identifier == "records.json" &&
          result.column_name == "_corrupt_record" &&
          result.pii_type == "email"
      ))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("inferSchemaSignature ignores internal corrupt json columns for mixed files") {
    val inputDir = Files.createTempDirectory("privyspark-mixed-json-schema-")

    try {
      val mixedFile = inputDir.resolve("mixed.json")
      val cleanFile = inputDir.resolve("clean.json")
      writeText(mixedFile,
        "{\"email\":\"alice@example.com\"}\n" +
          "{\"email\":\"broken@example.com\"\n")
      writeText(cleanFile,
        "{\"email\":\"bob@example.com\"}\n")

      assert(PrivySparkApp.inferSchemaSignature(spark, "json", mixedFile.toString) == Right("email"))
      assert(PrivySparkApp.inferSchemaSignature(spark, "json", cleanFile.toString) == Right("email"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanWithRules ignores malformed json payloads when valid rows are present") {
    val inputDir = Files.createTempDirectory("privyspark-mixed-json-payload-")
    val timestamp = "2026-04-09T00:00:00Z"

    try {
      writeText(inputDir.resolve("records.json"),
        "{\"email\":\"alice@example.com\"}\n" +
          "{\"email\":\"broken@example.com\"\n")

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = scanWithRules(
        inputDir.toString,
        inputDir.toString,
        rules,
        timestamp
      )

      assert(errors.isEmpty)
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)) == Seq(
        ("records.json", "email", 1L)
      ))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanDirectoryStructure keeps a single CSV file unsampled and preserves headerless mode") {
    val inputDir = Files.createTempDirectory("privyspark-single-headerless-csv-")
    val timestamp = "2026-03-13T00:00:00Z"

    try {
      writeText(inputDir.resolve("customers.csv"),
        "alice,alice@example.com\n" +
          "bob,bob@example.com\n")

      val plan = PrivySparkApp.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        timestamp
      )

      val group = plan.groups.head
      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = PrivySparkApp.scanGroup(
        spark,
        inputDir.toString,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp
      )

      assert(plan.errors.isEmpty)
      assert(plan.groups.size == 1)
      assert(!group.schemaSampled)
      assert(!group.csvHasHeader)
      assert(group.schemaSignature == "cols:2")
      assert(errors.isEmpty)
      assert(results.map(_.column_name).toSet == Set("_c1"))
      assert(results.forall(_.file_identifier == "customers.csv"))
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

  test("scanGroup uses directory identifier when a directory is an exact-confirmed grouped dataset") {
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

      val group = PrivySparkApp.ScanGroup(
        directoryPath = groupedDir.toString,
        format = "csv",
        schemaSignature = "name|email",
        filePaths = Seq(
          groupedDir.resolve("part-0001.csv").toString,
          groupedDir.resolve("part-0002.csv").toString
        ),
        useDirectoryIdentifier = true
      )
      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = PrivySparkApp.scanGroup(
        spark,
        inputDir.toString,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp
      )

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

  test("scanWithRules detects Korean passport numbers without matching alphanumeric-adjacent substrings") {
    val inputDir = Files.createTempDirectory("privyspark-passport-number-")
    val timestamp = "2026-03-27T00:00:00Z"

    try {
      writeText(inputDir.resolve("travellers.csv"),
        "name,passport_no\n" +
          "alice,M12345678\n" +
          "bob,ID:M87654321\n" +
          "carol,XM12345678\n" +
          "dave,M12345678Y\n" +
          "erin,m12345678\n" +
          "frank,M1234567\n")

      val rules = Seq(RulesetLoader.load("default").find(_.piiType == "passport_number").get)
      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(errors.isEmpty)
      assert(results.map(result => (result.column_name, result.pii_type, result.match_count, result.match_ratio)).toSet ==
        Set(("passport_no", "passport_number", 2L, 0.33)))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroup uses dot for the root directory group identifier when directory aggregation is enabled") {
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
      val rootGroup = PrivySparkApp.ScanGroup(
        directoryPath = datasetDir.toString,
        format = "csv",
        schemaSignature = "name|email",
        filePaths = Seq(
          datasetDir.resolve("root-0001.csv").toString,
          datasetDir.resolve("root-0002.csv").toString
        ),
        useDirectoryIdentifier = true
      )
      val nestedGroup = PrivySparkApp.ScanGroup(
        directoryPath = nestedDir.toString,
        format = "csv",
        schemaSignature = "name|email",
        filePaths = Seq(
          nestedDir.resolve("nested-0001.csv").toString,
          nestedDir.resolve("nested-0002.csv").toString
        ),
        useDirectoryIdentifier = true
      )

      val (rootResults, rootErrors) = PrivySparkApp.scanGroup(
        spark,
        datasetDir.toString,
        rootGroup,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp
      )
      val (nestedResults, nestedErrors) = PrivySparkApp.scanGroup(
        spark,
        datasetDir.toString,
        nestedGroup,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp
      )
      val results = rootResults ++ nestedResults
      val errors = rootErrors ++ nestedErrors

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
      writeBytes(groupedDir.resolve("unsupported.bin"), Array[Byte](0.toByte, 1.toByte, 2.toByte, 3.toByte))

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
      assert(plan.errors.map(_.file_identifier).toSet == Set("users/unsupported.bin"))

      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(errors.size == 1)
      assert(results.map(_.file_identifier).toSet == Set("users/part-0001.csv", "users/part-0002.csv"))
      assert(!results.exists(_.file_identifier == "users"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroup does not fall back when group file count exceeds 1000 files") {
    val inputDir = Files.createTempDirectory("privyspark-group-fallback-")

    try {
      val file = inputDir.resolve("part-a.csv")

      writeText(file,
        "name,email\n" +
          "alice,alice@example.com\n")

      val group = PrivySparkApp.ScanGroup(
        directoryPath = inputDir.toString,
        format = "csv",
        schemaSignature = "email|name",
        filePaths = Seq.fill(1001)(file.toString)
      )

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      var scanResult = (Seq.empty[ScanResult], Seq.empty[ScanError])
      val logs = captureStderr {
        scanResult = PrivySparkApp.scanGroup(
          spark,
          inputDir.toString,
          group,
          rules,
          sampleRatio = 1.0,
          timestamp = "2026-03-05T00:00:00Z"
        )
      }
      val (results, errors) = scanResult

      assert(errors.isEmpty)
      assert(results.nonEmpty)
      assert(!logs.contains("group_size_limit_exceeded"))
      assert(!logs.contains("group_scan_fallback"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroup keeps directory identifier for a grouped directory") {
    val inputDir = Files.createTempDirectory("privyspark-directory-group-fallback-")
    val groupedDir = Files.createDirectories(inputDir.resolve("users"))

    try {
      writeText(groupedDir.resolve("part-a.csv"),
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(groupedDir.resolve("part-b.csv"),
        "name,email\n" +
          "bob,bob@example.com\n")

      val group = PrivySparkApp.ScanGroup(
        directoryPath = groupedDir.toString,
        format = "csv",
        schemaSignature = "name|email",
        filePaths = Seq(
          groupedDir.resolve("part-a.csv").toString,
          groupedDir.resolve("part-b.csv").toString
        ),
        useDirectoryIdentifier = true
      )
      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))

      val (results, errors) = PrivySparkApp.scanGroup(
        spark,
        inputDir.toString,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = "2026-03-12T00:00:00Z"
      )

      assert(errors.isEmpty)
      assert(results.map(_.file_identifier).toSet == Set("users"))
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet == Set(("users", "email", 2L)))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroupByFile re-detects CSV headers per file for sampled groups") {
    val inputDir = Files.createTempDirectory("privyspark-sampled-csv-header-fallback-")
    val groupedDir = Files.createDirectories(inputDir.resolve("users"))
    val timestamp = "2026-03-13T00:00:00Z"

    try {
      val headerFile = groupedDir.resolve("part-a.csv")
      val headerlessFile = groupedDir.resolve("part-b.csv")

      writeText(headerFile,
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(headerlessFile,
        "bob,bob@example.com\n" +
          "carol,carol@example.com\n")

      val group = PrivySparkApp.ScanGroup(
        directoryPath = groupedDir.toString,
        format = "csv",
        schemaSignature = "name|email",
        filePaths = Seq(headerFile.toString, headerlessFile.toString),
        schemaSampled = true,
        csvHasHeader = true
      )
      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))

      val (results, errors) = PrivySparkApp.scanGroupByFile(
        spark,
        inputDir.toString,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp
      )

      assert(errors.isEmpty)
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set(
          ("users/part-a.csv", "email", 1L),
          ("users/part-b.csv", "_c1", 2L)
        ))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroup exact-splits sampled mixed CSV header modes before batch scan") {
    val inputDir = Files.createTempDirectory("privyspark-sampled-csv-exact-split-")
    val groupedDir = Files.createDirectories(inputDir.resolve("users"))
    val timestamp = "2026-03-13T00:00:00Z"

    try {
      val headerFile = groupedDir.resolve("part-a.csv")
      val headerlessFile = groupedDir.resolve("part-b.csv")

      writeText(headerFile,
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(headerlessFile,
        "bob,bob@example.com\n" +
          "carol,carol@example.com\n")

      val group = PrivySparkApp.ScanGroup(
        directoryPath = groupedDir.toString,
        format = "csv",
        schemaSignature = "name|email",
        filePaths = Seq(headerFile.toString, headerlessFile.toString),
        schemaSampled = true,
        csvHasHeader = true,
        directoryIdentifierEligible = true
      )
      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))

      val (results, errors) = PrivySparkApp.scanGroup(
        spark,
        inputDir.toString,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp
      )

      assert(errors.isEmpty)
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set(
          ("users/part-a.csv", "email", 1L),
          ("users/part-b.csv", "_c1", 2L)
        ))
      assert(!results.exists(_.file_identifier == "users"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroup exact split restores directory identifier for eligible sampled CSV groups") {
    val inputDir = Files.createTempDirectory("privyspark-sampled-csv-dir-id-")
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
        useDirectoryIdentifier = false,
        directoryIdentifierEligible = true,
        schemaSampled = true,
        csvHasHeader = true
      )
      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))

      val (results, errors) = PrivySparkApp.scanGroup(
        spark,
        inputDir.toString,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp
      )

      assert(errors.isEmpty)
      assert(results.map(_.file_identifier).toSet == Set("users"))
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set(("users", "email", 2L)))
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
        timestamp = "2026-03-12T00:00:00Z"
      )

      assert(errors.size == 1)
      assert(results.map(_.file_identifier).toSet == Set("users/part-a.csv"))
      assert(!results.exists(_.file_identifier == "users"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroup emits driver fallback logs when batch read failure switches to file scan") {
    val inputDir = Files.createTempDirectory("privyspark-group-fallback-log-")
    val groupedDir = Files.createDirectories(inputDir.resolve("users"))

    try {
      val file1 = groupedDir.resolve("part-a.csv")
      val file2 = groupedDir.resolve("part-missing.csv")

      writeText(file1,
        "name,email\n" +
          "alice,alice@example.com\n")

      val group = PrivySparkApp.ScanGroup(
        directoryPath = groupedDir.toString,
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
          timestamp = "2026-03-12T00:00:00Z"
        )
      }

      assert(logs.contains("group_scan_fallback"))
      assert(logs.contains("group_scan_fallback_execute"))
      assert(logs.contains("mode=file_scan"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroup resplits a sampled parquet group when batched read fails") {
    val inputDir = Files.createTempDirectory("privyspark-sampled-schema-fallback-")
    val leftWriteDir = Files.createDirectory(inputDir.resolve("left-source"))
    val rightWriteDir = Files.createDirectory(inputDir.resolve("right-source"))
    val timestamp = "2026-03-13T00:00:00Z"

    try {
      import spark.implicits._

      Seq(("alice@example.com", "010-1234-5678"))
        .toDF("email", "phone")
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(leftWriteDir.toString)
      Seq((42, 30))
        .toDF("email", "age")
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(rightWriteDir.toString)

      val parquetFileA = findDataFile(leftWriteDir, ".parquet").get
      val parquetFileB = findDataFile(rightWriteDir, ".parquet").get
      val groupedDir = Files.createDirectory(inputDir.resolve("grouped"))
      Files.move(parquetFileA, groupedDir.resolve("part-a.parquet"))
      Files.move(parquetFileB, groupedDir.resolve("part-b.parquet"))

      val plan = PrivySparkApp.scanDirectoryStructure(
        spark,
        groupedDir.toString,
        groupedDir.toString,
        timestamp
      )
      val group = plan.groups.head
      val rules = Seq(
        PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
        PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
      )

      val (results, errors) = PrivySparkApp.scanGroup(
        spark,
        groupedDir.toString,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp
      )

      assert(group.schemaSampled)
      assert(!group.useDirectoryIdentifier)
      assert(errors.isEmpty)
      assert(results.map(_.file_identifier).toSet == Set("part-a.parquet"))
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set(("part-a.parquet", "email", 1L), ("part-a.parquet", "phone", 1L)))
      assert(!results.exists(_.file_identifier == "."))
      assert(!results.exists(_.file_identifier == "grouped"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroup exact split restores directory identifier for eligible sampled parquet groups") {
    val inputDir = Files.createTempDirectory("privyspark-sampled-parquet-dir-id-")
    val leftWriteDir = Files.createDirectory(inputDir.resolve("left-source"))
    val rightWriteDir = Files.createDirectory(inputDir.resolve("right-source"))
    val groupedDir = Files.createDirectories(inputDir.resolve("users"))
    val timestamp = "2026-03-13T00:00:00Z"

    try {
      import spark.implicits._

      Seq(("alice@example.com", "010-1234-5678", "ok"))
        .toDF("email", "phone", "message")
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(leftWriteDir.toString)
      Seq(("bob@example.com", "031-555-7777", "ok"))
        .toDF("email", "phone", "message")
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(rightWriteDir.toString)

      Files.move(findDataFile(leftWriteDir, ".parquet").get, groupedDir.resolve("part-a.parquet"))
      Files.move(findDataFile(rightWriteDir, ".parquet").get, groupedDir.resolve("part-b.parquet"))

      val plan = PrivySparkApp.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        timestamp
      )
      val group = plan.groups.find(group =>
        group.format == "parquet" && group.filePaths.exists(_.endsWith("part-a.parquet"))
      ).getOrElse(fail("Expected parquet group containing part-a.parquet"))
      val rules = Seq(
        PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
        PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
      )

      val (results, errors) = PrivySparkApp.scanGroup(
        spark,
        inputDir.toString,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp
      )

      assert(group.schemaSampled)
      assert(!group.useDirectoryIdentifier)
      assert(errors.isEmpty)
      assert(results.map(_.file_identifier).toSet == Set("users"))
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set(("users", "email", 2L), ("users", "phone", 2L)))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroup resplit preserves file identifiers when directory aggregation was already disabled") {
    val inputDir = Files.createTempDirectory("privyspark-sampled-resplit-no-dir-id-")
    val groupedDir = Files.createDirectories(inputDir.resolve("users"))
    val timestamp = "2026-03-13T00:00:00Z"

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
        schemaSampled = true,
        csvHasHeader = true,
        useDirectoryIdentifier = false
      )
      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))

      val (results, errors) = PrivySparkApp.scanGroup(
        spark,
        inputDir.toString,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp
      )

      assert(errors.size == 1)
      assert(results.map(_.file_identifier).toSet == Set("users/part-a.csv"))
      assert(!results.exists(_.file_identifier == "users"))
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

      assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[DEBUG\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] group_scan_batch_start.*""")))
      assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[DEBUG\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] read_source_start.*""")))
      assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[DEBUG\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] group_scan_batch_source_ready.*""")))
      assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[DEBUG\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] group_scan_batch_complete.*""")))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("runMain emits structured scan_failed logs when Spark bootstrap fails") {
    val logs = captureStderr {
      val exit = intercept[ExitCalled] {
        withDriverLogLevel("off") {
          PrivySparkApp.runMain(
            Array("--path", "/data/input", "--output", "/data/output"),
            createSparkSession = () => throw new RuntimeException("spark bootstrap failed"),
            exitWith = code => throw ExitCalled(code)
          )
        }
      }

      assert(exit.code == 1)
    }

    assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[ERROR\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] scan_failed.*reason="spark bootstrap failed".*""")))
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

  test("scanWithRules detects extensionless parquet and orc files via magic bytes") {
    val outputDir = Files.createTempDirectory("privyspark-columnar-magic-fixture-")
    val timestamp = "2026-04-09T00:00:00Z"

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
    )

    try {
      val parquetFilePath = Paths.get(createColumnarDataFile(outputDir, "parquet"))
      val orcFilePath = Paths.get(createColumnarDataFile(outputDir, "orc"))
      val parquetWithoutExtension = Files.move(parquetFilePath, outputDir.resolve("parquet-fixture"))
      val orcWithoutExtension = Files.move(orcFilePath, outputDir.resolve("orc-fixture"))

      val (parquetResults, parquetErrors) = scanWithRules(
        parquetWithoutExtension.toString,
        parquetWithoutExtension.toString,
        rules,
        timestamp
      )
      val (orcResults, orcErrors) = scanWithRules(
        orcWithoutExtension.toString,
        orcWithoutExtension.toString,
        rules,
        timestamp
      )

      assert(parquetErrors.isEmpty)
      assert(orcErrors.isEmpty)
      assert(parquetResults.map(result => (result.column_name, result.pii_type)).toSet == Set(("email", "email"), ("phone", "phone")))
      assert(orcResults.map(result => (result.column_name, result.pii_type)).toSet == Set(("email", "email"), ("phone", "phone")))
      assert(parquetResults.forall(_.match_count == 2L))
      assert(orcResults.forall(_.match_count == 2L))
      assert(parquetResults.forall(_.file_identifier == "parquet-fixture"))
      assert(orcResults.forall(_.file_identifier == "orc-fixture"))
    } finally {
      deleteRecursively(outputDir)
    }
  }

  test("scanWithRules detects expected pii counts from avro files") {
    val outputDir = Files.createTempDirectory("privyspark-avro-fixture-")
    val timestamp = "2026-04-09T00:00:00Z"

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
    )

    try {
      val avroFilePath = createColumnarDataFile(outputDir, "avro")
      val (results, errors) = scanWithRules(avroFilePath, avroFilePath, rules, timestamp)

      assert(errors.isEmpty)
      assert(results.map(result => (result.column_name, result.pii_type)).toSet == Set(("email", "email"), ("phone", "phone")))
      assert(results.forall(_.match_count == 2L))
      assert(results.forall(_.file_identifier.toLowerCase.endsWith(".avro")))
    } finally {
      deleteRecursively(outputDir)
    }
  }

  test("scanWithRules detects expected pii counts from xlsx sheets") {
    val outputDir = Files.createTempDirectory("privyspark-xlsx-fixture-")
    val timestamp = "2026-04-09T00:00:00Z"

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
    )

    try {
      val workbookPath = createSpreadsheetFile(outputDir)
      val (results, errors) = scanWithRules(workbookPath, workbookPath, rules, timestamp)

      assert(errors.isEmpty)
      assert(results.map(result => (result.file_identifier, result.column_name, result.pii_type, result.match_count)).toSet ==
        Set(
          ("contacts.xlsx#Contacts", "email", "email", 2L),
          ("contacts.xlsx#Contacts", "phone", "phone", 2L)
        ))
    } finally {
      deleteRecursively(outputDir)
    }
  }

  test("scanWithRules expands zip archives and keeps nested identifiers") {
    val inputDir = Files.createTempDirectory("privyspark-zip-fixture-")
    val timestamp = "2026-04-09T00:00:00Z"

    try {
      val archivePath = createArchiveFile(
        inputDir.resolve("bundle.zip"),
        Seq(
          "nested/customers.csv" ->
            ("name,email\n" +
              "alice,alice@example.com\n" +
              "bob,bob@example.com\n")
        )
      )
      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(errors.isEmpty)
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set(("bundle.zip!nested/customers.csv", "email", 2L)))
      assert(results.forall(_.dataset_path == inputDir.toString))
      assert(archivePath.endsWith(".zip"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanWithRules expands archive entries without extensions when parquet magic bytes match") {
    val inputDir = Files.createTempDirectory("privyspark-zip-parquet-magic-fixture-")
    val payloadDir = Files.createTempDirectory("privyspark-zip-parquet-payload-")
    val timestamp = "2026-04-09T00:00:00Z"

    try {
      val parquetPayload = Files.readAllBytes(Paths.get(createColumnarDataFile(payloadDir, "parquet")))
      createArchiveFileWithBytes(
        inputDir.resolve("bundle.zip"),
        Seq("customers" -> parquetPayload)
      )

      val rules = Seq(
        PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
        PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
      )
      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(errors.isEmpty)
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set(
          ("bundle.zip!customers", "email", 2L),
          ("bundle.zip!customers", "phone", 2L)
        ))
    } finally {
      deleteRecursively(payloadDir)
      deleteRecursively(inputDir)
    }
  }

  test("scanWithRules scans archive entries with unsupported extensions as text when content is text-like") {
    val inputDir = Files.createTempDirectory("privyspark-zip-unsupported-extension-fixture-")
    val timestamp = "2026-04-10T00:00:00Z"

    try {
      createArchiveFile(
        inputDir.resolve("bundle.zip"),
        Seq(
          "notes.log" ->
            ("alice@example.com\n" +
              "bob@example.com\n")
        )
      )

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(errors.isEmpty)
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set(("bundle.zip!notes.log", "value", 2L)))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanDirectoryStructure skips zero-byte archive entries") {
    val inputDir = Files.createTempDirectory("privyspark-zip-zero-byte-entry-")

    try {
      createArchiveFileWithBytes(
        inputDir.resolve("bundle.zip"),
        Seq(
          "_SUCCESS" -> Array.emptyByteArray,
          "nested/customers.csv" ->
            ("name,email\n" +
              "alice,alice@example.com\n").getBytes(StandardCharsets.UTF_8)
        )
      )

      val plan = PrivySparkApp.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        "2026-04-10T00:00:00Z"
      )

      assert(plan.errors.isEmpty)
      assert(plan.groups.size == 1)
      assert(plan.groups.head.format == "csv")
      assert(plan.groups.head.logicalIdentifiersByKey.values.toSeq == Seq("bundle.zip!nested/customers.csv"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanDirectoryStructure skips zero-byte archive entries before recognized extension handling") {
    val inputDir = Files.createTempDirectory("privyspark-zip-zero-byte-recognized-entry-")

    try {
      createArchiveFileWithBytes(
        inputDir.resolve("bundle.zip"),
        Seq(
          "nested/empty.zip" -> Array.emptyByteArray,
          "good.csv" ->
            ("name,email\n" +
              "alice,alice@example.com\n").getBytes(StandardCharsets.UTF_8)
        )
      )

      val plan = PrivySparkApp.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        "2026-04-10T00:00:00Z"
      )

      assert(plan.errors.isEmpty)
      assert(plan.groups.size == 1)
      assert(plan.groups.head.format == "csv")
      assert(plan.groups.head.logicalIdentifiersByKey.values.toSeq == Seq("bundle.zip!good.csv"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanDirectoryStructure skips zero-byte archive entries before unsafe path errors") {
    val inputDir = Files.createTempDirectory("privyspark-zip-zero-byte-unsafe-entry-")

    try {
      createArchiveFileWithBytes(
        inputDir.resolve("bundle.zip"),
        Seq(
          "../empty.csv" -> Array.emptyByteArray,
          "good.csv" ->
            ("name,email\n" +
              "alice,alice@example.com\n").getBytes(StandardCharsets.UTF_8)
        )
      )

      val plan = PrivySparkApp.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        "2026-04-10T00:00:00Z"
      )

      assert(plan.errors.isEmpty)
      assert(plan.groups.size == 1)
      assert(plan.groups.head.format == "csv")
      assert(plan.groups.head.logicalIdentifiersByKey.values.toSeq == Seq("bundle.zip!good.csv"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanWithRules expands zip archives when entry filenames contain hash characters") {
    val inputDir = Files.createTempDirectory("privyspark-zip-hash-fixture-")
    val timestamp = "2026-04-09T00:00:00Z"

    try {
      createArchiveFile(
        inputDir.resolve("bundle.zip"),
        Seq(
          "nested/users#2024.csv" ->
            ("name,email\n" +
              "alice,alice@example.com\n" +
              "bob,bob@example.com\n")
        )
      )

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(errors.isEmpty)
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set(("bundle.zip!nested/users#2024.csv", "email", 2L)))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanWithRules preserves logical identifiers for malformed archive json entries") {
    val inputDir = Files.createTempDirectory("privyspark-zip-json-errors-")
    val timestamp = "2026-04-09T00:00:00Z"

    try {
      createArchiveFile(
        inputDir.resolve("bundle.zip"),
        Seq(
          "good.json" -> "{\"email\":\"alice@example.com\"}\n",
          "broken.json" -> "{\"email\":\"broken@example.com\"\n"
        )
      )

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set(("bundle.zip!good.json", "email", 1L)))
      assert(errors.map(_.file_identifier) == Seq("bundle.zip!broken.json"))
      assert(errors.head.error_message.contains("Malformed json input contains only corrupt records"))
      assert(!errors.head.file_identifier.contains(".privyspark-staging"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanWithRules rejects archive entries with dot-dot segments instead of overwriting staged files") {
    val inputDir = Files.createTempDirectory("privyspark-zip-dotdot-")
    val timestamp = "2026-04-09T00:00:00Z"

    try {
      createArchiveFile(
        inputDir.resolve("bundle.zip"),
        Seq(
          "report.csv" ->
            ("name,email\n" +
              "alice,alice@example.com\n"),
          "nested/../report.csv" ->
            ("name,email\n" +
              "mallory,mallory@example.com\n")
        )
      )

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set(("bundle.zip!report.csv", "email", 1L)))
      assert(errors.map(_.file_identifier) == Seq("bundle.zip!nested/../report.csv"))
      assert(errors.head.error_message.contains("Unsafe archive entry path"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("archive pre-scan errors do not disable directory aggregation for sibling flat files") {
    val inputDir = Files.createTempDirectory("privyspark-archive-error-scope-")
    val timestamp = "2026-04-09T00:00:00Z"

    try {
      writeText(inputDir.resolve("part-a.csv"),
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(inputDir.resolve("part-b.csv"),
        "name,email\n" +
          "bob,bob@example.com\n")
      createArchiveFile(
        inputDir.resolve("bundle.zip"),
        Seq("Widget.class" -> "\u0000\u0001\u0002\u0003")
      )

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set((".", "email", 2L)))
      assert(errors.exists(_.file_identifier == "bundle.zip!Widget.class"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanWithRules preserves distinct archive identifiers for hash and percent-encoded filenames") {
    val inputDir = Files.createTempDirectory("privyspark-zip-hash-variants-")
    val timestamp = "2026-04-09T00:00:00Z"

    try {
      createArchiveFile(
        inputDir.resolve("bundle.zip"),
        Seq(
          "users#2024.csv" ->
            ("name,email\n" +
              "alice,alice@example.com\n"),
          "users%232024.csv" ->
            ("name,email\n" +
              "bob,bob@example.com\n")
        )
      )

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(errors.isEmpty)
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set(
          ("bundle.zip!users#2024.csv", "email", 1L),
          ("bundle.zip!users%232024.csv", "email", 1L)
        ))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("archive entry path conflicts do not abort later valid entries") {
    val inputDir = Files.createTempDirectory("privyspark-zip-entry-conflicts-")
    val timestamp = "2026-04-09T00:00:00Z"

    try {
      createArchiveFile(
        inputDir.resolve("bundle.zip"),
        Seq(
          "foo.json" -> "{\"email\":\"alice@example.com\"}\n",
          "foo.json/bar.csv" ->
            ("name,email\n" +
              "mallory,mallory@example.com\n"),
          "good.csv" ->
            ("name,email\n" +
              "bob,bob@example.com\n")
        )
      )

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set(
          ("bundle.zip!foo.json", "email", 1L),
          ("bundle.zip!good.csv", "email", 1L)
        ))
      assert(errors.map(_.file_identifier) == Seq("bundle.zip!foo.json/bar.csv"))
      assert(errors.head.error_message.contains("Archive entry parent is not a directory"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanWithRules rejects nested archives instead of expanding them recursively") {
    val inputDir = Files.createTempDirectory("privyspark-nested-archive-")
    val timestamp = "2026-04-09T00:00:00Z"

    try {
      val nestedArchiveBytes = createArchiveBytes(
        Seq(
          "customers.csv" ->
            ("name,email\n" +
              "alice,alice@example.com\n").getBytes(StandardCharsets.UTF_8)
        )
      )
      createArchiveFileWithBytes(
        inputDir.resolve("bundle.zip"),
        Seq(
          "good.csv" ->
            ("name,email\n" +
              "bob,bob@example.com\n").getBytes(StandardCharsets.UTF_8),
          "nested/inner.zip" -> nestedArchiveBytes
        )
      )

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set(("bundle.zip!good.csv", "email", 1L)))
      assert(!results.exists(_.file_identifier.contains("customers.csv")))
      assert(errors.exists(error =>
        error.file_identifier == "bundle.zip!nested/inner.zip" &&
          error.error_message.contains("Nested archive expansion is not supported")))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("workbook pre-scan errors do not disable directory aggregation for sibling flat files") {
    val inputDir = Files.createTempDirectory("privyspark-xlsx-error-scope-")
    val timestamp = "2026-04-09T00:00:00Z"

    try {
      writeText(inputDir.resolve("part-a.csv"),
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(inputDir.resolve("part-b.csv"),
        "name,email\n" +
          "bob,bob@example.com\n")
      writeText(inputDir.resolve("broken.xlsx"), "not a real workbook")

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set((".", "email", 2L)))
      assert(errors.exists(_.file_identifier == "broken.xlsx"))
      assert(errors.exists(_.error_message.contains("Workbook read failed")))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanWithRules scans unsupported extension text files through the text fallback") {
    val inputDir = Files.createTempDirectory("privyspark-text-fixture-")
    val timestamp = "2026-04-09T00:00:00Z"

    try {
      writeText(inputDir.resolve("notes.log"),
        "alice@example.com\n" +
          "skip\n" +
          "bob@example.com\n")

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(errors.isEmpty)
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set(("notes.log", "value", 2L)))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanWithRules applies full_column as full-line matching for text fallback inputs") {
    val inputDir = Files.createTempDirectory("privyspark-text-full-column-fallback-")
    val timestamp = "2026-04-10T00:00:00Z"

    try {
      writeText(inputDir.resolve("notes.log"),
        "alice@example.com\n" +
          "Contact bob@example.com now\n")

      val rules = Seq(
        PiiRule(
          "email",
          "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}",
          matchType = PiiRuleMatchType.FullColumn
        )
      )
      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(errors.isEmpty)
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count, result.match_ratio)).toSet ==
        Set(("notes.log", "value", 1L, 0.5)))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanWithRules counts resident registration full_column matches by exact value") {
    val inputDir = Files.createTempDirectory("privyspark-rrn-full-column-")
    val timestamp = "2026-04-10T00:00:00Z"

    try {
      writeText(inputDir.resolve("customers.csv"),
        "rrn\n" +
          "9707211\n" +
          "19707211\n")

      val rules = Seq(
        PiiRule(
          "resident_registration_number",
          "(?<![0-9])[0-9]{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01])(?:-[1-4](?:[0-9]{6})?|[1-4](?:[0-9]{6})?)(?![0-9])",
          matchType = PiiRuleMatchType.FullColumn
        )
      )
      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(errors.isEmpty)
      assert(results.map(result => (result.file_identifier, result.column_name, result.pii_type, result.match_count, result.match_ratio)).toSet ==
        Set(("customers.csv", "rrn", "resident_registration_number", 1L, 0.5)))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanWithRules treats unsupported extension unicode text as text when the probe ends mid-character") {
    val inputDir = Files.createTempDirectory("privyspark-text-unicode-probe-fallback-")
    val timestamp = "2026-04-10T00:00:00Z"

    try {
      val probeBoundaryPrefix = "a" * 511
      writeText(inputDir.resolve("notes.log"),
        probeBoundaryPrefix + "가\n" +
          "alice@example.com\n")

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(errors.isEmpty)
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set(("notes.log", "value", 1L)))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanWithRules keeps malformed utf-8 fallback inputs unsupported") {
    val inputDir = Files.createTempDirectory("privyspark-text-malformed-utf8-fallback-")
    val timestamp = "2026-04-10T00:00:00Z"

    try {
      val malformedBytes = "alice@example.com".getBytes(StandardCharsets.UTF_8) ++ Array(0xff.toByte)
      writeBytes(inputDir.resolve("notes.log"), malformedBytes)

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(results.isEmpty)
      assert(errors.map(_.file_identifier) == Seq("notes.log"))
      assert(errors.head.error_message.contains("Unsupported file format"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanWithRules keeps invalid utf-8 prefixes unsupported when probe ends mid-sequence") {
    val inputDir = Files.createTempDirectory("privyspark-text-invalid-utf8-prefix-fallback-")
    val timestamp = "2026-04-10T00:00:00Z"

    try {
      val invalidSuffixes = Seq(
        Array(0xe0.toByte, 0x80.toByte),
        Array(0xf4.toByte, 0x90.toByte)
      )

      invalidSuffixes.zipWithIndex.foreach { case (suffix, index) =>
        writeBytes(
          inputDir.resolve(s"notes-$index.log"),
          "alice@example.com".getBytes(StandardCharsets.UTF_8) ++ suffix
        )
      }

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(results.isEmpty)
      assert(errors.map(_.file_identifier).toSet == Set("notes-0.log", "notes-1.log"))
      assert(errors.forall(_.error_message.contains("Unsupported file format")))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanWithRules keeps archive text fallbacks with invalid utf-8 prefixes unsupported") {
    val inputDir = Files.createTempDirectory("privyspark-zip-invalid-utf8-prefix-fallback-")
    val timestamp = "2026-04-10T00:00:00Z"

    try {
      createArchiveFileWithBytes(
        inputDir.resolve("bundle.zip"),
        Seq(
          "notes.log" -> ("alice@example.com".getBytes(StandardCharsets.UTF_8) ++ Array(0xe0.toByte, 0x80.toByte))
        )
      )

      val rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"))
      val (results, errors) = scanWithRules(inputDir.toString, inputDir.toString, rules, timestamp)

      assert(results.isEmpty)
      assert(errors.map(_.file_identifier) == Seq("bundle.zip!notes.log"))
      assert(errors.head.error_message.contains("Unsupported file format"))
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

  private def writeBytes(path: Path, content: Array[Byte]): Unit = {
    Files.write(path, content)
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
    withDriverLogLevel("debug")(block)
  }

  private def withDriverLogLevel[A](level: String)(block: => A): A = {
    val previous = sys.props.get("privyspark.debug")
    PrivySparkApp.resetDebugCache()
    DriverLogger.resetCache()
    System.setProperty("privyspark.debug", level)
    try {
      block
    } finally {
      previous match {
        case Some(value) => System.setProperty("privyspark.debug", value)
        case None => System.clearProperty("privyspark.debug")
      }
      PrivySparkApp.resetDebugCache()
      DriverLogger.resetCache()
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

  private case class ExitCalled(code: Int) extends ControlThrowable

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
      case "avro" => sourceDf.coalesce(1).write.mode("overwrite").format("avro").save(targetDir.toString)
      case _ => fail(s"Unsupported columnar fixture format: $format")
    }

    findDataFile(targetDir, s".$format")
      .map(_.toString)
      .getOrElse(fail(s"Failed to locate generated $format data file under $targetDir"))
  }

  private def createSpreadsheetFile(outputDir: Path): String = {
    val workbook = new XSSFWorkbook()
    try {
      val sheet = workbook.createSheet("Contacts")
      val header = sheet.createRow(0)
      header.createCell(0).setCellValue("email")
      header.createCell(1).setCellValue("phone")

      val row1 = sheet.createRow(1)
      row1.createCell(0).setCellValue("alpha@example.com")
      row1.createCell(1).setCellValue("010-1111-2222")

      val row2 = sheet.createRow(2)
      row2.createCell(0).setCellValue("invalid-email")
      row2.createCell(1).setCellValue("not-phone")

      val row3 = sheet.createRow(3)
      row3.createCell(0).setCellValue("beta@example.com")
      row3.createCell(1).setCellValue("031-555-7777")

      val workbookPath = outputDir.resolve("contacts.xlsx")
      val outputStream = Files.newOutputStream(workbookPath)
      try {
        workbook.write(outputStream)
      } finally {
        outputStream.close()
      }
      workbookPath.toString
    } finally {
      workbook.close()
    }
  }

  private def createArchiveFile(path: Path, entries: Seq[(String, String)]): String = {
    createArchiveFileWithBytes(
      path,
      entries.map { case (entryName, content) => entryName -> content.getBytes(StandardCharsets.UTF_8) }
    )
  }

  private def createArchiveFileWithBytes(path: Path, entries: Seq[(String, Array[Byte])]): String = {
    val outputStream = new ZipOutputStream(Files.newOutputStream(path))
    try {
      entries.foreach {
        case (entryName, content) =>
          outputStream.putNextEntry(new ZipEntry(entryName))
          outputStream.write(content)
          outputStream.closeEntry()
      }
    } finally {
      outputStream.close()
    }
    path.toString
  }

  private def createArchiveBytes(entries: Seq[(String, Array[Byte])]): Array[Byte] = {
    val outputStream = new ByteArrayOutputStream()
    val zipOutputStream = new ZipOutputStream(outputStream)
    try {
      entries.foreach {
        case (entryName, content) =>
          zipOutputStream.putNextEntry(new ZipEntry(entryName))
          zipOutputStream.write(content)
          zipOutputStream.closeEntry()
      }
    } finally {
      zipOutputStream.close()
    }
    outputStream.toByteArray
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

  private def normalizePlanGroups(
    groups: Seq[PrivySparkApp.ScanGroup]
  ): Seq[(String, String, String, Seq[String], Boolean, Boolean, Boolean)] = {
    groups
      .map(group =>
        (
          group.directoryPath,
          group.format,
          group.schemaSignature,
          group.logicalIdentifiersByKey.values.toSeq.sorted,
          group.schemaSampled,
          group.csvHasHeader,
          group.allowDirectoryIdentifier
        )
      )
      .sortBy { case (directoryPath, format, schemaSignature, logicalIdentifiers, schemaSampled, csvHasHeader, allowDirectoryIdentifier) =>
        (directoryPath, format, schemaSignature, logicalIdentifiers.mkString("|"), schemaSampled, csvHasHeader, allowDirectoryIdentifier)
      }
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

object PartialReadFileSystem {
  private val fileContents = TrieMap.empty[String, Array[Byte]]
  private def key(path: String): String = new org.apache.hadoop.fs.Path(path).toUri.getPath
  private def key(path: org.apache.hadoop.fs.Path): String = path.toUri.getPath

  def register(path: String, bytes: Array[Byte]): Unit = {
    fileContents.put(key(path), bytes.clone())
  }

  def clear(): Unit = {
    fileContents.clear()
  }

  private[privyspark] def contents(path: org.apache.hadoop.fs.Path): Array[Byte] = {
    fileContents.getOrElse(key(path), throw new java.io.FileNotFoundException(path.toString))
  }
}

class PartialReadFileSystem extends org.apache.hadoop.fs.FileSystem {
  private val fsUri = URI.create("partial:///")
  private var workingDirectory: org.apache.hadoop.fs.Path = new org.apache.hadoop.fs.Path("/")

  override def getUri: URI = fsUri

  override def open(path: org.apache.hadoop.fs.Path, bufferSize: Int): org.apache.hadoop.fs.FSDataInputStream = {
    val bytes = PartialReadFileSystem.contents(path)
    new org.apache.hadoop.fs.FSDataInputStream(new org.apache.hadoop.fs.FSInputStream {
      private var position = 0

      override def read(): Int = {
        if (position >= bytes.length) -1
        else {
          val value = bytes(position) & 0xFF
          position += 1
          value
        }
      }

      override def read(buffer: Array[Byte], offset: Int, length: Int): Int = {
        if (position >= bytes.length) {
          -1
        } else {
          val chunkSize = math.min(2, math.min(length, bytes.length - position))
          System.arraycopy(bytes, position, buffer, offset, chunkSize)
          position += chunkSize
          chunkSize
        }
      }

      override def seek(targetPos: Long): Unit = {
        position = targetPos.toInt
      }

      override def getPos: Long = position.toLong

      override def seekToNewSource(targetPos: Long): Boolean = false
    })
  }

  override def create(
    path: org.apache.hadoop.fs.Path,
    permission: org.apache.hadoop.fs.permission.FsPermission,
    overwrite: Boolean,
    bufferSize: Int,
    replication: Short,
    blockSize: Long,
    progress: org.apache.hadoop.util.Progressable
  ): org.apache.hadoop.fs.FSDataOutputStream = {
    throw new UnsupportedOperationException("create is not supported")
  }

  override def append(
    path: org.apache.hadoop.fs.Path,
    bufferSize: Int,
    progress: org.apache.hadoop.util.Progressable
  ): org.apache.hadoop.fs.FSDataOutputStream = {
    throw new UnsupportedOperationException("append is not supported")
  }

  override def rename(src: org.apache.hadoop.fs.Path, dst: org.apache.hadoop.fs.Path): Boolean = false

  override def delete(path: org.apache.hadoop.fs.Path, recursive: Boolean): Boolean = false

  override def listStatus(path: org.apache.hadoop.fs.Path): Array[org.apache.hadoop.fs.FileStatus] =
    Array(getFileStatus(path))

  override def setWorkingDirectory(path: org.apache.hadoop.fs.Path): Unit = {
    workingDirectory = path
  }

  override def getWorkingDirectory: org.apache.hadoop.fs.Path = workingDirectory

  override def mkdirs(
    path: org.apache.hadoop.fs.Path,
    permission: org.apache.hadoop.fs.permission.FsPermission
  ): Boolean = false

  override def getFileStatus(path: org.apache.hadoop.fs.Path): org.apache.hadoop.fs.FileStatus = {
    val bytes = PartialReadFileSystem.contents(path)
    new org.apache.hadoop.fs.FileStatus(
      bytes.length.toLong,
      false,
      1,
      4096L,
      0L,
      path.makeQualified(fsUri, workingDirectory)
    )
  }
}
