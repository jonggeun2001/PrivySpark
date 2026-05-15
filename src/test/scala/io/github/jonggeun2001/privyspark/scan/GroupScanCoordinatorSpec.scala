package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.PrivySparkSpecFixtures
import io.github.jonggeun2001.privyspark.detect.testing.DetectionFaultInjectors
import io.github.jonggeun2001.privyspark.model.{CachedSchemaSignature, PiiRule, ScanGroup}
import io.github.jonggeun2001.privyspark.review.ReviewScopeIdentifierCodec
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.file.{Files, Path}

@RunWith(classOf[JUnitRunner])
class GroupScanCoordinatorSpec extends AnyFunSuite with PrivySparkSpecFixtures {
  private val Timestamp = "2026-04-30T00:00:00Z"
  private val EmailRule = PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}")

  test("scanGroup runs a batch scan for an exact non-directory group") {
    val inputDir = Files.createTempDirectory("privyspark-coordinator-batch-")
    val groupedDir = Files.createDirectories(inputDir.resolve("users"))

    try {
      val left = writeCustomerCsv(groupedDir, "part-a.csv", "alice", "alice@example.com")
      val right = writeCustomerCsv(groupedDir, "part-b.csv", "bob", "bob@example.com")
      val group = exactCsvGroup(groupedDir, left, right)

      val (results, errors) = GroupScanCoordinator.scanGroup(
        spark,
        inputDir.toString,
        group,
        Seq(EmailRule),
        sampleRatio = 1.0,
        timestamp = Timestamp
      )

      assert(errors.isEmpty)
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set(
          ("users/part-a.csv", "email", 1L),
          ("users/part-b.csv", "email", 1L)
        ))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroup falls back to file scan when the batch path fails") {
    val inputDir = Files.createTempDirectory("privyspark-coordinator-batch-fallback-")
    val groupedDir = Files.createDirectories(inputDir.resolve("users"))

    try {
      val left = writeCustomerCsv(groupedDir, "part-a.csv", "alice", "alice@example.com")
      val right = writeCustomerCsv(groupedDir, "part-b.csv", "bob", "bob@example.com")
      val group = exactCsvGroup(groupedDir, left, right)

      val (results, errors) = DetectionFaultInjectors.withForcedFileBatchFailure {
        GroupScanCoordinator.scanGroup(
          spark,
          inputDir.toString,
          group,
          Seq(EmailRule),
          sampleRatio = 1.0,
          timestamp = Timestamp
        )
      }

      assert(errors.isEmpty)
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set(
          ("users/part-a.csv", "email", 1L),
          ("users/part-b.csv", "email", 1L)
        ))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroup runs a batch scan for sampled Parquet groups") {
    val inputDir = Files.createTempDirectory("privyspark-coordinator-sampled-batch-")
    val leftWriteDir = Files.createDirectory(inputDir.resolve("left-source"))
    val rightWriteDir = Files.createDirectory(inputDir.resolve("right-source"))
    val groupedDir = Files.createDirectories(inputDir.resolve("users"))

    try {
      import spark.implicits._

      Seq("alice@example.com")
        .toDF("email")
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(leftWriteDir.toString)
      Seq("bob@example.com")
        .toDF("email")
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(rightWriteDir.toString)

      val left = groupedDir.resolve("part-a.parquet")
      val right = groupedDir.resolve("part-b.parquet")
      Files.move(findDataFile(leftWriteDir, ".parquet").get, left)
      Files.move(findDataFile(rightWriteDir, ".parquet").get, right)

      val group = ScanGroup(
        directoryPath = groupedDir.toString,
        format = "parquet",
        schemaSignature = "email",
        filePaths = Seq(left.toString, right.toString),
        schemaSampled = true,
        directoryIdentifierEligible = true
      )

      val (results, errors) = GroupScanCoordinator.scanGroup(
        spark,
        inputDir.toString,
        group,
        Seq(EmailRule),
        sampleRatio = 1.0,
        timestamp = Timestamp
      )

      assert(errors.isEmpty)
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set(
          ("users/part-a.parquet", "email", 1L),
          ("users/part-b.parquet", "email", 1L)
        ))
      assert(!results.exists(_.file_identifier == "users"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroup reads hive table scan groups through spark.table and returns table-level findings") {
    val inputDir = Files.createTempDirectory("privyspark-coordinator-hive-table-")
    val tableDir = Files.createDirectories(inputDir.resolve("warehouse").resolve("finance.db").resolve("cards"))
    val firstPartition = Files.createDirectories(tableDir.resolve("dt=2026-05-01"))
    val secondPartition = Files.createDirectories(tableDir.resolve("dt=2026-05-02"))
    val viewName = s"privyspark_hive_table_${System.nanoTime()}"

    try {
      import spark.implicits._

      Seq(
        ("alice@example.com", "2026-05-01"),
        ("bob@example.com", "2026-05-02")
      ).toDF("email", "dt").createOrReplaceTempView(viewName)

      val firstFile = firstPartition.resolve("part-00000.parquet").toString
      val secondFile = secondPartition.resolve("part-00001.parquet").toString
      val group = ScanGroup(
        directoryPath = tableDir.toString,
        format = "hive_table",
        schemaSignature = viewName,
        filePaths = Seq(firstFile, secondFile),
        useDirectoryIdentifier = true,
        directoryIdentifierEligible = true,
        hiveTableFqn = viewName,
        hiveTableScan = true,
        fileSizesByKey = Map(firstFile -> 100L, secondFile -> 200L),
        fileMtimesByKey = Map(firstFile -> 11L, secondFile -> 22L)
      )

      val (results, errors) = GroupScanCoordinator.scanGroup(
        spark,
        inputDir.toString,
        group,
        Seq(EmailRule),
        sampleRatio = 1.0,
        timestamp = Timestamp
      )

      assert(errors.isEmpty)
      assert(results.size == 1)

      val result = results.head
      assert(result.file_identifier == "warehouse/finance.db/cards")
      assert(result.column_name == "email")
      assert(result.match_count == 2L)
      assert(result.sampled_row_count == 2L)
      assert(result.non_empty_value_count == 2L)
      assert(result.hive_table_fqn == viewName)
      assert(result.file_size == 300L)
      assert(result.file_mtime_epoch_ms == 22L)
      assert(!result.file_identifier.contains("dt="))
      val reviewScopeIdentifiers =
        ReviewScopeIdentifierCodec
          .decode(result.review_scope_file_identifiers)
          .fold(error => fail(error), identifiers => identifiers)
      assert(reviewScopeIdentifiers.toSet ==
        Set(
          "warehouse/finance.db/cards/dt=2026-05-01/part-00000.parquet",
          "warehouse/finance.db/cards/dt=2026-05-02/part-00001.parquet"
        ))
    } finally {
      spark.catalog.dropTempView(viewName)
      deleteRecursively(inputDir)
    }
  }

  test("prepareSampledGroupForBatchScan skips exact split for sampled text groups") {
    val group = ScanGroup(
      directoryPath = "/data/users",
      format = "text",
      schemaSignature = "value",
      filePaths = Seq("/missing/a.log", "/missing/b.log"),
      schemaSampled = true,
      directoryIdentifierEligible = true
    )

    val result = GroupScanCoordinator.prepareSampledGroupForBatchScan(
      spark,
      "/data",
      Timestamp,
      group,
      new CsvHeadCache(),
      new SchemaSignatureCache()
    )

    assert(result == Right(group))
  }

  test("prepareSampledGroupForBatchScan reuses schema signature cache during sampled batch validation") {
    val group = ScanGroup(
      directoryPath = "/data/users",
      format = "parquet",
      schemaSignature = "email",
      filePaths = Seq("/missing/a.parquet", "/missing/b.parquet"),
      schemaSampled = true,
      directoryIdentifierEligible = true
    )
    val schemaSigCache = new SchemaSignatureCache()
    group.filePaths.foreach { path =>
      schemaSigCache.getOrCompute(path, group.format) {
        CachedSchemaSignature("email", csvHasHeader = true)
      }
    }

    val result = GroupScanCoordinator.prepareSampledGroupForBatchScan(
      spark,
      "/data",
      Timestamp,
      group,
      new CsvHeadCache(),
      schemaSigCache
    )

    assert(result.map(_.filePaths) == Right(group.filePaths))
  }

  test("scanGroup exact-splits sampled Parquet groups when later files add columns") {
    val inputDir = Files.createTempDirectory("privyspark-coordinator-sampled-parquet-drift-")
    val leftWriteDir = Files.createDirectory(inputDir.resolve("left-source"))
    val rightWriteDir = Files.createDirectory(inputDir.resolve("right-source"))
    val groupedDir = Files.createDirectories(inputDir.resolve("users"))

    try {
      import spark.implicits._

      Seq("alice")
        .toDF("name")
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(leftWriteDir.toString)
      Seq("010-1234-5678")
        .toDF("phone")
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(rightWriteDir.toString)

      val left = groupedDir.resolve("part-a.parquet")
      val right = groupedDir.resolve("part-b.parquet")
      Files.move(findDataFile(leftWriteDir, ".parquet").get, left)
      Files.move(findDataFile(rightWriteDir, ".parquet").get, right)

      val group = ScanGroup(
        directoryPath = groupedDir.toString,
        format = "parquet",
        schemaSignature = "name",
        filePaths = Seq(left.toString, right.toString),
        schemaSampled = true
      )
      val phoneRule = PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")

      val (results, errors) = GroupScanCoordinator.scanGroup(
        spark,
        inputDir.toString,
        group,
        Seq(phoneRule),
        sampleRatio = 1.0,
        timestamp = Timestamp
      )

      assert(errors.isEmpty)
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set(("users/part-b.parquet", "phone", 1L)))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroup uses the directory identifier when requested") {
    val inputDir = Files.createTempDirectory("privyspark-coordinator-directory-id-")
    val groupedDir = Files.createDirectories(inputDir.resolve("users"))

    try {
      val left = writeCustomerCsv(groupedDir, "part-a.csv", "alice", "alice@example.com")
      val right = writeCustomerCsv(groupedDir, "part-b.csv", "bob", "bob@example.com")
      val group = exactCsvGroup(groupedDir, left, right).copy(useDirectoryIdentifier = true)

      val (results, errors) = GroupScanCoordinator.scanGroup(
        spark,
        inputDir.toString,
        group,
        Seq(EmailRule),
        sampleRatio = 1.0,
        timestamp = Timestamp
      )

      assert(errors.isEmpty)
      assert(results.map(result => (result.file_identifier, result.column_name, result.match_count)).toSet ==
        Set(("users", "email", 2L)))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroups returns one outcome per group") {
    val inputDir = Files.createTempDirectory("privyspark-coordinator-groups-")

    try {
      val dailyDir = Files.createDirectories(inputDir.resolve("daily"))
      val monthlyDir = Files.createDirectories(inputDir.resolve("monthly"))
      val dailyFile = writeCustomerCsv(dailyDir, "part-a.csv", "alice", "alice@example.com")
      val monthlyFile = writeCustomerCsv(monthlyDir, "part-a.csv", "bob", "bob@example.com")
      val groups = Seq(
        exactCsvGroup(dailyDir, dailyFile),
        exactCsvGroup(monthlyDir, monthlyFile)
      )

      val outcomes = GroupScanCoordinator.scanGroups(
        spark,
        inputDir.toString,
        groups,
        Seq(EmailRule),
        sampleRatio = 1.0,
        timestamp = Timestamp,
        groupParallelism = 1,
        retainPayloads = true
      )

      assert(outcomes.map(_._1.directoryPath).toSet == Set(dailyDir.toString, monthlyDir.toString))
      assert(normalizeOutcomeErrors(outcomes).isEmpty)
      assert(normalizeOutcomeResults(outcomes).map(_._1).toSet == Set("daily/part-a.csv", "monthly/part-a.csv"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  private def writeCustomerCsv(directory: Path, fileName: String, name: String, email: String): Path = {
    val file = directory.resolve(fileName)
    writeText(file,
      "name,email\n" +
        s"$name,$email\n")
    file
  }

  private def exactCsvGroup(directory: Path, files: Path*): ScanGroup =
    ScanGroup(
      directoryPath = directory.toString,
      format = "csv",
      schemaSignature = "name|email",
      filePaths = files.map(_.toString)
    )
}
