package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.PrivySparkSpecFixtures
import io.github.jonggeun2001.privyspark.detect.testing.DetectionFaultInjectors
import io.github.jonggeun2001.privyspark.model.{PiiRule, ScanGroup}
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

  test("scanGroup runs a batch scan for sampled CSV groups") {
    val inputDir = Files.createTempDirectory("privyspark-coordinator-sampled-batch-")
    val groupedDir = Files.createDirectories(inputDir.resolve("users"))

    try {
      val left = groupedDir.resolve("part-a.csv")
      val right = groupedDir.resolve("part-b.csv")
      writeText(left,
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(right,
        "name,email\n" +
          "bob,bob@example.com\n")

      val group = ScanGroup(
        directoryPath = groupedDir.toString,
        format = "csv",
        schemaSignature = "name|email",
        filePaths = Seq(left.toString, right.toString),
        schemaSampled = true,
        csvHasHeader = true,
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
          ("users/part-a.csv", "email", 1L),
          ("users/part-b.csv", "email", 1L)
        ))
      assert(!results.exists(_.file_identifier == "users"))
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
