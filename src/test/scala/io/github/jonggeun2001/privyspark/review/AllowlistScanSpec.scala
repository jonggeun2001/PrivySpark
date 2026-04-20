package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.model.PiiRule
import io.github.jonggeun2001.privyspark.scan.{DirectoryScanner, GroupScanner}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

@RunWith(classOf[JUnitRunner])
class AllowlistScanSpec extends AnyFunSuite {
  private lazy val spark = SparkSession.builder()
    .appName("AllowlistScanSpec")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  test("scanGroups suppresses allowlisted file-column-pii matches") {
    val inputRoot = Files.createTempDirectory("privyspark-allowlist-scan-")

    try {
      val csvFile = inputRoot.resolve("users.csv")
      Files.write(csvFile, "name,email\nalice,alice@example.com\n".getBytes(StandardCharsets.UTF_8))
      val fingerprint = FileIdentifierResolver.resolveFingerprints(
        spark.sparkContext.hadoopConfiguration,
        inputRoot.toString,
        "users.csv"
      ).right.get.head
      val matcher = AllowlistMatcher.fromEntries(Seq(
        AllowlistEntry(
          datasetPath = inputRoot.toString,
          fileIdentifier = "users.csv",
          columnName = "email",
          piiType = "email",
          reason = "known dummy data",
          reviewer = "reviewer@example.com",
          reviewedAt = "2026-04-20T00:00:00Z",
          sourceRunId = "",
          fileSize = fingerprint.fileSize,
          fileMtimeEpochMs = fingerprint.fileMtimeEpochMs,
          fileChecksumAlgo = fingerprint.fileChecksumAlgo,
          fileChecksum = fingerprint.fileChecksum
        )
      ))
      val plan = DirectoryScanner.scanDirectoryStructure(spark, inputRoot.toString, inputRoot.toString, "2026-04-20T00:00:00Z")

      val scanned = GroupScanner.scanGroups(
        spark,
        inputRoot.toString,
        plan.groups,
        Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}")),
        sampleRatio = 1.0,
        timestamp = "2026-04-20T00:00:00Z",
        allowlistMatcher = matcher,
        allowlistInputRoot = Some(inputRoot.toString)
      )

      assert(scanned.flatMap(_._2).isEmpty)
    } finally {
      deleteRecursively(inputRoot)
    }
  }

  test("scanGroups keeps invalidated matches with review metadata when checksum changed") {
    val inputRoot = Files.createTempDirectory("privyspark-allowlist-invalidated-")

    try {
      val csvFile = inputRoot.resolve("users.csv")
      Files.write(csvFile, "name,email\nalice,alice@example.com\n".getBytes(StandardCharsets.UTF_8))
      val fingerprint = FileIdentifierResolver.resolveFingerprints(
        spark.sparkContext.hadoopConfiguration,
        inputRoot.toString,
        "users.csv"
      ).right.get.head
      val matcher = AllowlistMatcher.fromEntries(Seq(
        AllowlistEntry(
          datasetPath = inputRoot.toString,
          fileIdentifier = "users.csv",
          columnName = "email",
          piiType = "email",
          reason = "known dummy data",
          reviewer = "reviewer@example.com",
          reviewedAt = "2026-04-20T00:00:00Z",
          sourceRunId = "",
          fileSize = fingerprint.fileSize,
          fileMtimeEpochMs = fingerprint.fileMtimeEpochMs,
          fileChecksumAlgo = fingerprint.fileChecksumAlgo,
          fileChecksum = "deadbeef"
        )
      ))
      val plan = DirectoryScanner.scanDirectoryStructure(spark, inputRoot.toString, inputRoot.toString, "2026-04-20T00:00:00Z")

      val scanned = GroupScanner.scanGroups(
        spark,
        inputRoot.toString,
        plan.groups,
        Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}")),
        sampleRatio = 1.0,
        timestamp = "2026-04-20T00:00:00Z",
        allowlistMatcher = matcher,
        allowlistInputRoot = Some(inputRoot.toString)
      )

      val results = scanned.flatMap(_._2)
      assert(results.size == 1)
      assert(results.head.review_status == "false_positive")
      assert(results.head.review_reason == "known dummy data")
      assert(results.head.review_invalidated)
    } finally {
      deleteRecursively(inputRoot)
    }
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      Files.walk(path)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.deleteIfExists)
    }
  }
}
