package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.cli.ReviewApplyCliConfig
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

@RunWith(classOf[JUnitRunner])
class ReviewApplyCommandSpec extends AnyFunSuite {
  private lazy val spark = SparkSession.builder()
    .appName("ReviewApplyCommandSpec")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  test("run writes false_positive review rows to allowlist jsonl") {
    val inputRoot = Files.createTempDirectory("privyspark-review-apply-")

    try {
      Files.write(inputRoot.resolve("users.csv"), "name,email\nalice,alice@example.com\n".getBytes(StandardCharsets.UTF_8))
      val scanResultsPath = inputRoot.resolve("scan_results.csv")
      Files.write(
        scanResultsPath,
        (
          "dataset_path,file_identifier,column_name,pii_type,review_status,review_reason\n" +
            s"${inputRoot.toString},users.csv,email,email,false_positive,known dummy data\n"
        ).getBytes(StandardCharsets.UTF_8)
      )
      val allowlistPath = inputRoot.resolve("allowlist.jsonl")

      ReviewApplyCommand.run(
        spark,
        ReviewApplyCliConfig(
          scanResultsPath = scanResultsPath.toString,
          inputRoot = inputRoot.toString,
          allowlistPath = allowlistPath.toString,
          reviewer = "reviewer@example.com"
        )
      )

      val matcher = AllowlistMatcher.load(spark.sparkContext.hadoopConfiguration, allowlistPath.toString)
      assert(matcher.hasExactCandidate(inputRoot.toString, "users.csv", "email", "email"))
    } finally {
      deleteRecursively(inputRoot)
    }
  }

  test("run expands directory identifiers into direct child allowlist entries") {
    val inputRoot = Files.createTempDirectory("privyspark-review-apply-dir-")

    try {
      val reviewsDir = Files.createDirectories(inputRoot.resolve("reviews"))
      Files.write(reviewsDir.resolve("a.csv"), "id\n1\n".getBytes(StandardCharsets.UTF_8))
      Files.write(reviewsDir.resolve("b.csv"), "id\n2\n".getBytes(StandardCharsets.UTF_8))
      val scanResultsPath = inputRoot.resolve("scan_results.csv")
      Files.write(
        scanResultsPath,
        (
          "dataset_path,file_identifier,column_name,pii_type,review_status,review_reason,review_scope_file_identifiers\n" +
          s"${inputRoot.toString},reviews,resident_registration_number,rrn,false_positive,dummy data\n"
        ).getBytes(StandardCharsets.UTF_8)
      )
      val allowlistPath = inputRoot.resolve("allowlist.jsonl")

      val scopedResultsPath = inputRoot.resolve("scan_results_scoped.csv")
      Files.write(
        scopedResultsPath,
        (
          "dataset_path,file_identifier,column_name,pii_type,review_status,review_reason,review_scope_file_identifiers\n" +
            s"${inputRoot.toString},reviews,resident_registration_number,rrn,false_positive,dummy data,reviews/a.csv|reviews/b.csv\n"
        ).getBytes(StandardCharsets.UTF_8)
      )

      ReviewApplyCommand.run(
        spark,
        ReviewApplyCliConfig(
          scanResultsPath = scopedResultsPath.toString,
          inputRoot = inputRoot.toString,
          allowlistPath = allowlistPath.toString,
          reviewer = "reviewer@example.com"
        )
      )

      val matcher = AllowlistMatcher.load(spark.sparkContext.hadoopConfiguration, allowlistPath.toString)
      assert(matcher.hasExactCandidate(inputRoot.toString, "reviews/a.csv", "resident_registration_number", "rrn"))
      assert(matcher.hasExactCandidate(inputRoot.toString, "reviews/b.csv", "resident_registration_number", "rrn"))
    } finally {
      deleteRecursively(inputRoot)
    }
  }

  test("run does not write output in dry-run mode") {
    val inputRoot = Files.createTempDirectory("privyspark-review-apply-dry-run-")

    try {
      Files.write(inputRoot.resolve("users.csv"), "name,email\nalice,alice@example.com\n".getBytes(StandardCharsets.UTF_8))
      val scanResultsPath = inputRoot.resolve("scan_results.csv")
      Files.write(
        scanResultsPath,
        (
          "dataset_path,file_identifier,column_name,pii_type,review_status,review_reason\n" +
            s"${inputRoot.toString},users.csv,email,email,false_positive,known dummy data\n"
        ).getBytes(StandardCharsets.UTF_8)
      )
      val allowlistPath = inputRoot.resolve("allowlist.jsonl")

      ReviewApplyCommand.run(
        spark,
        ReviewApplyCliConfig(
          scanResultsPath = scanResultsPath.toString,
          inputRoot = inputRoot.toString,
          allowlistPath = allowlistPath.toString,
          reviewer = "reviewer@example.com",
          dryRun = true
        )
      )

      assert(!Files.exists(allowlistPath))
    } finally {
      deleteRecursively(inputRoot)
    }
  }

  test("run removes existing allowlist entries when a reviewed row is no longer false_positive") {
    val inputRoot = Files.createTempDirectory("privyspark-review-apply-reclassify-")

    try {
      Files.write(inputRoot.resolve("users.csv"), "name,email\nalice,alice@example.com\n".getBytes(StandardCharsets.UTF_8))
      val allowlistPath = inputRoot.resolve("allowlist.jsonl")

      ReviewApplyCommand.run(
        spark,
        ReviewApplyCliConfig(
          scanResultsPath = writeScanResultsCsv(
            inputRoot,
            "scan_results_false_positive.csv",
            "dataset_path,file_identifier,column_name,pii_type,review_status,review_reason\n" +
              s"${inputRoot.toString},users.csv,email,email,false_positive,known dummy data\n"
          ).toString,
          inputRoot = inputRoot.toString,
          allowlistPath = allowlistPath.toString,
          reviewer = "reviewer@example.com"
        )
      )

      ReviewApplyCommand.run(
        spark,
        ReviewApplyCliConfig(
          scanResultsPath = writeScanResultsCsv(
            inputRoot,
            "scan_results_true_positive.csv",
            "dataset_path,file_identifier,column_name,pii_type,review_status,review_reason\n" +
              s"${inputRoot.toString},users.csv,email,email,true_positive,\n"
          ).toString,
          inputRoot = inputRoot.toString,
          allowlistPath = allowlistPath.toString,
          reviewer = "reviewer@example.com"
        )
      )

      val matcher = AllowlistMatcher.load(spark.sparkContext.hadoopConfiguration, allowlistPath.toString)
      assert(!matcher.hasExactCandidate(inputRoot.toString, "users.csv", "email", "email"))
    } finally {
      deleteRecursively(inputRoot)
    }
  }

  private def writeScanResultsCsv(root: Path, fileName: String, contents: String): Path = {
    val path = root.resolve(fileName)
    Files.write(path, contents.getBytes(StandardCharsets.UTF_8))
    path
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      Files.walk(path)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.deleteIfExists)
    }
  }
}
