package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.cli.ReviewApplyCliConfig
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.nio.file.attribute.FileTime

@RunWith(classOf[JUnitRunner])
class ReviewApplyCommandSpec extends AnyFunSuite {
  private val BaseScanResultHeader =
    "dataset_path,file_identifier,column_name,pii_type,review_status,review_reason,file_size,file_mtime_epoch_ms"
  private val BaseScanResultColumnCount = BaseScanResultHeader.count(_ == ',') + 1

  private lazy val spark = SparkSession.builder()
    .appName("ReviewApplyCommandSpec")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  test("run writes false_positive review rows to allowlist jsonl") {
    val inputRoot = Files.createTempDirectory("privyspark-review-apply-")

    try {
      val usersCsv = inputRoot.resolve("users.csv")
      Files.write(usersCsv, "name,email\nalice,alice@example.com\n".getBytes(StandardCharsets.UTF_8))
      val (fileSize, fileMtimeEpochMs) = metadataOf(usersCsv)
      val scanResultsPath = writeScanResultsCsv(
        inputRoot,
        "scan_results.csv",
        Seq(scanResultRow(
          datasetPath = inputRoot.toString,
          fileIdentifier = "users.csv",
          columnName = "email",
          piiType = "email",
          reviewStatus = ReviewStatus.FalsePositive,
          reviewReason = "known dummy data",
          fileSize = fileSize,
          fileMtimeEpochMs = fileMtimeEpochMs
        ))
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
      val firstCsv = reviewsDir.resolve("a.csv")
      val secondCsv = reviewsDir.resolve("b.csv")
      Files.write(firstCsv, "id\n1\n".getBytes(StandardCharsets.UTF_8))
      Files.write(secondCsv, "id\n2\n".getBytes(StandardCharsets.UTF_8))
      val scopeIdentifiers = Seq("reviews/a.csv", "reviews/b.csv")
      val (fileSize, fileMtimeEpochMs) = aggregateMetadata(Seq(firstCsv, secondCsv))
      val allowlistPath = inputRoot.resolve("allowlist.jsonl")
      val scopedResultsPath = writeScanResultsCsv(
        inputRoot,
        "scan_results_scoped.csv",
        Seq(scanResultRow(
          datasetPath = inputRoot.toString,
          fileIdentifier = "reviews",
          columnName = "resident_registration_number",
          piiType = "rrn",
          reviewStatus = ReviewStatus.FalsePositive,
          reviewReason = "dummy data",
          fileSize = fileSize,
          fileMtimeEpochMs = fileMtimeEpochMs,
          reviewScopeFileIdentifiers = scopeIdentifiers,
          reviewScopeFileFingerprints = scopeFingerprints(inputRoot.toString, scopeIdentifiers)
        ))
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
      val usersCsv = inputRoot.resolve("users.csv")
      Files.write(usersCsv, "name,email\nalice,alice@example.com\n".getBytes(StandardCharsets.UTF_8))
      val (fileSize, fileMtimeEpochMs) = metadataOf(usersCsv)
      val scanResultsPath = writeScanResultsCsv(
        inputRoot,
        "scan_results.csv",
        Seq(scanResultRow(
          datasetPath = inputRoot.toString,
          fileIdentifier = "users.csv",
          columnName = "email",
          piiType = "email",
          reviewStatus = ReviewStatus.FalsePositive,
          reviewReason = "known dummy data",
          fileSize = fileSize,
          fileMtimeEpochMs = fileMtimeEpochMs
        ))
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
      val usersCsv = inputRoot.resolve("users.csv")
      Files.write(usersCsv, "name,email\nalice,alice@example.com\n".getBytes(StandardCharsets.UTF_8))
      val (fileSize, fileMtimeEpochMs) = metadataOf(usersCsv)
      val allowlistPath = inputRoot.resolve("allowlist.jsonl")

      ReviewApplyCommand.run(
        spark,
        ReviewApplyCliConfig(
          scanResultsPath = writeScanResultsCsv(
            inputRoot,
            "scan_results_false_positive.csv",
            Seq(scanResultRow(
              datasetPath = inputRoot.toString,
              fileIdentifier = "users.csv",
              columnName = "email",
              piiType = "email",
              reviewStatus = ReviewStatus.FalsePositive,
              reviewReason = "known dummy data",
              fileSize = fileSize,
              fileMtimeEpochMs = fileMtimeEpochMs
            ))
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
            Seq(scanResultRow(
              datasetPath = inputRoot.toString,
              fileIdentifier = "users.csv",
              columnName = "email",
              piiType = "email",
              reviewStatus = ReviewStatus.TruePositive,
              reviewReason = "",
              fileSize = fileSize,
              fileMtimeEpochMs = fileMtimeEpochMs
            ))
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

  test("run fails fast when review_status contains an unsupported value") {
    val inputRoot = Files.createTempDirectory("privyspark-review-apply-invalid-status-")

    try {
      val usersCsv = inputRoot.resolve("users.csv")
      Files.write(usersCsv, "name,email\nalice,alice@example.com\n".getBytes(StandardCharsets.UTF_8))
      val (fileSize, fileMtimeEpochMs) = metadataOf(usersCsv)

      val error = intercept[IllegalArgumentException] {
        ReviewApplyCommand.run(
          spark,
          ReviewApplyCliConfig(
            scanResultsPath = writeScanResultsCsv(
              inputRoot,
              "scan_results_invalid_status.csv",
              Seq(scanResultRow(
                datasetPath = inputRoot.toString,
                fileIdentifier = "users.csv",
                columnName = "email",
                piiType = "email",
                reviewStatus = "false-postive",
                reviewReason = "typo",
                fileSize = fileSize,
                fileMtimeEpochMs = fileMtimeEpochMs
              ))
            ).toString,
            inputRoot = inputRoot.toString,
            allowlistPath = inputRoot.resolve("allowlist.jsonl").toString,
            reviewer = "reviewer@example.com"
          )
        )
      }

      assert(error.getMessage.contains("Unsupported review_status"))
      assert(error.getMessage.contains("false-postive"))
    } finally {
      deleteRecursively(inputRoot)
    }
  }

  test("run fails when scan result metadata is stale before writing allowlist") {
    val inputRoot = Files.createTempDirectory("privyspark-review-apply-stale-metadata-")

    try {
      val usersCsv = inputRoot.resolve("users.csv")
      Files.write(usersCsv, "name,email\nalice,alice@example.com\n".getBytes(StandardCharsets.UTF_8))
      val (fileSize, fileMtimeEpochMs) = metadataOf(usersCsv)
      val allowlistPath = inputRoot.resolve("allowlist.jsonl")

      val error = intercept[IllegalArgumentException] {
        ReviewApplyCommand.run(
          spark,
          ReviewApplyCliConfig(
            scanResultsPath = writeScanResultsCsv(
              inputRoot,
              "scan_results_stale.csv",
              Seq(scanResultRow(
                datasetPath = inputRoot.toString,
                fileIdentifier = "users.csv",
                columnName = "email",
                piiType = "email",
                reviewStatus = ReviewStatus.FalsePositive,
                reviewReason = "known dummy data",
                fileSize = fileSize + 1L,
                fileMtimeEpochMs = fileMtimeEpochMs
              ))
            ).toString,
            inputRoot = inputRoot.toString,
            allowlistPath = allowlistPath.toString,
            reviewer = "reviewer@example.com"
          )
        )
      }

      assert(error.getMessage.contains("Scan result metadata is stale"))
      assert(!Files.exists(allowlistPath))
    } finally {
      deleteRecursively(inputRoot)
    }
  }

  test("run fails when a directory scope fingerprint is stale even if aggregate metadata still matches") {
    val inputRoot = Files.createTempDirectory("privyspark-review-apply-stale-scope-fingerprint-")

    try {
      val reviewsDir = Files.createDirectories(inputRoot.resolve("reviews"))
      val firstCsv = reviewsDir.resolve("a.csv")
      val secondCsv = reviewsDir.resolve("b.csv")
      val pinnedMtime = FileTime.fromMillis(1710000000000L)
      Files.write(firstCsv, "id\n1\n".getBytes(StandardCharsets.UTF_8))
      Files.write(secondCsv, "id\n2\n".getBytes(StandardCharsets.UTF_8))
      Files.setLastModifiedTime(firstCsv, pinnedMtime)
      Files.setLastModifiedTime(secondCsv, pinnedMtime)

      val scopeIdentifiers = Seq("reviews/a.csv", "reviews/b.csv")
      val (fileSize, fileMtimeEpochMs) = aggregateMetadata(Seq(firstCsv, secondCsv))
      val originalScopeFingerprints = scopeFingerprints(inputRoot.toString, scopeIdentifiers)

      Files.write(firstCsv, "id\n9\n".getBytes(StandardCharsets.UTF_8))
      Files.setLastModifiedTime(firstCsv, pinnedMtime)

      val allowlistPath = inputRoot.resolve("allowlist.jsonl")
      val error = intercept[IllegalArgumentException] {
        ReviewApplyCommand.run(
          spark,
          ReviewApplyCliConfig(
            scanResultsPath = writeScanResultsCsv(
              inputRoot,
              "scan_results_stale_scope.csv",
              Seq(scanResultRow(
                datasetPath = inputRoot.toString,
                fileIdentifier = "reviews",
                columnName = "resident_registration_number",
                piiType = "rrn",
                reviewStatus = ReviewStatus.FalsePositive,
                reviewReason = "dummy data",
                fileSize = fileSize,
                fileMtimeEpochMs = fileMtimeEpochMs,
                reviewScopeFileIdentifiers = scopeIdentifiers,
                reviewScopeFileFingerprints = originalScopeFingerprints
              ))
            ).toString,
            inputRoot = inputRoot.toString,
            allowlistPath = allowlistPath.toString,
            reviewer = "reviewer@example.com"
          )
        )
      }

      assert(error.getMessage.contains("Scan result metadata is stale"))
      assert(!Files.exists(allowlistPath))
    } finally {
      deleteRecursively(inputRoot)
    }
  }

  private def metadataOf(path: Path): (Long, Long) = {
    Files.size(path) -> Files.getLastModifiedTime(path).toMillis
  }

  private def aggregateMetadata(paths: Seq[Path]): (Long, Long) = {
    paths.map(metadataOf).foldLeft(0L -> 0L) {
      case ((totalSize, latestMtime), (fileSize, fileMtimeEpochMs)) =>
        (totalSize + fileSize) -> math.max(latestMtime, fileMtimeEpochMs)
    }
  }

  private def scanResultRow(
    datasetPath: String,
    fileIdentifier: String,
    columnName: String,
    piiType: String,
    reviewStatus: String,
    reviewReason: String,
    fileSize: Long,
    fileMtimeEpochMs: Long,
    reviewScopeFileIdentifiers: Seq[String] = Seq.empty,
    reviewScopeFileFingerprints: String = ""
  ): String = {
    val values = Seq(
      datasetPath,
      fileIdentifier,
      columnName,
      piiType,
      reviewStatus,
      reviewReason,
      fileSize.toString,
      fileMtimeEpochMs.toString
    ) ++
      (if (reviewScopeFileIdentifiers.nonEmpty || reviewScopeFileFingerprints.nonEmpty) {
        Seq(reviewScopeFileIdentifiers.mkString("|"))
      } else {
        Seq.empty
      }) ++
      (if (reviewScopeFileFingerprints.nonEmpty) Seq(reviewScopeFileFingerprints) else Seq.empty)
    values.mkString(",")
  }

  private def writeScanResultsCsv(root: Path, fileName: String, rows: Seq[String]): Path = {
    val path = root.resolve(fileName)
    val maxColumnCount = rows.map(_.count(_ == ',') + 1).foldLeft(BaseScanResultColumnCount)(math.max)
    val header = maxColumnCount match {
      case columnCount if columnCount == BaseScanResultColumnCount =>
        BaseScanResultHeader
      case columnCount if columnCount == BaseScanResultColumnCount + 1 =>
        s"$BaseScanResultHeader,review_scope_file_identifiers"
      case _ =>
        s"$BaseScanResultHeader,review_scope_file_identifiers,review_scope_file_fingerprints"
    }
    val contents = s"$header\n${rows.mkString("\n")}\n"
    Files.write(path, contents.getBytes(StandardCharsets.UTF_8))
    path
  }

  private def scopeFingerprints(inputRoot: String, identifiers: Seq[String]): String = {
    val fingerprints = identifiers.flatMap { identifier =>
      FileIdentifierResolver.resolveFingerprints(spark.sparkContext.hadoopConfiguration, inputRoot, identifier) match {
        case Right(resolvedFingerprints) =>
          resolvedFingerprints.map(RecordedFileFingerprint.fromResolved)
        case Left(errorMessage) =>
          fail(s"Failed to resolve $identifier: $errorMessage")
      }
    }
    ReviewScopeFingerprintCodec.encode(fingerprints)
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      Files.walk(path)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.deleteIfExists)
    }
  }
}
