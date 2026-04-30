package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.report.JsonCodec.jsonString
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.charset.StandardCharsets
import java.nio.file.Files

@RunWith(classOf[JUnitRunner])
class AllowlistMatcherSpec extends AnyFunSuite {
  test("evaluate suppresses recurring hive entries without fingerprint checks") {
    val matcher = AllowlistMatcher.fromRecurringEntries(Seq(
      recurringEntry(
        scanPath = "/data",
        hiveTableFqn = "mart.customers",
        fileIdentifierPattern = ""
      )
    ))

    val evaluation = matcher.evaluate(
      datasetPath = "/data",
      hiveTableFqn = "mart.customers",
      fileIdentifier = "users-20260430.csv",
      columnName = "email",
      piiType = "email",
      fingerprints = Seq(fingerprint("users-20260430.csv", "changed"))
    )

    assert(evaluation.shouldSuppress)
    assert(!evaluation.reviewInvalidated)
  }

  test("evaluate suppresses recurring file pattern entries when hive table is absent") {
    val matcher = AllowlistMatcher.fromRecurringEntries(Seq(
      recurringEntry(
        scanPath = "/data",
        hiveTableFqn = "",
        fileIdentifierPattern = "daily/customers/*.csv"
      )
    ))

    val evaluation = matcher.evaluate(
      datasetPath = "/data",
      hiveTableFqn = "",
      fileIdentifier = "daily/customers/20260430.csv",
      columnName = "email",
      piiType = "email",
      fingerprints = Seq.empty
    )

    assert(evaluation.shouldSuppress)
  }

  test("evaluate preserves leading spaces in recurring file identifier patterns") {
    val matcher = AllowlistMatcher.fromRecurringEntries(Seq(
      recurringEntry(
        scanPath = "/data",
        hiveTableFqn = "",
        fileIdentifierPattern = " reviews/a.csv"
      )
    ))

    val evaluation = matcher.evaluate(
      datasetPath = "/data",
      hiveTableFqn = "",
      fileIdentifier = " reviews/a.csv",
      columnName = "email",
      piiType = "email",
      fingerprints = Seq.empty
    )

    assert(evaluation.shouldSuppress)
  }

  test("evaluate treats equivalent hdfs slash variants as the same scan path") {
    val matcher = AllowlistMatcher.fromRecurringEntries(Seq(
      recurringEntry(
        scanPath = "hdfs:////user/username",
        hiveTableFqn = "mart.customers",
        fileIdentifierPattern = ""
      )
    ))

    val evaluation = matcher.evaluate(
      datasetPath = "hdfs:///user/username/",
      hiveTableFqn = "mart.customers",
      fileIdentifier = "customers/part-000.parquet",
      columnName = "email",
      piiType = "email",
      fingerprints = Seq.empty
    )

    assert(evaluation.shouldSuppress)
  }

  test("evaluate ignores expired recurring entries") {
    val matcher = AllowlistMatcher.fromRecurringEntries(Seq(
      recurringEntry(
        scanPath = "/data",
        hiveTableFqn = "mart.customers",
        fileIdentifierPattern = "",
        expiresAt = "2000-01-01"
      )
    ))

    val evaluation = matcher.evaluate(
      datasetPath = "/data",
      hiveTableFqn = "mart.customers",
      fileIdentifier = "customers/part-000.parquet",
      columnName = "email",
      piiType = "email",
      fingerprints = Seq.empty
    )

    assert(!evaluation.shouldSuppress)
  }

  test("evaluate treats recurring wildcard field characters as exact values") {
    val matcher = AllowlistMatcher.fromRecurringEntries(Seq(
      recurringEntry(
        scanPath = "/data",
        hiveTableFqn = "mart.customers",
        fileIdentifierPattern = "",
        columnName = "*"
      )
    ))

    val evaluation = matcher.evaluate(
      datasetPath = "/data",
      hiveTableFqn = "mart.customers",
      fileIdentifier = "customers/part-000.parquet",
      columnName = "email",
      piiType = "email",
      fingerprints = Seq.empty
    )

    assert(!evaluation.shouldSuppress)
  }

  test("evaluate ignores legacy exact entries") {
    val matcher = AllowlistMatcher.fromEntries(Seq(
      AllowlistEntry(
        datasetPath = "/data",
        fileIdentifier = "users.csv",
        columnName = "email",
        piiType = "email",
        reason = "legacy exact",
        reviewer = "reviewer@example.com",
        reviewedAt = "2026-04-20T00:00:00Z",
        sourceRunId = "",
        fileSize = 128L,
        fileMtimeEpochMs = 1710000000000L,
        fileChecksumAlgo = "CRC32",
        fileChecksum = "a1b2c3d4"
      )
    ))

    val evaluation = matcher.evaluate("/data", "email", "email", Seq(fingerprint("users.csv", "a1b2c3d4")))

    assert(!matcher.hasExactCandidate("/data", "users.csv", "email", "email"))
    assert(!evaluation.shouldSuppress)
  }

  test("load preserves escaped string fields in recurring entries") {
    val tempFile = Files.createTempFile("privyspark-allowlist-recurring-escaped-", ".jsonl")
    val scanPath = "/data/root"
    val filePattern = """archive\"name.zip!folder\quoted\".csv"""
    val reason = """contains "quote" and \ slash"""

    try {
      val line =
        s"""{"entry_type":"recurring","scan_path":${jsonString(scanPath)},"hive_table_fqn":"","file_identifier_pattern":${jsonString(filePattern)},"column_name":"email","pii_type":"email","reason":${jsonString(reason)},"reviewer":"reviewer@example.com","reviewed_at":"2026-04-20T00:00:00Z","expires_at":"2999-12-31","source_finding_key":"finding","sample_row_count":100,"match_count":3,"non_empty_match_ratio":0.03}"""
      Files.write(tempFile, s"$line\n".getBytes(StandardCharsets.UTF_8))

      val matcher = AllowlistMatcher.load(new org.apache.hadoop.conf.Configuration(), tempFile.toAbsolutePath.toString)

      val evaluation = matcher.evaluate(
        scanPath,
        "",
        filePattern,
        "email",
        "email",
        Seq.empty
      )
      assert(evaluation.shouldSuppress)
    } finally {
      Files.deleteIfExists(tempFile)
    }
  }

  test("load treats legacy pattern entries as recurring entries") {
    val tempFile = Files.createTempFile("privyspark-pattern-allowlist-", ".jsonl")
    val line =
      """{"entry_type":"pattern","dataset_path":"/data","file_identifier":"reviews/*","column_name":"temp_*","pii_type":"email","reason":"known temporary identifiers","reviewer":"reviewer@example.com","reviewed_at":"2026-04-20T00:00:00Z","expires_at":"2999-12-31","source_finding_key":"finding"}"""

    try {
      Files.write(tempFile, s"$line\n".getBytes(StandardCharsets.UTF_8))
      val matcher = AllowlistMatcher.load(new org.apache.hadoop.conf.Configuration(), tempFile.toAbsolutePath.toString)

      val evaluation = matcher.evaluate(
        "/data",
        "",
        "reviews/a.csv",
        "temp_email",
        "email",
        Seq.empty
      )

      assert(evaluation.shouldSuppress)
      assert(matcher.size == 1)
    } finally {
      Files.deleteIfExists(tempFile)
    }
  }

  test("local allowlist fallback ignores absolute paths without a scheme") {
    val tempFile = Files.createTempFile("privyspark-allowlist-absolute-", ".jsonl")

    try {
      val method = AllowlistMatcher.getClass.getDeclaredMethod("resolveLocalAllowlistFile", classOf[String])
      method.setAccessible(true)

      val resolved = method.invoke(AllowlistMatcher, tempFile.toAbsolutePath.toString)
        .asInstanceOf[Option[java.nio.file.Path]]

      assert(resolved.isEmpty)
    } finally {
      Files.deleteIfExists(tempFile)
    }
  }

  private def recurringEntry(
    scanPath: String,
    hiveTableFqn: String,
    fileIdentifierPattern: String,
    expiresAt: String = "2999-12-31",
    columnName: String = "email",
    piiType: String = "email"
  ): RecurringAllowlistEntry =
    RecurringAllowlistEntry(
      scanPath = scanPath,
      hiveTableFqn = hiveTableFqn,
      fileIdentifierPattern = fileIdentifierPattern,
      columnName = columnName,
      piiType = piiType,
      reason = "daily dummy account column",
      reviewer = "reviewer@example.com",
      reviewedAt = "2026-04-30T00:00:00Z",
      expiresAt = expiresAt,
      sourceFindingKey = "finding",
      sampleRowCount = 1000L,
      matchCount = 12L,
      nonEmptyMatchRatio = 0.12
    )

  private def fingerprint(fileIdentifier: String, checksum: String): ResolvedFileFingerprint =
    ResolvedFileFingerprint(
      fileIdentifier = fileIdentifier,
      physicalPath = s"/data/$fileIdentifier",
      fileSize = 42L,
      fileMtimeEpochMs = 1710000000000L,
      fileChecksumAlgo = "CRC32",
      fileChecksum = checksum
    )
}
