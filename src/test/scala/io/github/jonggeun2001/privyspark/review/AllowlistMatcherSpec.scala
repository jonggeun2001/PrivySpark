package io.github.jonggeun2001.privyspark.review

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AllowlistMatcherSpec extends AnyFunSuite {
  test("evaluate suppresses an exact file match when metadata is unchanged") {
    val fingerprint = ResolvedFileFingerprint(
      fileIdentifier = "users.csv",
      physicalPath = "/data/users.csv",
      fileSize = 128L,
      fileMtimeEpochMs = 1710000000000L,
      fileChecksumAlgo = "CRC32",
      fileChecksum = "a1b2c3d4"
    )
    val matcher = AllowlistMatcher.fromEntries(Seq(
      AllowlistEntry(
        datasetPath = "/data",
        fileIdentifier = "users.csv",
        columnName = "email",
        piiType = "email",
        reason = "known dummy data",
        reviewer = "reviewer@example.com",
        reviewedAt = "2026-04-20T00:00:00Z",
        sourceRunId = "",
        fileSize = 128L,
        fileMtimeEpochMs = 1710000000000L,
        fileChecksumAlgo = "CRC32",
        fileChecksum = "a1b2c3d4"
      )
    ))

    val evaluation = matcher.evaluate("/data", "email", "email", Seq(fingerprint))

    assert(evaluation.shouldSuppress)
    assert(!evaluation.reviewInvalidated)
  }

  test("evaluate keeps the result and marks it invalidated when checksum changed") {
    val fingerprint = ResolvedFileFingerprint(
      fileIdentifier = "users.csv",
      physicalPath = "/data/users.csv",
      fileSize = 128L,
      fileMtimeEpochMs = 1710000000000L,
      fileChecksumAlgo = "CRC32",
      fileChecksum = "deadbeef"
    )
    val matcher = AllowlistMatcher.fromEntries(Seq(
      AllowlistEntry(
        datasetPath = "/data",
        fileIdentifier = "users.csv",
        columnName = "email",
        piiType = "email",
        reason = "known dummy data",
        reviewer = "reviewer@example.com",
        reviewedAt = "2026-04-20T00:00:00Z",
        sourceRunId = "",
        fileSize = 128L,
        fileMtimeEpochMs = 1710000000000L,
        fileChecksumAlgo = "CRC32",
        fileChecksum = "a1b2c3d4"
      )
    ))

    val evaluation = matcher.evaluate("/data", "email", "email", Seq(fingerprint))

    assert(!evaluation.shouldSuppress)
    assert(evaluation.reviewInvalidated)
    assert(evaluation.reviewStatus == "false_positive")
    assert(evaluation.reviewReason == "known dummy data")
  }

  test("evaluate suppresses directory aggregates only when every child file is allowlisted") {
    val matcher = AllowlistMatcher.fromEntries(Seq(
      allowlistEntry("reviews/a.csv"),
      allowlistEntry("reviews/b.csv")
    ))

    val suppressEvaluation = matcher.evaluate(
      "/data",
      "resident_registration_number",
      "rrn",
      Seq(
        fingerprint("reviews/a.csv", "11112222"),
        fingerprint("reviews/b.csv", "33334444")
      )
    )
    val keepEvaluation = matcher.evaluate(
      "/data",
      "resident_registration_number",
      "rrn",
      Seq(
        fingerprint("reviews/a.csv", "11112222"),
        fingerprint("reviews/c.csv", "55556666")
      )
    )

    assert(suppressEvaluation.shouldSuppress)
    assert(!keepEvaluation.shouldSuppress)
    assert(!keepEvaluation.reviewInvalidated)
    assert(matcher.hasDirectoryCandidate("/data", "reviews", "resident_registration_number", "rrn"))
  }

  private def allowlistEntry(fileIdentifier: String): AllowlistEntry =
    AllowlistEntry(
      datasetPath = "/data",
      fileIdentifier = fileIdentifier,
      columnName = "resident_registration_number",
      piiType = "rrn",
      reason = "dummy",
      reviewer = "reviewer@example.com",
      reviewedAt = "2026-04-20T00:00:00Z",
      sourceRunId = "",
      fileSize = 42L,
      fileMtimeEpochMs = 1710000000000L,
      fileChecksumAlgo = "CRC32",
      fileChecksum = checksumFor(fileIdentifier)
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

  private def checksumFor(fileIdentifier: String): String = fileIdentifier match {
    case "reviews/a.csv" => "11112222"
    case "reviews/b.csv" => "33334444"
    case other => other
  }
}
