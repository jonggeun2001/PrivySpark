package io.github.jonggeun2001.privyspark.review.collect

import io.github.jonggeun2001.privyspark.review.{ResponseEnvelope, ResponseItem, ReviewStatus}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ResponseValidatorSpec extends AnyFunSuite {
  test("validateEnvelope requires parseable metadata and responses") {
    val valid = ResponseEnvelope(
      sourcePath = "/review/inbox/owner.json",
      scanPath = "/data/project",
      responder = "owner@example.com",
      respondedAt = "2026-04-30T10:00:00Z",
      responses = Seq(validFalsePositiveItem())
    )

    assert(ResponseValidator.validateEnvelope(valid).isEmpty)
    assert(ResponseValidator.validateEnvelope(valid.copy(scanPath = "   ")).contains("scan_path is required"))
    assert(ResponseValidator.validateEnvelope(valid.copy(responder = "   ")).contains("responder is required"))
    assert(ResponseValidator.validateEnvelope(valid.copy(respondedAt = "today")).contains("responded_at must be an ISO-8601 instant"))
    assert(ResponseValidator.validateEnvelope(valid.copy(responses = Seq.empty)).contains("responses must not be empty"))
  }

  test("validateItem rejects wildcard recurring false positives and accepts valid decisions") {
    val falsePositive = validFalsePositiveItem()
    val truePositive = falsePositive.copy(
      decision = ReviewStatus.TruePositive,
      falsePositiveReason = "",
      expiresAt = "",
      actionPlan = "Mask column",
      actionDueDate = "2026-05-15"
    )

    assert(ResponseValidator.validateItem(falsePositive).isEmpty)
    assert(ResponseValidator.validateItem(truePositive).isEmpty)
    assert(ResponseValidator.validateItem(falsePositive.copy(columnName = "email*")).contains(
      "column_name and pii_type must be exact values without wildcard '*': finding-1"
    ))
    assert(ResponseValidator.validateItem(falsePositive.copy(fileIdentifier = "", fileIdentifierPattern = "")).contains(
      "file_identifier_pattern is required when hive_table_fqn is empty: finding-1"
    ))
  }

  private def validFalsePositiveItem(): ResponseItem =
    ResponseItem(
      findingKey = "finding-1",
      findingHash = "hash-1",
      fileIdentifier = "part-0001.csv",
      fileIdentifierPattern = "",
      hiveDatabase = "",
      hiveTable = "",
      hiveTableFqn = "",
      columnName = "email",
      piiType = "EMAIL",
      sampleRowCount = 10L,
      matchCount = 2L,
      nonEmptyMatchRatio = 0.2,
      decision = ReviewStatus.FalsePositive,
      falsePositiveReason = "Known test account",
      allowlistScope = "recurring",
      expiresAt = "2026-12-31",
      actionPlan = "",
      actionDueDate = ""
    )
}
