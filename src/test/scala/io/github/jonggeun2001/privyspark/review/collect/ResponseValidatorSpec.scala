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
      responder = "owner1",
      respondedAt = "2026-04-30T10:00:00Z",
      responses = Seq(validFalsePositiveItem())
    )

    assert(ResponseValidator.validateEnvelope(valid).isEmpty)
    assert(ResponseValidator.validateEnvelope(valid.copy(scanPath = "   ")).contains("scan_path is required"))
    assert(ResponseValidator.validateEnvelope(valid.copy(responder = "   ")).contains("responder is required"))
    assert(ResponseValidator.validateEnvelope(valid.copy(responder = "owner@example.com")).contains("responder must use lowercase letters and digits only"))
    assert(ResponseValidator.validateEnvelope(valid.copy(responder = "Owner1")).contains("responder must use lowercase letters and digits only"))
    assert(ResponseValidator.validateEnvelope(valid.copy(responder = " owner1 ")).contains("responder must use lowercase letters and digits only"))
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

  Seq[(String, ResponseItem => ResponseItem)](
    "finding key" -> (_.copy(findingKey = " ")),
    "column" -> (_.copy(columnName = " ")),
    "PII type" -> (_.copy(piiType = " ")),
    "decision" -> (_.copy(decision = "")),
    "unknown decision" -> (_.copy(decision = "approve")),
    "reason" -> (_.copy(falsePositiveReason = " ")),
    "expiry" -> (_.copy(expiresAt = " ")),
    "invalid expiry date" -> (_.copy(expiresAt = "2026-02-30")),
    "unsupported scope" -> (_.copy(allowlistScope = "exact")),
    "wildcard PII type" -> (_.copy(piiType = "EMAIL*"))
  ).foreach { case (name, modify) =>
    test(s"rejects an invalid false-positive $name") {
      assert(ResponseValidator.validateItem(modify(validFalsePositiveItem())).nonEmpty)
    }
  }

  test("true positives require a plan and a valid calendar date") {
    val valid = validFalsePositiveItem().copy(decision = ReviewStatus.TruePositive, actionPlan = "mask", actionDueDate = "2024-02-29")
    assert(ResponseValidator.validateItem(valid).isEmpty)
    Seq(valid.copy(actionPlan = " "), valid.copy(actionDueDate = ""), valid.copy(actionDueDate = "2025-02-29"))
      .foreach(item => assert(ResponseValidator.validateItem(item).nonEmpty))
    // Collection accepts historical dates; the browser's 30-day entry window is separate.
    assert(ResponseValidator.validateItem(valid.copy(actionDueDate = "2000-01-01")).isEmpty)
  }

  test("Hive table scope can omit file paths and legacy file identifiers remain usable") {
    val valid = validFalsePositiveItem()
    assert(ResponseValidator.validateItem(valid.copy(hiveTableFqn = "mart.contacts", fileIdentifier = "")).isEmpty)
    assert(ResponseValidator.validateItem(valid.copy(fileIdentifierPattern = "   ")).isEmpty)
    assert(ResponseValidator.recurringFileIdentifierPattern(valid.copy(fileIdentifierPattern = "folder/*")) == "folder/*")
    assert(ResponseValidator.validateItem(valid.copy(allowlistScope = "")).isEmpty)
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
