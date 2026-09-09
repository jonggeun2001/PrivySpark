package io.github.jonggeun2001.privyspark.review

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReviewActionPlanStatusSpec extends AnyFunSuite {
  private val finding = ReviewFinding(
    scanPath = "hdfs://nn/data/", fileIdentifier = "current.csv",
    hiveDatabase = "mart", hiveTable = "contacts", hiveTableFqn = "mart.contacts",
    columnName = "email", piiType = "email", matchCount = 1L, sampledRowCount = 1L,
    matchRatio = 1.0, nonEmptyMatchRatio = 1.0, confidence = 1.0,
    findingKey = "current-key", findingHash = "hash", fingerprintComplete = true,
    hasMultipleFileEvidence = false, aggregatedFileCount = 1, aggregatedPartitionCount = 1,
    aggregated = false, evidence = Seq.empty
  )
  private val plan = ActionPlan(
    findingKey = "old-key", scanPath = "hdfs://nn//data//", fileIdentifier = "old.csv",
    hiveDatabase = "mart", hiveTable = "contacts", hiveTableFqn = "mart.contacts",
    columnName = "email", piiType = "email", actionPlan = "마스킹",
    actionDueDate = "2999-12-31", responder = "owner", respondedAt = "2026-01-01T00:00:00Z",
    status = "remediation_planned"
  )

  test("matching keeps latest timestamp, finding key and last input tie priority") {
    val plans = Seq(
      plan.copy(respondedAt = "2025-12-31T00:00:00Z", findingKey = "z-key", actionPlan = "old"),
      plan.copy(findingKey = "a-key", actionPlan = "smaller key"),
      plan.copy(findingKey = "z-key", actionPlan = "first tie"),
      plan.copy(findingKey = "z-key", actionPlan = "last tie", status = "verified"),
      plan.copy(scanPath = "hdfs://other/data", actionPlan = "wrong scan"),
      plan.copy(columnName = "phone", actionPlan = "wrong column"),
      plan.copy(piiType = "phone", actionPlan = "wrong type"),
      plan.copy(hiveTableFqn = "mart.other", actionPlan = "wrong table")
    )

    val actual = ReviewActionPlanStatus.matchFindings(Seq(finding), plans)
    assert(actual.keySet == Set("current-key"))
    assert(actual("current-key").actionPlan == "last tie")
    assert(actual("current-key").status == "verified")
    assert(actual("current-key").statusLabel == "조치 확인됨")
  }

  test("file matching treats whitespace-only tables as absent and keeps table and file scopes separate") {
    val fileFinding = finding.copy(hiveTableFqn = " ", fileIdentifier = "shared.csv", findingKey = "file")
    val tableFinding = finding.copy(hiveTableFqn = "shared.csv", findingKey = "table")
    val missing = finding.copy(hiveTableFqn = "", fileIdentifier = "missing.csv", findingKey = "missing")
    val plans = Seq(
      plan.copy(hiveTableFqn = "", fileIdentifier = "shared.csv", actionPlan = "file plan"),
      plan.copy(hiveTableFqn = "shared.csv", actionPlan = "table plan"),
      plan.copy(hiveTableFqn = " shared.csv ", actionPlan = "different raw table"),
      plan.copy(hiveTableFqn = "", fileIdentifier = "other.csv", actionPlan = "other file")
    )

    val actual = ReviewActionPlanStatus.matchFindings(Seq(fileFinding, tableFinding, missing), plans)
    assert(actual.keySet == Set("file", "table"))
    assert(actual("file").actionPlan == "file plan")
    assert(actual("table").actionPlan == "table plan")
    assert(ReviewActionPlanStatus.matchFindings(Seq.empty, plans).isEmpty)
    assert(ReviewActionPlanStatus.matchFindings(Seq(finding), Seq.empty).isEmpty)
  }
}
