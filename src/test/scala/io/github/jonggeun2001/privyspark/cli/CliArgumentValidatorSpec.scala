package io.github.jonggeun2001.privyspark.cli

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CliArgumentValidatorSpec extends AnyFunSuite {
  test("validate accepts absolute scan paths and optional absolute review paths") {
    val command = CliCommand.Scan(
      CliConfig(
        inputPath = "/data/input",
        outputPath = "/data/output",
        allowlist = Some("/data/allowlist.jsonl"),
        reviewStateRoot = Some("/data/review-state"),
        reviewHtmlDir = Some("/data/review-html"),
        hiveMetastorePasswordFile = Some("/secrets/hive-password")
      )
    )

    assert(CliArgumentValidator.validate(command).isEmpty)
  }

  test("validate rejects relative scan optional paths before session creation") {
    val command = CliCommand.Scan(
      CliConfig(
        inputPath = "/data/input",
        outputPath = "/data/output",
        reviewHtmlDir = Some("review")
      )
    )

    assert(CliArgumentValidator.validate(command).contains(2))
  }

  test("validate allows blank review collect scan results and validates state root") {
    val command = CliCommand.ReviewCollect(
      ReviewCollectCliConfig(
        scanResultsPath = "",
        reviewStateRoot = "/data/review-state"
      )
    )

    assert(CliArgumentValidator.validate(command).isEmpty)
  }

  test("validate rejects relative review apply inputs") {
    val command = CliCommand.ReviewApply(
      ReviewApplyCliConfig(
        scanResultsPath = "scan_results.xlsx",
        inputRoot = "/data/input",
        allowlistPath = "/data/allowlist.jsonl",
        reviewer = "reviewer@example.com"
      )
    )

    assert(CliArgumentValidator.validate(command).contains(2))
  }
}
