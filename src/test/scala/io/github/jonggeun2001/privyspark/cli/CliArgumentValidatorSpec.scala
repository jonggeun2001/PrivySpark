package io.github.jonggeun2001.privyspark.cli

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CliArgumentValidatorSpec extends AnyFunSuite {
  private val validScan = CliConfig(inputPath = "/data/input", outputPath = "/data/output")
  private val scanPathFields: Seq[(String, (CliConfig, String) => CliConfig)] = Seq(
    "input" -> ((config, value) => config.copy(inputPath = value)),
    "output" -> ((config, value) => config.copy(outputPath = value)),
    "allowlist" -> ((config, value) => config.copy(allowlist = Some(value))),
    "review state" -> ((config, value) => config.copy(reviewStateRoot = Some(value))),
    "review HTML" -> ((config, value) => config.copy(reviewHtmlDir = Some(value))),
    "Hive password" -> ((config, value) => config.copy(hiveMetastorePasswordFile = Some(value)))
  )

  scanPathFields.foreach { case (name, update) =>
    test(s"scan rejects blank and relative $name paths") {
      Seq("", "   ", "relative/path").foreach { invalid =>
        assert(CliArgumentValidator.validate(CliCommand.Scan(update(validScan, invalid))).contains(2), invalid)
      }
    }
    test(s"scan accepts a filesystem URI for the $name path") {
      assert(CliArgumentValidator.validate(CliCommand.Scan(update(validScan, "hdfs://nn/data"))).isEmpty)
    }
  }

  test("review apply validates every required path independently") {
    val valid = ReviewApplyCliConfig("/results.csv", "/input", "/allowlist.jsonl", "reviewer")
    Seq("", " ", "relative").foreach { invalid =>
      Seq(valid.copy(scanResultsPath = invalid), valid.copy(inputRoot = invalid), valid.copy(allowlistPath = invalid))
        .foreach(config => assert(CliArgumentValidator.validate(CliCommand.ReviewApply(config)).contains(2)))
    }
    assert(CliArgumentValidator.validate(CliCommand.ReviewApply(valid)).isEmpty)
  }

  test("review collect permits omitted scan results but still rejects invalid state paths") {
    Seq("", " ", "relative").foreach { invalid =>
      val command = CliCommand.ReviewCollect(ReviewCollectCliConfig(scanResultsPath = "", reviewStateRoot = invalid))
      assert(CliArgumentValidator.validate(command).contains(2))
    }
    val invalidResults = CliCommand.ReviewCollect(ReviewCollectCliConfig(scanResultsPath = "relative", reviewStateRoot = "/state"))
    assert(CliArgumentValidator.validate(invalidResults).contains(2))
  }

  test("path validation rejects null, whitespace, NUL and incomplete URIs") {
    Seq(null, "", " \t ", "relative", "hdfs://", "/data/\u0000invalid").foreach { path =>
      assert(!PathValidator.isAbsolute(path), String.valueOf(path))
    }
  }

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
