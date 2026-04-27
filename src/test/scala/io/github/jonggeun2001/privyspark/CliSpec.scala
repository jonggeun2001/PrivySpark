package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.cli.Cli
import io.github.jonggeun2001.privyspark.cli.CliCommand
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CliSpec extends AnyFunSuite {
  test("parses scan arguments and applies defaults") {
    val parsed = Cli.parse(Array("--path", "/data/input", "--output", "/data/output"))

    assert(parsed.nonEmpty)
    assert(parsed.get.isInstanceOf[CliCommand.Scan])

    val config = parsed.get.asInstanceOf[CliCommand.Scan].config
    assert(config.inputPath == "/data/input")
    assert(config.outputPath == "/data/output")
    assert(config.ruleset == "default")
    assert(config.sampleRatio == 0.2)
    assert(config.fileSampleRatio.isEmpty)
    assert(config.fileSampleMinFiles == 10)
    assert(config.preScanParallelism.isEmpty)
    assert(config.groupParallelism.isEmpty)
    assert(config.fileParallelism.isEmpty)
    assert(config.excelMaxRowsInMemory.isEmpty)
    assert(config.excelByteArrayMaxOverride.isEmpty)
    assert(config.ignorePatterns.isEmpty)
    assert(config.ignoreFile.isEmpty)
    assert(config.allowlist.isEmpty)
    assert(config.reviewStateRoot.isEmpty)
    assert(config.reviewSampleMode == "masked")
    assert(config.suppressions.isEmpty)
    assert(config.suppressionFile.isEmpty)
    assert(!config.disableHiveTableLookup)
    assert(config.effectiveOutputFormats == Seq("parquet"))
  }

  test("parses optional scan ruleset, sampling, parallelism, Hive lookup, ignore options, allowlist, and output formats") {
    val parsed = Cli.parse(
      Array(
        "--path",
        "hdfs://cluster/data/input",
        "--output",
        "hdfs://cluster/data/output",
        "--ruleset",
        "/etc/privyspark/rules.yaml",
        "--sample-ratio",
        "0.75",
        "--file-sample-ratio",
        "0.4",
        "--file-sample-min-files",
        "12",
        "--pre-scan-parallelism",
        "3",
        "--group-parallelism",
        "8",
        "--file-parallelism",
        "6",
        "--excel-max-rows-in-memory",
        "2048",
        "--excel-byte-array-max-override",
        "300000000",
        "--output-format",
        "csv",
        "--output-format",
        "excel",
        "--output-format",
        "csv",
        "--ignore",
        "_SUCCESS",
        "--ignore",
        "backup/**",
        "--allowlist",
        "/etc/privyspark/allowlist.jsonl",
        "--review-state-root",
        "/var/lib/privyspark/review-state",
        "--review-sample-mode",
        "raw",
        "--ignore-file",
        "/etc/privyspark/ignore.txt",
        "--disable-hive-table-lookup",
        "--suppress",
        "prdctcd:driver_license_number",
        "--suppress",
        "foo:email",
        "--suppress",
        "ns:email:email",
        "--suppression-file",
        "/etc/privyspark/suppressions.txt"
      )
    )

    assert(parsed.nonEmpty)
    val config = parsed.get.asInstanceOf[CliCommand.Scan].config
    assert(config.ruleset == "/etc/privyspark/rules.yaml")
    assert(config.sampleRatio == 0.75)
    assert(config.fileSampleRatio.contains(0.4))
    assert(config.fileSampleMinFiles == 12)
    assert(config.preScanParallelism.contains(3))
    assert(config.groupParallelism.contains(8))
    assert(config.fileParallelism.contains(6))
    assert(config.excelMaxRowsInMemory.contains(2048))
    assert(config.excelByteArrayMaxOverride.contains(300000000))
    assert(config.effectiveOutputFormats == Seq("csv", "excel"))
    assert(config.ignorePatterns == Seq("_SUCCESS", "backup/**"))
    assert(config.allowlist.contains("/etc/privyspark/allowlist.jsonl"))
    assert(config.reviewStateRoot.contains("/var/lib/privyspark/review-state"))
    assert(config.reviewSampleMode == "raw")
    assert(config.ignoreFile.contains("/etc/privyspark/ignore.txt"))
    assert(config.disableHiveTableLookup)
    assert(config.suppressions == Seq("prdctcd:driver_license_number", "foo:email", "ns:email:email"))
    assert(config.suppressionFile.contains("/etc/privyspark/suppressions.txt"))
  }

  test("rejects invalid sampling, parallelism, and output format values") {
    val largePreScanParallelismValue = "128"
    val zeroRatio = Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--sample-ratio", "0.0"))
    val overOneRatio = Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--sample-ratio", "1.1"))
    val zeroFileSampleRatio =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--file-sample-ratio", "0.0"))
    val overOneFileSampleRatio =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--file-sample-ratio", "1.1"))
    val zeroFileSampleMinFiles =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--file-sample-min-files", "0"))
    val zeroPreScanParallelism =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--pre-scan-parallelism", "0"))
    val largePreScanParallelism =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--pre-scan-parallelism", largePreScanParallelismValue))
    val zeroGroupParallelism =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--group-parallelism", "0"))
    val negativeFileParallelism =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--file-parallelism", "-1"))
    val zeroExcelMaxRowsInMemory =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--excel-max-rows-in-memory", "0"))
    val zeroExcelByteArrayMaxOverride =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--excel-byte-array-max-override", "0"))
    val invalidOutputFormat =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--output-format", "json"))
    val missingSuppressionSeparator =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--suppress", "prdctcd"))
    val missingSuppressionColumn =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--suppress", ":email"))
    val missingSuppressionPiiType =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--suppress", "prdctcd:"))
    val blankSuppressionFile =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--suppression-file", "   "))
    val invalidReviewSampleMode =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--review-sample-mode", "verbose"))

    assert(zeroRatio.isEmpty)
    assert(overOneRatio.isEmpty)
    assert(zeroFileSampleRatio.isEmpty)
    assert(overOneFileSampleRatio.isEmpty)
    assert(zeroFileSampleMinFiles.isEmpty)
    assert(zeroPreScanParallelism.isEmpty)
    assert(largePreScanParallelism.nonEmpty)
    assert(largePreScanParallelism.get.asInstanceOf[CliCommand.Scan].config.preScanParallelism.contains(128))
    assert(zeroGroupParallelism.isEmpty)
    assert(negativeFileParallelism.isEmpty)
    assert(zeroExcelMaxRowsInMemory.isEmpty)
    assert(zeroExcelByteArrayMaxOverride.isEmpty)
    assert(invalidOutputFormat.isEmpty)
    assert(missingSuppressionSeparator.isEmpty)
    assert(missingSuppressionColumn.isEmpty)
    assert(missingSuppressionPiiType.isEmpty)
    assert(blankSuppressionFile.isEmpty)
    assert(invalidReviewSampleMode.isEmpty)
  }

  test("parses review apply subcommand") {
    val parsed = Cli.parse(
      Array(
        "review",
        "apply",
        "--scan-results",
        "/data/output/excel/scan_results.xlsx",
        "--input-root",
        "/data/input",
        "--allowlist",
        "/data/review/allowlist.jsonl",
        "--reviewer",
        "reviewer@example.com",
        "--dry-run"
      )
    )

    assert(parsed.nonEmpty)
    assert(parsed.get.isInstanceOf[CliCommand.ReviewApply])

    val config = parsed.get.asInstanceOf[CliCommand.ReviewApply].config
    assert(config.scanResultsPath == "/data/output/excel/scan_results.xlsx")
    assert(config.inputRoot == "/data/input")
    assert(config.allowlistPath == "/data/review/allowlist.jsonl")
    assert(config.reviewer == "reviewer@example.com")
    assert(config.dryRun)
  }

  test("parses review collect subcommand") {
    val parsed = Cli.parse(
      Array(
        "review",
        "collect",
        "--scan-results",
        "/data/output/parquet/scan_results",
        "--review-state-root",
        "/data/review-state"
      )
    )

    assert(parsed.nonEmpty)
    assert(parsed.get.isInstanceOf[CliCommand.ReviewCollect])

    val config = parsed.get.asInstanceOf[CliCommand.ReviewCollect].config
    assert(config.scanResultsPath == "/data/output/parquet/scan_results")
    assert(config.reviewStateRoot == "/data/review-state")
  }

  test("captures parser errors without terminating") {
    val missingPath = Cli.parseWithErrors(Array("--output", "/data/output"))
    val invalidSampleRatio = Cli.parseWithErrors(Array("--path", "/data/input", "--output", "/data/output", "--sample-ratio", "0.0"))
    val invalidFileSampleRatio =
      Cli.parseWithErrors(Array("--path", "/data/input", "--output", "/data/output", "--file-sample-ratio", "0.0"))
    val invalidFileSampleMinFiles =
      Cli.parseWithErrors(Array("--path", "/data/input", "--output", "/data/output", "--file-sample-min-files", "0"))
    val invalidOutputFormat =
      Cli.parseWithErrors(Array("--path", "/data/input", "--output", "/data/output", "--output-format", "json"))
    val invalidSuppression =
      Cli.parseWithErrors(Array("--path", "/data/input", "--output", "/data/output", "--suppress", "prdctcd"))
    val invalidSuppressionFile =
      Cli.parseWithErrors(Array("--path", "/data/input", "--output", "/data/output", "--suppression-file", " "))
    val invalidReviewApply = Cli.parseWithErrors(Array("review", "apply", "--scan-results", "/tmp/results.xlsx"))

    assert(missingPath.command.isEmpty)
    assert(missingPath.errors.exists(_.contains("--path")))
    assert(invalidSampleRatio.command.isEmpty)
    assert(invalidSampleRatio.errors.exists(_.contains("sample-ratio must be > 0.0 and <= 1.0")))
    assert(invalidFileSampleRatio.command.isEmpty)
    assert(invalidFileSampleRatio.errors.exists(_.contains("file-sample-ratio must be > 0.0 and <= 1.0")))
    assert(invalidFileSampleMinFiles.command.isEmpty)
    assert(invalidFileSampleMinFiles.errors.exists(_.contains("file-sample-min-files must be >= 1")))
    assert(invalidOutputFormat.command.isEmpty)
    assert(invalidOutputFormat.errors.exists(_.contains("output-format must be one of: parquet, csv, excel")))
    assert(invalidSuppression.command.isEmpty)
    assert(invalidSuppression.errors.exists(_.contains("suppress must use column:pii_type with non-empty values")))
    assert(invalidSuppressionFile.command.isEmpty)
    assert(invalidSuppressionFile.errors.exists(_.contains("suppression-file must not be blank")))
    assert(invalidReviewApply.command.isEmpty)
    assert(invalidReviewApply.errors.exists(_.contains("--input-root")))
  }
}
