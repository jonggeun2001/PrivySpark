package io.github.jonggeun2001.privyspark

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CliSpec extends AnyFunSuite {
  test("parses required arguments and applies defaults") {
    val parsed = Cli.parse(Array("--path", "/data/input", "--output", "/data/output"))

    assert(parsed.nonEmpty)
    assert(parsed.get.inputPath == "/data/input")
    assert(parsed.get.outputPath == "/data/output")
    assert(parsed.get.ruleset == "default")
    assert(parsed.get.sampleRatio == 0.2)
    assert(parsed.get.fileSampleRatio.isEmpty)
    assert(parsed.get.preScanParallelism.isEmpty)
    assert(parsed.get.groupParallelism.isEmpty)
    assert(parsed.get.fileParallelism.isEmpty)
    assert(parsed.get.ignorePatterns.isEmpty)
    assert(parsed.get.ignoreFile.isEmpty)
    assert(parsed.get.effectiveOutputFormats == Seq("parquet"))
  }

  test("parses optional ruleset, sampling, parallelism, ignore options, and output formats") {
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
        "--pre-scan-parallelism",
        "3",
        "--group-parallelism",
        "8",
        "--file-parallelism",
        "6",
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
        "--ignore-file",
        "/etc/privyspark/ignore.txt"
      )
    )

    assert(parsed.nonEmpty)
    assert(parsed.get.ruleset == "/etc/privyspark/rules.yaml")
    assert(parsed.get.sampleRatio == 0.75)
    assert(parsed.get.fileSampleRatio.contains(0.4))
    assert(parsed.get.preScanParallelism.contains(3))
    assert(parsed.get.groupParallelism.contains(8))
    assert(parsed.get.fileParallelism.contains(6))
    assert(parsed.get.effectiveOutputFormats == Seq("csv", "excel"))
    assert(parsed.get.ignorePatterns == Seq("_SUCCESS", "backup/**"))
    assert(parsed.get.ignoreFile.contains("/etc/privyspark/ignore.txt"))
  }

  test("rejects invalid sampling, parallelism, and output format values") {
    val largePreScanParallelismValue = "128"
    val zeroRatio = Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--sample-ratio", "0.0"))
    val overOneRatio = Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--sample-ratio", "1.1"))
    val zeroFileSampleRatio =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--file-sample-ratio", "0.0"))
    val overOneFileSampleRatio =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--file-sample-ratio", "1.1"))
    val zeroPreScanParallelism =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--pre-scan-parallelism", "0"))
    val largePreScanParallelism =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--pre-scan-parallelism", largePreScanParallelismValue))
    val zeroGroupParallelism =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--group-parallelism", "0"))
    val negativeFileParallelism =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--file-parallelism", "-1"))
    val invalidOutputFormat =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--output-format", "json"))

    assert(zeroRatio.isEmpty)
    assert(overOneRatio.isEmpty)
    assert(zeroFileSampleRatio.isEmpty)
    assert(overOneFileSampleRatio.isEmpty)
    assert(zeroPreScanParallelism.isEmpty)
    assert(largePreScanParallelism.nonEmpty)
    assert(largePreScanParallelism.get.preScanParallelism.contains(128))
    assert(zeroGroupParallelism.isEmpty)
    assert(negativeFileParallelism.isEmpty)
    assert(invalidOutputFormat.isEmpty)
  }

  test("captures parser errors without terminating") {
    val missingPath = Cli.parseWithErrors(Array("--output", "/data/output"))
    val invalidSampleRatio = Cli.parseWithErrors(Array("--path", "/data/input", "--output", "/data/output", "--sample-ratio", "0.0"))
    val invalidFileSampleRatio =
      Cli.parseWithErrors(Array("--path", "/data/input", "--output", "/data/output", "--file-sample-ratio", "0.0"))
    val invalidOutputFormat =
      Cli.parseWithErrors(Array("--path", "/data/input", "--output", "/data/output", "--output-format", "json"))

    assert(missingPath.config.isEmpty)
    assert(missingPath.errors.exists(_.contains("--path")))
    assert(invalidSampleRatio.config.isEmpty)
    assert(invalidSampleRatio.errors.exists(_.contains("sample-ratio must be > 0.0 and <= 1.0")))
    assert(invalidFileSampleRatio.config.isEmpty)
    assert(invalidFileSampleRatio.errors.exists(_.contains("file-sample-ratio must be > 0.0 and <= 1.0")))
    assert(invalidOutputFormat.config.isEmpty)
    assert(invalidOutputFormat.errors.exists(_.contains("output-format must be one of: parquet, csv, excel")))
  }
}
