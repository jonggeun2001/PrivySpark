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
    assert(parsed.get.groupParallelism.isEmpty)
    assert(parsed.get.fileParallelism.isEmpty)
  }

  test("parses optional ruleset, sample-ratio, and parallelism options") {
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
        "--group-parallelism",
        "8",
        "--file-parallelism",
        "6"
      )
    )

    assert(parsed.nonEmpty)
    assert(parsed.get.ruleset == "/etc/privyspark/rules.yaml")
    assert(parsed.get.sampleRatio == 0.75)
    assert(parsed.get.groupParallelism.contains(8))
    assert(parsed.get.fileParallelism.contains(6))
  }

  test("rejects invalid sample-ratio and parallelism values") {
    val zeroRatio = Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--sample-ratio", "0.0"))
    val overOneRatio = Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--sample-ratio", "1.1"))
    val zeroGroupParallelism =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--group-parallelism", "0"))
    val negativeFileParallelism =
      Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--file-parallelism", "-1"))

    assert(zeroRatio.isEmpty)
    assert(overOneRatio.isEmpty)
    assert(zeroGroupParallelism.isEmpty)
    assert(negativeFileParallelism.isEmpty)
  }
}
