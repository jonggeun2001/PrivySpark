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
  }

  test("parses optional ruleset and sample-ratio") {
    val parsed = Cli.parse(
      Array(
        "--path",
        "hdfs://cluster/data/input",
        "--output",
        "hdfs://cluster/data/output",
        "--ruleset",
        "/etc/privyspark/rules.yaml",
        "--sample-ratio",
        "0.75"
      )
    )

    assert(parsed.nonEmpty)
    assert(parsed.get.ruleset == "/etc/privyspark/rules.yaml")
    assert(parsed.get.sampleRatio == 0.75)
  }

  test("rejects sample-ratio outside (0.0, 1.0]") {
    val zeroRatio = Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--sample-ratio", "0.0"))
    val overOneRatio = Cli.parse(Array("--path", "/data/input", "--output", "/data/output", "--sample-ratio", "1.1"))

    assert(zeroRatio.isEmpty)
    assert(overOneRatio.isEmpty)
  }
}
