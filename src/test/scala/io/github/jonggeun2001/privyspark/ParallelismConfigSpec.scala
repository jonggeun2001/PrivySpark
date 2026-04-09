package io.github.jonggeun2001.privyspark

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ParallelismConfigSpec extends AnyFunSuite {
  test("resolveCliParallelism returns explicit CLI values when present") {
    val config = CliConfig(
      inputPath = "/data/input",
      outputPath = "/data/output",
      groupParallelism = Some(7),
      fileParallelism = Some(5)
    )

    assert(PrivySparkApp.resolveCliParallelism(config) == (7, 5))
  }

  test("resolveCliParallelism returns fallback markers when CLI values are absent") {
    val config = CliConfig(
      inputPath = "/data/input",
      outputPath = "/data/output"
    )

    assert(PrivySparkApp.resolveCliParallelism(config) == (-1, -1))
  }
}
