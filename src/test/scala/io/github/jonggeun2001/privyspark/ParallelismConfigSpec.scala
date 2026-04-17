package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.cli.CliConfig
import io.github.jonggeun2001.privyspark.util.ParallelismConfig
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ParallelismConfigSpec extends AnyFunSuite {
  test("resolveCliParallelism returns explicit CLI values when present") {
    val config = CliConfig(
      inputPath = "/data/input",
      outputPath = "/data/output",
      preScanParallelism = Some(9),
      groupParallelism = Some(7),
      fileParallelism = Some(5)
    )

    assert(
      ParallelismConfig.resolveCliParallelism(
        config.preScanParallelism,
        config.groupParallelism,
        config.fileParallelism
      ) == (9, 7, 5)
    )
  }

  test("resolveCliParallelism returns fallback markers when CLI values are absent") {
    val config = CliConfig(
      inputPath = "/data/input",
      outputPath = "/data/output"
    )

    assert(
      ParallelismConfig.resolveCliParallelism(
        config.preScanParallelism,
        config.groupParallelism,
        config.fileParallelism
      ) == (-1, -1, -1)
    )
  }

  test("defaultPreScanParallelism keeps the fixed IO-oriented default") {
    assert(ParallelismConfig.defaultPreScanParallelism == 32)
  }

  test("defaultGroupParallelism keeps the higher driver submission default") {
    assert(ParallelismConfig.defaultGroupParallelism == 16)
  }

  test("defaultFileParallelism keeps the higher fallback scan default") {
    assert(ParallelismConfig.defaultFileParallelism == 8)
  }
}
