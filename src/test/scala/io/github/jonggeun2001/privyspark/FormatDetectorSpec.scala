package io.github.jonggeun2001.privyspark

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FormatDetectorSpec extends AnyFunSuite {
  test("infers csv format") {
    assert(FormatDetector.infer("/data/input.csv").contains("csv"))
  }

  test("infers json family formats") {
    assert(FormatDetector.infer("/data/input.json").contains("json"))
    assert(FormatDetector.infer("/data/input.jsonl").contains("json"))
    assert(FormatDetector.infer("/data/input.ndjson").contains("json"))
  }

  test("infers parquet format") {
    assert(FormatDetector.infer("/data/input.parquet").contains("parquet"))
  }

  test("infers orc format") {
    assert(FormatDetector.infer("/data/input.orc").contains("orc"))
  }

  test("returns empty for unsupported extensions") {
    assert(FormatDetector.infer("/data/input.xlsx").isEmpty)
  }
}
