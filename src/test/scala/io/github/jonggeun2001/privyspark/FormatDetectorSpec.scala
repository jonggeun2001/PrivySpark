package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.format.FormatDetector
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

  test("infers avro format") {
    assert(FormatDetector.infer("/data/input.avro").contains("avro"))
  }

  test("infers xlsx format") {
    assert(FormatDetector.infer("/data/input.xlsx").contains("xlsx"))
  }

  test("infers archive formats") {
    assert(FormatDetector.infer("/data/input.zip").contains("zip"))
    assert(FormatDetector.infer("/data/input.jar").contains("jar"))
  }

  test("preserves ordinary hash characters in structured filenames") {
    assert(FormatDetector.infer("/data/report#1.json").contains("json"))
    assert(FormatDetector.infer("/data/nested/users#2024.csv").contains("csv"))
  }

  test("returns empty for unsupported extensions") {
    assert(FormatDetector.infer("/data/input.unknown").isEmpty)
  }

  test("skips probe for known non-data extensions") {
    assert(FormatDetector.shouldSkipProbe("/data/brochure.pdf"))
    assert(FormatDetector.shouldSkipProbe("/data/photo.jpg"))
    assert(!FormatDetector.shouldSkipProbe("/data/input.csv"))
    assert(!FormatDetector.shouldSkipProbe("/data/input.json"))
    assert(!FormatDetector.shouldSkipProbe("/data/app.log"))
    assert(!FormatDetector.shouldSkipProbe("/data/input.dat"))
  }
}
