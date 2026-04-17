package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.format.FormatDetector
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FormatDetectorSpec extends AnyFunSuite {
  test("classifies direct compressed data files") {
    val csv = FormatDetector.detect("/data/input.csv.gz").get
    assert(csv.baseFormat.contains("csv"))
    assert(csv.codec.contains("gz"))
    assert(!csv.isArchive)

    val json = FormatDetector.detect("/data/input.json.zst").get
    assert(json.baseFormat.contains("json"))
    assert(json.codec.contains("zst"))
    assert(!json.isArchive)

    val jsonl = FormatDetector.detect("/data/input.jsonl.xz").get
    assert(jsonl.baseFormat.contains("json"))
    assert(jsonl.codec.contains("xz"))
    assert(!jsonl.isArchive)
  }

  test("does not classify compressed columnar files as direct passthrough inputs") {
    assert(FormatDetector.detect("/data/input.parquet.xz").isEmpty)
    assert(FormatDetector.detect("/data/input.orc.gz").isEmpty)
    assert(FormatDetector.detect("/data/input.avro.zst").isEmpty)
  }

  test("does not classify compressed workbooks as direct passthrough inputs") {
    assert(FormatDetector.detect("/data/input.xlsx.gz").isEmpty)
    assert(FormatDetector.infer("/data/input.xlsx.gz").isEmpty)
  }

  test("classifies archive families including compressed tar aliases") {
    val tarZst = FormatDetector.detect("/data/input.tar.zst").get
    assert(tarZst.archiveFormat.contains("tar"))
    assert(tarZst.codec.contains("zst"))
    assert(tarZst.isArchive)

    val tgz = FormatDetector.detect("/data/input.tgz").get
    assert(tgz.archiveFormat.contains("tar"))
    assert(tgz.codec.contains("gz"))
    assert(tgz.isArchive)

    val sevenZip = FormatDetector.detect("/data/input.7z").get
    assert(sevenZip.archiveFormat.contains("7z"))
    assert(sevenZip.isArchive)

    val rar = FormatDetector.detect("/data/input.rar").get
    assert(rar.archiveFormat.contains("rar"))
    assert(rar.isArchive)
  }

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
    assert(!FormatDetector.shouldSkipProbe("/data/input.csv.gz"))
    assert(!FormatDetector.shouldSkipProbe("/data/input.parquet.zst"))
    assert(!FormatDetector.shouldSkipProbe("/data/app.log"))
    assert(!FormatDetector.shouldSkipProbe("/data/input.dat"))
  }
}
