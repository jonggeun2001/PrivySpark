package io.github.jonggeun2001.privyspark.report

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class OutputFormatsSpec extends AnyFunSuite {
  test("Default and All keep parquet as default while listing every supported format") {
    assert(OutputFormats.Default == Seq(OutputFormats.Parquet))
    assert(OutputFormats.All == Seq(OutputFormats.Parquet, OutputFormats.Csv, OutputFormats.Excel))
  }

  test("normalizeAll trims blanks, lowercases values, de-duplicates, and falls back to Default") {
    assert(OutputFormats.normalizeAll(Seq(" CSV ", "", "Parquet", "csv", "EXCEL")) == Seq("csv", "parquet", "excel"))
    assert(OutputFormats.normalizeAll(Seq(" ", "")) == OutputFormats.Default)
  }

  test("requireSupported returns normalized formats and rejects unsupported values") {
    assert(OutputFormats.requireSupported(Seq(" CSV ", "parquet")) == Seq("csv", "parquet"))

    val thrown = intercept[IllegalArgumentException] {
      OutputFormats.requireSupported(Seq("parquet", "json", "xml"))
    }
    assert(thrown.getMessage.contains("Unsupported output formats: json, xml"))
  }
}
