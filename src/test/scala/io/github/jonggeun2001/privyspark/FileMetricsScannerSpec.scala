package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.scan.FileMetricsScanner

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FileMetricsScannerSpec extends AnyFunSuite {
  test("scan file metrics skips file status when size and mtime overrides are present") {
    assert(!FileMetricsScanner.shouldLoadFileStatus(Some(100L), Some(200L)))
  }

  test("scan file metrics loads file status when either metadata override is missing") {
    assert(FileMetricsScanner.shouldLoadFileStatus(None, Some(200L)))
    assert(FileMetricsScanner.shouldLoadFileStatus(Some(100L), None))
    assert(FileMetricsScanner.shouldLoadFileStatus(None, None))
  }
}
