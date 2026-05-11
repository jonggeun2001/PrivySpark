package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.scan.GroupFileScanner

import org.apache.spark.SparkConf
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GroupFileScannerSpec extends AnyFunSuite {
  test("file scope in-flight markers are disabled by default") {
    assert(!GroupFileScanner.fileInFlightMarkersEnabled(new SparkConf(false)))
  }

  test("file scope in-flight markers can be enabled for compatibility") {
    val conf = new SparkConf(false).set(GroupFileScanner.FileInFlightMarkerEnabledConfKey, "true")

    assert(GroupFileScanner.fileInFlightMarkersEnabled(conf))
  }
}
