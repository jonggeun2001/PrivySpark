package io.github.jonggeun2001.privyspark.progress

import org.apache.spark.SparkConf
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ProgressIOFlushModeSpec extends AnyFunSuite {
  test("progress flush mode defaults to group") {
    assert(ProgressIO.resolveFlushMode(new SparkConf(false)).name == "group")
  }

  test("progress flush mode can opt back into file writes") {
    val conf = new SparkConf(false).set(ProgressIO.ProgressFlushModeConfKey, "file")

    assert(ProgressIO.resolveFlushMode(conf).name == "file")
  }

  test("progress flush mode rejects unknown values") {
    val conf = new SparkConf(false).set(ProgressIO.ProgressFlushModeConfKey, "row")

    intercept[IllegalArgumentException] {
      ProgressIO.resolveFlushMode(conf)
    }
  }
}
