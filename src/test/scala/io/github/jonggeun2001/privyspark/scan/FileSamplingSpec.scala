package io.github.jonggeun2001.privyspark.scan

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FileSamplingSpec extends AnyFunSuite {
  test("selectSampledFileKeys keeps at least one deterministic file in original order") {
    val fileKeys = Seq("a.csv", "b.csv", "c.csv", "d.csv")

    val sampledKeys = FileSampling.selectSampledFileKeys(fileKeys, 0.01, "fixture")

    assert(sampledKeys.size == 1)
    assert(fileKeys.contains(sampledKeys.head))
  }

  test("selectSampledFileKeys uses ceiling for sampled file count") {
    val sampledKeys = FileSampling.selectSampledFileKeys(Seq("a.csv", "b.csv", "c.csv", "d.csv"), 0.51)

    assert(sampledKeys.size == 3)
  }

  test("selectSampledFileKeys is stable across repeated calls and input ordering") {
    val fileKeys = Seq("users-a.csv", "users-b.csv", "users-c.csv", "users-d.csv", "users-e.csv")

    val sampledRuns = (1 to 20).map(_ => FileSampling.selectSampledFileKeys(fileKeys, 0.4, "reviews"))
    val reversedInputSample = FileSampling.selectSampledFileKeys(fileKeys.reverse, 0.4, "reviews").toSet

    assert(sampledRuns.distinct.size == 1)
    assert(sampledRuns.head.toSet == reversedInputSample)
    assert(sampledRuns.head == fileKeys.filter(reversedInputSample.contains))
  }
}
