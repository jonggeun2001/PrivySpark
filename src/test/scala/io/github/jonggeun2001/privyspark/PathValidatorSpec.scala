package io.github.jonggeun2001.privyspark

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PathValidatorSpec extends AnyFunSuite {
  test("accepts absolute local path") {
    assert(PathValidator.isAbsolute("/data/input"))
  }

  test("accepts URI style path") {
    assert(PathValidator.isAbsolute("hdfs://cluster/data/input"))
  }

  test("rejects relative path") {
    assert(!PathValidator.isAbsolute("data/input"))
  }
}
