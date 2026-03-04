package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.config.RulesetLoader
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RulesetLoaderSpec extends AnyFunSuite {
  test("loads default ruleset") {
    val rules = RulesetLoader.load("default")
    assert(rules.nonEmpty)
    assert(rules.exists(_.piiType == "email"))
  }

  test("throws on missing ruleset file") {
    assertThrows[IllegalArgumentException] {
      RulesetLoader.load("/tmp/does-not-exist-ruleset.yaml")
    }
  }
}
