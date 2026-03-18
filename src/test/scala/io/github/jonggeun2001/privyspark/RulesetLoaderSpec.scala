package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.config.RulesetLoader
import io.github.jonggeun2001.privyspark.validator.KoreanNameValidator
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.charset.StandardCharsets
import java.nio.file.Files

@RunWith(classOf[JUnitRunner])
class RulesetLoaderSpec extends AnyFunSuite {
  test("loads default ruleset") {
    val rules = RulesetLoader.load("default")
    assert(rules.nonEmpty)
    assert(rules.exists(_.piiType == "email"))
    assert(rules.find(_.piiType == "name").flatMap(_.validator).contains(KoreanNameValidator.ValidatorName))
    assert(rules.forall(_.columnHints.isEmpty))
  }

  test("loads optional column hints and validator from ruleset file and ignores blank entries") {
    val rulesetPath = Files.createTempFile("privyspark-ruleset", ".yaml")
    val yaml =
      """rules:
        |  - pii_type: name
        |    regex: '(김|이|박)[가-힣]{1,2}'
        |    column_hints:
        |      - name
        |      -
        |      - "   "
        |      - customer
        |    validator: korean_name_dict
        |  - pii_type: phone_number
        |    regex: '01[016789]-?[0-9]{3,4}-?[0-9]{4}'
        |    validator: "   "
        |""".stripMargin

    Files.write(rulesetPath, yaml.getBytes(StandardCharsets.UTF_8))
    try {
      val rules = RulesetLoader.load(rulesetPath.toString)
      assert(rules.map(_.piiType) == Seq("name", "phone_number"))
      assert(rules.head.columnHints == Seq("name", "customer"))
      assert(rules.head.validator.contains(KoreanNameValidator.ValidatorName))
      assert(rules(1).columnHints.isEmpty)
      assert(rules(1).validator.isEmpty)
    } finally {
      Files.deleteIfExists(rulesetPath)
    }
  }

  test("throws on unsupported validator") {
    val rulesetPath = Files.createTempFile("privyspark-ruleset", ".yaml")
    val yaml =
      """rules:
        |  - pii_type: name
        |    regex: '[가-힣]{2,4}'
        |    validator: unknown_validator
        |""".stripMargin

    Files.write(rulesetPath, yaml.getBytes(StandardCharsets.UTF_8))
    try {
      assertThrows[IllegalArgumentException] {
        RulesetLoader.load(rulesetPath.toString)
      }
    } finally {
      Files.deleteIfExists(rulesetPath)
    }
  }

  test("throws on missing ruleset file") {
    assertThrows[IllegalArgumentException] {
      RulesetLoader.load("/tmp/does-not-exist-ruleset.yaml")
    }
  }
}
