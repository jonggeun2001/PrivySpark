package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.config.RulesetLoader
import io.github.jonggeun2001.privyspark.model.PiiRuleMatchType
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
    assert(rules.find(_.piiType == "name").map(_.regex).contains(KoreanNameValidator.RuleRegex))
    assert(rules.forall(_.columnHints.isEmpty))
    assert(rules.forall(_.matchType == PiiRuleMatchType.Value))
  }

  test("loads optional column hints, validator and match type from ruleset file and ignores blank entries") {
    val rulesetPath = Files.createTempFile("privyspark-ruleset", ".yaml")
    val yaml =
      """rules:
        |  - pii_type: email
        |    regex: '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}'
        |    match_type: full_column
        |    column_hints:
        |      - email
        |      -
        |      - "   "
        |      - mail
        |  - pii_type: name
        |    regex: '(김|이|박)[가-힣]{1,2}'
        |    validator: korean_name_dict
        |    column_hints:
        |      - name
        |      -
        |      - "   "
        |      - customer
        |""".stripMargin

    Files.write(rulesetPath, yaml.getBytes(StandardCharsets.UTF_8))
    try {
      val rules = RulesetLoader.load(rulesetPath.toString)
      assert(rules.map(_.piiType) == Seq("email", "name"))
      assert(rules.head.columnHints == Seq("email", "mail"))
      assert(rules.head.matchType == PiiRuleMatchType.FullColumn)
      assert(rules.head.validator.isEmpty)
      assert(rules(1).columnHints == Seq("name", "customer"))
      assert(rules(1).validator.contains(KoreanNameValidator.ValidatorName))
      assert(rules(1).matchType == PiiRuleMatchType.Value)
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
      val error = intercept[IllegalArgumentException] {
        RulesetLoader.load(rulesetPath.toString)
      }
      assert(error.getMessage.contains("Unsupported validator"))
    } finally {
      Files.deleteIfExists(rulesetPath)
    }
  }

  test("resolves shared korean name regex reference when validator is enabled") {
    val rulesetPath = Files.createTempFile("privyspark-ruleset-shared-regex", ".yaml")
    val yaml =
      s"""rules:
         |  - pii_type: name
         |    regex: '${KoreanNameValidator.RuleRegexReference}'
         |    validator: ${KoreanNameValidator.ValidatorName}
         |""".stripMargin

    Files.write(rulesetPath, yaml.getBytes(StandardCharsets.UTF_8))
    try {
      val rules = RulesetLoader.load(rulesetPath.toString)
      assert(rules.head.regex == KoreanNameValidator.RuleRegex)
      assert(rules.head.validator.contains(KoreanNameValidator.ValidatorName))
    } finally {
      Files.deleteIfExists(rulesetPath)
    }
  }

  test("throws on unsupported match type") {
    val rulesetPath = Files.createTempFile("privyspark-ruleset-invalid", ".yaml")
    val yaml =
      """rules:
        |  - pii_type: email
        |    regex: '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}'
        |    match_type: unknown_mode
        |""".stripMargin

    Files.write(rulesetPath, yaml.getBytes(StandardCharsets.UTF_8))
    try {
      val error = intercept[IllegalArgumentException] {
        RulesetLoader.load(rulesetPath.toString)
      }
      assert(error.getMessage.contains("Unsupported match_type"))
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
