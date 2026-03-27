package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.config.RulesetLoader
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
    assert(!rules.exists(_.piiType == "name"))
    assert(rules.forall(_.columnHints.isEmpty))
    assert(rules.forall(_.matchType == "value"))
  }

  test("loads optional column hints and match type from ruleset file and ignores blank entries") {
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
        |  - pii_type: phone_number
        |    regex: '01[016789]-?[0-9]{3,4}-?[0-9]{4}'
        |""".stripMargin

    Files.write(rulesetPath, yaml.getBytes(StandardCharsets.UTF_8))
    try {
      val rules = RulesetLoader.load(rulesetPath.toString)
      assert(rules.map(_.piiType) == Seq("email", "phone_number"))
      assert(rules.head.columnHints == Seq("email", "mail"))
      assert(rules.head.matchType == "full_column")
      assert(rules(1).columnHints.isEmpty)
      assert(rules(1).matchType == "value")
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

  test("throws when ruleset still uses validator field") {
    val rulesetPath = Files.createTempFile("privyspark-ruleset-validator", ".yaml")
    val yaml =
      """rules:
        |  - pii_type: email
        |    regex: '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}'
        |    validator: korean_name_dict
        |""".stripMargin

    Files.write(rulesetPath, yaml.getBytes(StandardCharsets.UTF_8))
    try {
      val error = intercept[IllegalArgumentException] {
        RulesetLoader.load(rulesetPath.toString)
      }
      assert(error.getMessage.contains("validator is no longer supported"))
    } finally {
      Files.deleteIfExists(rulesetPath)
    }
  }

  test("throws when ruleset defines removed name pii type") {
    val rulesetPath = Files.createTempFile("privyspark-ruleset-name", ".yaml")
    val yaml =
      """rules:
        |  - pii_type: name
        |    regex: '[가-힣]{2,4}'
        |""".stripMargin

    Files.write(rulesetPath, yaml.getBytes(StandardCharsets.UTF_8))
    try {
      val error = intercept[IllegalArgumentException] {
        RulesetLoader.load(rulesetPath.toString)
      }
      assert(error.getMessage.contains("pii_type 'name' is no longer supported"))
    } finally {
      Files.deleteIfExists(rulesetPath)
    }
  }

  test("throws when ruleset still uses removed internal regex reference") {
    val rulesetPath = Files.createTempFile("privyspark-ruleset-regex-ref", ".yaml")
    val yaml =
      """rules:
        |  - pii_type: email
        |    regex: '__KOREAN_NAME_RULE_REGEX__'
        |""".stripMargin

    Files.write(rulesetPath, yaml.getBytes(StandardCharsets.UTF_8))
    try {
      val error = intercept[IllegalArgumentException] {
        RulesetLoader.load(rulesetPath.toString)
      }
      assert(error.getMessage.contains("__KOREAN_NAME_RULE_REGEX__ is no longer supported"))
    } finally {
      Files.deleteIfExists(rulesetPath)
    }
  }

  test("throws when ruleset embeds removed internal regex reference inside a larger pattern") {
    val rulesetPath = Files.createTempFile("privyspark-ruleset-regex-ref-embedded", ".yaml")
    val yaml =
      """rules:
        |  - pii_type: email
        |    regex: '(?:__KOREAN_NAME_RULE_REGEX__)|[가-힣]{2,4}'
        |""".stripMargin

    Files.write(rulesetPath, yaml.getBytes(StandardCharsets.UTF_8))
    try {
      val error = intercept[IllegalArgumentException] {
        RulesetLoader.load(rulesetPath.toString)
      }
      assert(error.getMessage.contains("__KOREAN_NAME_RULE_REGEX__ is no longer supported"))
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
