package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.config.RulesetLoader
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.util.matching.Regex

@RunWith(classOf[JUnitRunner])
class RulesetLoaderSpec extends AnyFunSuite {
  test("loads default ruleset") {
    val rules = RulesetLoader.load("default")
    val driverLicenseRule = rules.find(_.piiType == "driver_license_number")
    val foreignRegistrationNumberRule = rules.find(_.piiType == "foreign_registration_number")
    assert(rules.nonEmpty)
    assert(rules.exists(_.piiType == "email"))
    assert(rules.exists(_.piiType == "passport_number"))
    assert(driverLicenseRule.nonEmpty)
    assert(driverLicenseRule.get.regex == "(?<![0-9])(?:[0-9]{10}|[0-9]{12}|[0-9]{2}-[0-9]{6}-[0-9]{2}|[0-9]{2}-[0-9]{2}-[0-9]{6}-[0-9]{2})(?![0-9])")
    assert(foreignRegistrationNumberRule.nonEmpty)
    assert(foreignRegistrationNumberRule.get.regex == "(?<![0-9])[0-9]{6}-?[5-8][0-9]{6}(?![0-9])")
    assert(!rules.exists(_.piiType == "name"))
    assert(rules.forall(_.columnHints.isEmpty))
    assert(rules.forall(_.matchType == "value"))
  }

  test("default resident registration rule accepts short and full forms without matching numeric dates") {
    val residentRegistrationNumberRule = RulesetLoader.load("default").find(_.piiType == "resident_registration_number")
    assert(residentRegistrationNumberRule.nonEmpty)

    val regex = new Regex(residentRegistrationNumberRule.get.regex)
    val fullMatchRegex = new Regex(s"\\A(?:${residentRegistrationNumberRule.get.regex})\\z").pattern

    assert(fullMatchRegex.matcher("901225-1").matches())
    assert(fullMatchRegex.matcher("9012251").matches())
    assert(fullMatchRegex.matcher("901225-1234567").matches())
    assert(fullMatchRegex.matcher("9012251234567").matches())
    assert(!regex.findFirstIn("20251027").nonEmpty)
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
