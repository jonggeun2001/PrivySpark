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
    val passportRule = rules.find(_.piiType == "passport_number")
    assert(rules.nonEmpty)
    assert(rules.exists(_.piiType == "email"))
    assert(rules.exists(_.piiType == "passport_number"))
    assert(driverLicenseRule.nonEmpty)
    assert(driverLicenseRule.get.regex == "(?:(?<![0-9])(?:[0-9]{2}-[0-9]{6}-[0-9]{2}|(?:1[1-9]|2[0-6]|28)-[0-9]{2}-[0-9]{6}-[0-9]{2}|(?:1[1-9]|2[0-6]|28)[0-9]{10})(?![0-9])|(?<![가-힣A-Za-z0-9])(?:서울|부산|경기|강원|충북|충남|전북|전남|경북|경남|제주|대구|인천|광주|대전|울산)\\s*(?:[0-9]{10}|[0-9]{2}\\s*-\\s*[0-9]{6}\\s*-\\s*[0-9]{2})(?![가-힣A-Za-z0-9]))")
    assert(foreignRegistrationNumberRule.nonEmpty)
    assert(foreignRegistrationNumberRule.get.regex == "(?<![0-9])[0-9]{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01])(?:-[5-8][0-9]{6}|[5-8][0-9]{6})(?![0-9])")
    assert(passportRule.nonEmpty)
    assert(passportRule.get.regex == "(?<![A-Za-z0-9])[MSROD](?!0{8})[0-9]{8}(?![A-Za-z0-9])")
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
    assert(!fullMatchRegex.matcher("901332-1234567").matches())
    assert(!fullMatchRegex.matcher("9013001234567").matches())
    assert(!fullMatchRegex.matcher("901232-1234567").matches())
    assert(!fullMatchRegex.matcher("9012001234567").matches())
    assert(!regex.findFirstIn("20251027").nonEmpty)
  }

  test("default phone rule accepts domestic and +82 mobile forms") {
    val phoneRule = RulesetLoader.load("default").find(_.piiType == "phone_number")
    assert(phoneRule.nonEmpty)

    val regex = new Regex(phoneRule.get.regex)
    val fullMatchRegex = new Regex(s"\\A(?:${phoneRule.get.regex})\\z").pattern

    assert(fullMatchRegex.matcher("010-1234-5678").matches())
    assert(fullMatchRegex.matcher("01012345678").matches())
    assert(fullMatchRegex.matcher("+821012345678").matches())
    assert(fullMatchRegex.matcher("+82-10-1234-5678").matches())
    assert(regex.findFirstIn("call me at +821012345678 now").contains("+821012345678"))
    assert(!fullMatchRegex.matcher("+821312345678").matches())
    assert(!fullMatchRegex.matcher("821012345678").matches())
    assert(!regex.findFirstIn("+8210123456789").nonEmpty)
    assert(!regex.findFirstIn("010123456789").nonEmpty)
  }

  test("default foreign registration rule constrains month and day and avoids numeric token false positives") {
    val foreignRegistrationRule = RulesetLoader.load("default").find(_.piiType == "foreign_registration_number")
    assert(foreignRegistrationRule.nonEmpty)

    val regex = new Regex(foreignRegistrationRule.get.regex)
    val fullMatchRegex = new Regex(s"\\A(?:${foreignRegistrationRule.get.regex})\\z").pattern

    assert(fullMatchRegex.matcher("880101-5123456").matches())
    assert(fullMatchRegex.matcher("8801015123456").matches())
    assert(!fullMatchRegex.matcher("881332-5123456").matches())
    assert(!fullMatchRegex.matcher("8813005123456").matches())
    assert(!fullMatchRegex.matcher("880101-9123456").matches())
    assert(!regex.findFirstIn("20251027").nonEmpty)
  }

  test("default driver license rule keeps legacy formats, supports Korean region names, and constrains current regional prefixes") {
    val driverLicenseRule = RulesetLoader.load("default").find(_.piiType == "driver_license_number")
    assert(driverLicenseRule.nonEmpty)

    val regex = new Regex(driverLicenseRule.get.regex)
    val fullMatchRegex = new Regex(s"\\A(?:${driverLicenseRule.get.regex})\\z").pattern

    assert(fullMatchRegex.matcher("12-345678-90").matches())
    assert(!fullMatchRegex.matcher("1234567890").matches())
    assert(fullMatchRegex.matcher("11-12-345678-90").matches())
    assert(fullMatchRegex.matcher("111234567890").matches())
    assert(fullMatchRegex.matcher("서울 07 - 111111 - 10").matches())
    assert(fullMatchRegex.matcher("부산0711111110").matches())
    assert(!fullMatchRegex.matcher("27-12-345678-90").matches())
    assert(!fullMatchRegex.matcher("271234567890").matches())
    assert(!fullMatchRegex.matcher("세종 07 - 111111 - 10").matches())
    assert(!regex.findFirstIn("1112345678901").nonEmpty)
  }

  test("default token-based rules reduce common false positives for email, bank account, credit card, passport, and ip address") {
    val defaultRules = RulesetLoader.load("default")
    val emailRule = defaultRules.find(_.piiType == "email").get
    val bankAccountRule = defaultRules.find(_.piiType == "bank_account_number").get
    val creditCardRule = defaultRules.find(_.piiType == "credit_card_number").get
    val passportRule = defaultRules.find(_.piiType == "passport_number").get
    val ipAddressRule = defaultRules.find(_.piiType == "ip_address").get

    val emailRegex = new Regex(emailRule.regex)
    val emailFullMatch = new Regex(s"\\A(?:${emailRule.regex})\\z").pattern
    assert(emailFullMatch.matcher("alice@example.com").matches())
    assert(emailFullMatch.matcher("alice+tag@example.co.kr").matches())
    assert(emailRegex.findFirstIn("contact:alice@example.com").contains("alice@example.com"))
    assert(!emailFullMatch.matcher("a@b.c").matches())
    assert(!emailFullMatch.matcher("user@example.123").matches())
    assert(!emailFullMatch.matcher("alice@example..com").matches())
    assert(!emailFullMatch.matcher("alice@example-.com").matches())
    assert(!emailRegex.findFirstIn("alice@example.com_suffix").nonEmpty)

    val bankAccountFullMatch = new Regex(s"\\A(?:${bankAccountRule.regex})\\z").pattern
    assert(bankAccountFullMatch.matcher("110-123-456789").matches())
    assert(bankAccountFullMatch.matcher("1234-567-890123").matches())
    assert(!bankAccountFullMatch.matcher("2024-01-01").matches())
    assert(!bankAccountFullMatch.matcher("1234-01-01").matches())

    val creditCardFullMatch = new Regex(s"\\A(?:${creditCardRule.regex})\\z").pattern
    assert(creditCardFullMatch.matcher("4111111111111111").matches())
    assert(creditCardFullMatch.matcher("5555555555554444").matches())
    assert(creditCardFullMatch.matcher("6011111111111117").matches())
    assert(creditCardFullMatch.matcher("3566002020360505").matches())
    assert(!creditCardFullMatch.matcher("2220111111111111").matches())
    assert(!creditCardFullMatch.matcher("2721111111111111").matches())
    assert(!creditCardFullMatch.matcher("2025102712345678").matches())
    assert(!creditCardFullMatch.matcher("1234-5678-9012-3456").matches())

    val passportFullMatch = new Regex(s"\\A(?:${passportRule.regex})\\z").pattern
    assert(passportFullMatch.matcher("M12345678").matches())
    assert(!passportFullMatch.matcher("S00000000").matches())
    assert(!passportFullMatch.matcher("XM12345678").matches())

    val ipRegex = new Regex(ipAddressRule.regex)
    val ipFullMatch = new Regex(s"\\A(?:${ipAddressRule.regex})\\z").pattern
    assert(ipFullMatch.matcher("192.168.0.1").matches())
    assert(ipRegex.findFirstIn("server=192.168.0.1").contains("192.168.0.1"))
    assert(ipRegex.findFirstIn("server=192.168.0.1.").contains("192.168.0.1"))
    assert(!ipFullMatch.matcher("256.168.0.1").matches())
    assert(!ipRegex.findFirstIn("10.0.0.1.5").nonEmpty)
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

  test("throws on invalid regex before scanning starts") {
    val rulesetPath = Files.createTempFile("privyspark-ruleset-invalid-regex", ".yaml")
    val yaml =
      """rules:
        |  - pii_type: email
        |    regex: '[A-Za-z'
        |""".stripMargin

    Files.write(rulesetPath, yaml.getBytes(StandardCharsets.UTF_8))
    try {
      val error = intercept[IllegalArgumentException] {
        RulesetLoader.load(rulesetPath.toString)
      }
      assert(error.getMessage.contains("Invalid regex"))
      assert(error.getMessage.contains("email"))
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

  test("rejects duplicate pii types in a ruleset") {
    val rulesetPath = Files.createTempFile("privyspark-ruleset-duplicate-pii", ".yaml")
    val yaml =
      """rules:
        |  - pii_type: email
        |    regex: '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}'
        |  - pii_type: email
        |    regex: 'support@[A-Za-z0-9.-]+\.[A-Za-z]{2,}'
        |""".stripMargin

    Files.write(rulesetPath, yaml.getBytes(StandardCharsets.UTF_8))
    try {
      val error = intercept[IllegalArgumentException] {
        RulesetLoader.load(rulesetPath.toString)
      }
      assert(error.getMessage.contains("Duplicate pii_type"))
      assert(error.getMessage.contains("email"))
    } finally {
      Files.deleteIfExists(rulesetPath)
    }
  }
}
