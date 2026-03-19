package io.github.jonggeun2001.privyspark.config

import io.github.jonggeun2001.privyspark.model.{PiiRule, PiiRuleMatchType}
import io.github.jonggeun2001.privyspark.validator.KoreanNameValidator
import org.yaml.snakeyaml.Yaml

import java.io.FileInputStream
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

object RulesetLoader {
  private val DefaultRulesetPath = "config/rules/default.yaml"
  private val YarnDistributedDefaultRuleset = "default-rules.yaml"

  def load(ruleset: String): Seq[PiiRule] = {
    val rulesetPath = resolvePath(ruleset)
    if (!Files.exists(rulesetPath)) {
      throw new IllegalArgumentException(s"Ruleset not found: ${rulesetPath.toAbsolutePath}")
    }

    val input = new FileInputStream(rulesetPath.toFile)
    try {
      val yaml = new Yaml()
      val root = yaml.load[java.util.Map[String, Object]](input)
      val rawRules = Option(root.get("rules"))
        .getOrElse(throw new IllegalArgumentException("rules key is required"))
        .asInstanceOf[java.util.List[java.util.Map[String, Object]]]

      val parsed = rawRules.asScala.map { item =>
        val piiType = Option(item.get("pii_type")).map(_.toString.trim).getOrElse("")
        val rawRegex = Option(item.get("regex")).map(_.toString.trim).getOrElse("")
        val columnHints = Option(item.get("column_hints")).map(parseColumnHints).getOrElse(Seq.empty)
        val validator = Option(item.get("validator")).flatMap(parseValidator)
        val regex = resolveRegex(rawRegex, validator)
        val rawMatchType = Option(item.get("match_type")).map(_.toString.trim).filter(_.nonEmpty).getOrElse(PiiRuleMatchType.Value)
        val matchType = PiiRuleMatchType.normalize(rawMatchType).getOrElse {
          throw new IllegalArgumentException(
            s"Unsupported match_type: $rawMatchType. Supported values: ${PiiRuleMatchType.Supported.toSeq.sorted.mkString(", ")}"
          )
        }

        if (piiType.isEmpty || regex.isEmpty) {
          throw new IllegalArgumentException("Each rule must include pii_type and regex")
        }

        PiiRule(
          piiType = piiType,
          regex = regex,
          columnHints = columnHints,
          validator = validator,
          matchType = matchType
        )
      }.toSeq

      if (parsed.isEmpty) {
        throw new IllegalArgumentException("rules must contain at least one rule")
      }

      parsed
    } finally {
      input.close()
    }
  }

  private def resolvePath(ruleset: String) = {
    if (ruleset == "default") {
      defaultRulesetCandidates.find(path => Files.exists(path)).getOrElse(Paths.get(DefaultRulesetPath))
    } else {
      Paths.get(ruleset)
    }
  }

  private def parseColumnHints(rawValue: Object): Seq[String] = {
    rawValue match {
      case values: java.util.List[_] =>
        values.asScala.flatMap(value => Option(value).map(_.toString.trim)).filter(_.nonEmpty).toSeq
      case value =>
        Option(value).map(_.toString.trim).filter(_.nonEmpty).toSeq
    }
  }

  private def parseValidator(rawValue: Object): Option[String] = {
    Option(rawValue).map(_.toString.trim).filter(_.nonEmpty).map { validator =>
      if (validator != KoreanNameValidator.ValidatorName) {
        throw new IllegalArgumentException(s"Unsupported validator: $validator")
      }
      validator
    }
  }

  private def resolveRegex(rawRegex: String, validator: Option[String]): String = {
    rawRegex match {
      case KoreanNameValidator.RuleRegexReference =>
        validator match {
          case Some(KoreanNameValidator.ValidatorName) => KoreanNameValidator.RuleRegex
          case _ =>
            throw new IllegalArgumentException(
              s"${KoreanNameValidator.RuleRegexReference} can only be used with validator ${KoreanNameValidator.ValidatorName}"
            )
        }
      case other => other
    }
  }

  private def defaultRulesetCandidates = {
    val envPath = sys.env.get("PRIVYSPARK_DEFAULT_RULESET").map(Paths.get(_))
    val yarnDistributed = Some(Paths.get(YarnDistributedDefaultRuleset))
    val projectLocal = Some(Paths.get(DefaultRulesetPath))
    Seq(envPath, yarnDistributed, projectLocal).flatten
  }
}
