package io.github.jonggeun2001.privyspark.config

import io.github.jonggeun2001.privyspark.model.PiiRule
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
        val regex = Option(item.get("regex")).map(_.toString.trim).getOrElse("")

        if (piiType.isEmpty || regex.isEmpty) {
          throw new IllegalArgumentException("Each rule must include pii_type and regex")
        }

        PiiRule(piiType, regex)
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

  private def defaultRulesetCandidates = {
    val envPath = sys.env.get("PRIVYSPARK_DEFAULT_RULESET").map(Paths.get(_))
    val yarnDistributed = Some(Paths.get(YarnDistributedDefaultRuleset))
    val projectLocal = Some(Paths.get(DefaultRulesetPath))
    Seq(envPath, yarnDistributed, projectLocal).flatten
  }
}
