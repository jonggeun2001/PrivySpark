package io.github.jonggeun2001.privyspark.detect

import io.github.jonggeun2001.privyspark.config.SuppressionSet
import io.github.jonggeun2001.privyspark.model.{PiiRule, PiiRuleMatchType, Suppression}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.util.regex.PatternSyntaxException

@RunWith(classOf[JUnitRunner])
class DetectionMetricsSpec extends AnyFunSuite {
  test("metric planning preserves column order, rule aliases and full-column matching") {
    val rules = Seq(PiiRule("value", "abc"), PiiRule("full", "abc", matchType = PiiRuleMatchType.FullColumn))
    val metrics = DetectionMetrics.buildMetrics(Seq("first", "second"), rules, SuppressionSet.empty)

    assert(metrics.map(_.alias) == Seq("m_0_0", "m_0_1", "m_1_0", "m_1_1"))
    assert(metrics.map(_.metricKey) == Seq("first#0", "first#1", "second#0", "second#1"))
    assert(metrics.map(_.pattern.matcher("xabcx").find()) == Seq(true, false, true, false))
    assert(metrics.forall(_.pattern.matcher("abc").matches()))
  }

  test("invalid regex remains unevaluated when excluded by hints or suppressions") {
    val rules = Seq(
      PiiRule("hinted", "[", Seq("other")),
      PiiRule("suppressed", "["),
      PiiRule("valid", "abc", Seq(" MAIL "))
    )
    val suppressions = SuppressionSet.from(Seq(Suppression("mail", "suppressed")))
    val metrics = DetectionMetrics.buildMetrics(Seq("mail"), rules, suppressions)

    assert(metrics.map(_.metricKey) == Seq("mail#2"))
    assert(DetectionMetrics.buildMetrics(Seq.empty, rules, SuppressionSet.empty).isEmpty)
    intercept[PatternSyntaxException] {
      DetectionMetrics.buildMetrics(Seq("other"), rules, SuppressionSet.empty)
    }
  }
}
