package io.github.jonggeun2001.privyspark.detect

import io.github.jonggeun2001.privyspark.PrivySparkSpecFixtures
import io.github.jonggeun2001.privyspark.config.SuppressionSet
import io.github.jonggeun2001.privyspark.model.{PiiRule, Suppression}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DetectionCountsSpec extends AnyFunSuite with PrivySparkSpecFixtures {
  private val singleExpression = DetectionAggregator.AggregationConfig(maxExpressionsPerAgg = 1)

  test("dataset counts exclude null and space-only strings but include numeric zero across batches") {
    import spark.implicits._
    val df = Seq[(String, java.lang.Integer)]((null, null), ("", 0), ("   ", 1), ("present", null)).toDF("text", "number")
    val counts = DetectionCounts.countNonEmpty(df, Seq("text", "missing", "text", "number"), singleExpression)
    assert(counts == Map("text" -> 1L, "number" -> 2L))
  }

  test("empty dataset keeps zero counts for valid target columns") {
    import spark.implicits._
    val df = Seq.empty[(String, Int)].toDF("text", "number")
    assert(DetectionCounts.countNonEmpty(df, Seq("text", "number"), singleExpression) == Map("text" -> 0L, "number" -> 0L))
    assert(DetectionCounts.countNonEmpty(df, Seq.empty, singleExpression).isEmpty)
    assert(DetectionCounts.countNonEmpty(df, Seq("missing"), singleExpression).isEmpty)
  }

  test("file counts keep groups separate and exclude null or empty file identifiers") {
    import spark.implicits._
    val df = Seq(
      ("a", "value", 0), ("a", " ", 1), ("b", null.asInstanceOf[String], 2),
      ("", "ignored", 3), (null.asInstanceOf[String], "ignored", 4)
    ).toDF("file_id", "text", "number")
    val counts = DetectionCounts.countNonEmptyByFile(df, "file_id", Seq("file_id", "text", "number", "text", "missing"), singleExpression)
    assert(counts == Map(("a", "text") -> 1L, ("a", "number") -> 2L, ("b", "text") -> 0L, ("b", "number") -> 1L))
  }

  test("empty grouped data and identifier-only targets return no counts") {
    import spark.implicits._
    val df = Seq.empty[(String, String)].toDF("file_id", "text")
    assert(DetectionCounts.countNonEmptyByFile(df, "file_id", Seq("text"), singleExpression).isEmpty)
    assert(DetectionCounts.countNonEmptyByFile(df, "file_id", Seq("file_id", "missing"), singleExpression).isEmpty)
  }

  test("invalid aggregation budgets and empty grouping identifiers fail before aggregation") {
    import spark.implicits._
    val df = Seq("value").toDF("text")
    Seq(0, -1).foreach { budget =>
      val config = DetectionAggregator.AggregationConfig(maxExpressionsPerAgg = budget)
      assertThrows[IllegalArgumentException](DetectionCounts.countNonEmpty(df, Seq("text"), config))
      assertThrows[IllegalArgumentException](DetectionCounts.countNonEmptyByFile(df, "file_id", Seq("text"), config))
    }
    assertThrows[IllegalArgumentException](DetectionCounts.countNonEmptyByFile(df, "", Seq("text"), singleExpression))
  }

  test("covered columns obey hints and suppressions without duplicates") {
    val rules = Seq(PiiRule("email", "abc", Seq("mail")), PiiRule("other", "abc", Seq("mail")))
    val suppressions = SuppressionSet.from(Seq(Suppression("blocked_mail", "email"), Suppression("blocked_mail", "other")))
    assert(DetectionCounts.columnsCoveredByRules(Seq("phone", "mail", "blocked_mail"), rules, suppressions) == Seq("mail"))
    assert(DetectionCounts.columnsCoveredByRules(Seq("mail"), Seq.empty, suppressions).isEmpty)
  }
}
