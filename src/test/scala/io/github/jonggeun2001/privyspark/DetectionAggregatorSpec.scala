package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.DetectionAggregator.{AggregationConfig, FileMatchCount, MatchCount}
import io.github.jonggeun2001.privyspark.model.{PiiRule, PiiRuleMatchType}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.functions.{col, trim, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger

@RunWith(classOf[JUnitRunner])
class DetectionAggregatorSpec extends AnyFunSuite with BeforeAndAfterAll {
  private val spark = SparkSession.builder()
    .appName("DetectionAggregatorSpec")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("matches legacy filter/count behavior including null values") {
    val df = Seq(
      ("alpha@example.com", "010-1234-5678", null.asInstanceOf[String]),
      (null.asInstanceOf[String], "noise", "beta@example.com"),
      ("invalid", null.asInstanceOf[String], "text")
    ).toDF("c_email", "c_phone", "c_mixed")

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
    )

    val actual = sortByKey(DetectionAggregator.aggregate(df, rules))
    val expected = sortByKey(legacyCounts(df, rules))

    assert(actual == expected)
  }

  test("uses legacy fallback when metric count exceeds threshold") {
    val df = Seq(
      ("alpha@example.com", "010-1234-5678"),
      ("beta@example.com", "010-9876-5432")
    ).toDF("c1", "c2")

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
    )

    val forcedFallback = DetectionAggregator.aggregate(
      df,
      rules,
      AggregationConfig(maxExpressionsPerAgg = 2, legacyFallbackThreshold = 1)
    )

    val expected = legacyCounts(df, rules)

    assert(sortByKey(forcedFallback) == sortByKey(expected))
  }

  test("logs dataset fallback when metric count exceeds threshold") {
    val df = Seq(
      ("alpha@example.com", "010-1234-5678"),
      ("beta@example.com", "010-9876-5432")
    ).toDF("c1", "c2")

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
    )

    val logs = captureStderr {
      DetectionAggregator.aggregate(
        df,
        rules,
        AggregationConfig(maxExpressionsPerAgg = 2, legacyFallbackThreshold = 1)
      )
    }

    assert(logs.contains("detection_aggregation_fallback"))
    assert(logs.contains("scope=dataset"))
    assert(logs.contains("metric_threshold_exceeded(1)"))
  }

  test("logs dataset aggregation debug lifecycle") {
    val df = Seq(
      ("alpha@example.com", "010-1234-5678"),
      ("beta@example.com", "010-9876-5432")
    ).toDF("c1", "c2")

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
    )

    val logs = captureStderr {
      withDebugLoggingEnabled {
        DetectionAggregator.aggregate(df, rules)
      }
    }

    assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[DEBUG\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] detection_aggregation_start scope=dataset.*""")))
    assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[DEBUG\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] detection_aggregation_metrics_built scope=dataset.*""")))
    assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[DEBUG\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] detection_aggregation_complete scope=dataset.*""")))
  }

  test("suppresses dataset fallback logs when driver log level is off") {
    val df = Seq(
      ("alpha@example.com", "010-1234-5678"),
      ("beta@example.com", "010-9876-5432")
    ).toDF("c1", "c2")

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
    )

    val logs = captureStderr {
      withDriverLogLevel("off") {
        DetectionAggregator.aggregate(
          df,
          rules,
          AggregationConfig(maxExpressionsPerAgg = 2, legacyFallbackThreshold = 1)
        )
      }
    }

    assert(!logs.contains("detection_aggregation_fallback"))
    assert(!logs.contains("[PrivySpark][DEBUG]"))
  }

  test("filters dataset metrics by column hints before aggregation") {
    val df = Seq(
      ("alpha@example.com", "010-1234-5678", "alpha@example.com"),
      ("beta@example.com", "010-9999-8888", "010-9999-8888")
    ).toDF("customer_email", "contact_phone", "notes")

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}", Seq("email", "mail")),
      PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b", Seq("phone", "mobile"))
    )

    val actual = sortByKey(DetectionAggregator.aggregate(df, rules))
    val expected = Seq(
      MatchCount("contact_phone", "phone", 2L),
      MatchCount("customer_email", "email", 2L)
    )

    assert(actual == sortByKey(expected))
  }

  test("counts only full-value matches for full-column rules") {
    val df = Seq(
      ("alpha@example.com", "prefix alpha@example.com", "alpha@example.com"),
      ("beta@example.com", "beta@example.com suffix", "not-an-email"),
      ("", "", ""),
      (null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String])
    ).toDF("strict_email", "embedded_email", "mixed_email")

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}", matchType = PiiRuleMatchType.FullColumn)
    )

    val actual = sortByKey(DetectionAggregator.aggregate(df, rules))
    val expected = sortByKey(
      Seq(
        MatchCount("mixed_email", "email", 1L),
        MatchCount("strict_email", "email", 2L)
      )
    )

    assert(actual == expected)
  }

  test("produces correct results when aggregation is split into batches") {
    val columnCount = 32
    val columns = (1 to columnCount).map(i => s"c$i")

    val row1 = columns.map {
      case "c1" => "alpha@example.com"
      case "c2" => "010-1234-5678"
      case _ => "x"
    }

    val row2 = columns.map {
      case "c3" => "beta@example.com"
      case "c4" => "010-9999-8888"
      case _ => null
    }

    val df = Seq(Tuple1(row1), Tuple1(row2)).toDF("values")
      .select(columns.zipWithIndex.map { case (name, idx) => col("values")(idx).cast(StringType).as(name) }: _*)

    val rules = (1 to 20).map(i => PiiRule(s"email_rule_$i", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}")) ++
      (1 to 20).map(i => PiiRule(s"phone_rule_$i", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b"))

    val actual = DetectionAggregator.aggregate(
      df,
      rules,
      AggregationConfig(maxExpressionsPerAgg = 80, legacyFallbackThreshold = 10000)
    )

    val expected = rowBasedExpected(df, rules)

    assert(sortByKey(actual) == sortByKey(expected))
  }

  test("sampleMatches extracts dataset samples with bounded Spark jobs per batch") {
    val df = Seq(
      ("alpha@example.com", "010-1234-5678"),
      ("beta@example.com", "noise"),
      ("noise", "010-9999-8888")
    ).toDF("c_email", "c_phone")

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
    )

    val matchCounts = DetectionAggregator.aggregate(df, rules)
    val (samples, jobCount) = captureJobCount {
      DetectionAggregator.sampleMatches(df, rules, matchCounts)
    }
    val emailMatch = matchCounts.find(matchCount => matchCount.columnName == "c_email" && matchCount.piiType == "email").get
    val phoneMatch = matchCounts.find(matchCount => matchCount.columnName == "c_phone" && matchCount.piiType == "phone").get

    assert(jobCount == 2, s"expected two Spark jobs for batched sample extraction, found $jobCount")
    assert(samples(emailMatch.metricAlias).sampleMatchedFragment == "alpha@example.com")
    assert(samples(phoneMatch.metricAlias).sampleMatchedFragment == "010-1234-5678")
  }

  test("sampleMatches respects aggregation fallback config") {
    val df = Seq(
      ("alpha@example.com", "010-1234-5678"),
      ("beta@example.com", "noise"),
      ("noise", "010-9999-8888")
    ).toDF("c_email", "c_phone")

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
    )

    val config = AggregationConfig(maxExpressionsPerAgg = 1, legacyFallbackThreshold = 1)
    val matchCounts = DetectionAggregator.aggregate(df, rules, config)
    val logs = captureStderr {
      val samples = DetectionAggregator.sampleMatches(df, rules, matchCounts, config)
      val emailMatch = matchCounts.find(matchCount => matchCount.columnName == "c_email" && matchCount.piiType == "email").get
      val phoneMatch = matchCounts.find(matchCount => matchCount.columnName == "c_phone" && matchCount.piiType == "phone").get
      assert(samples(emailMatch.metricAlias).sampleMatchedFragment == "alpha@example.com")
      assert(samples(phoneMatch.metricAlias).sampleMatchedFragment == "010-1234-5678")
    }

    assert(logs.contains("detection_aggregation_fallback"))
    assert(logs.contains("scope=dataset_sample"))
    assert(logs.contains("metric_threshold_exceeded(1)"))
  }

  test("sampleMatches truncates long full-column sample raw values") {
    val rawValue = ("A" * 80) + ("B" * 80)
    val df = Seq(rawValue).toDF("strict_value")
    val rules = Seq(PiiRule("freeform_text", ".+", matchType = PiiRuleMatchType.FullColumn))

    val matchCounts = DetectionAggregator.aggregate(df, rules)
    val samples = DetectionAggregator.sampleMatches(df, rules, matchCounts)
    val matchCount = matchCounts.head

    assert(samples(matchCount.metricAlias).sampleMatchedFragment == rawValue)
    assert(samples(matchCount.metricAlias).sampleRawValue == (rawValue.take(50) + "..." + rawValue.takeRight(50)))
  }

  test("sampleMatches keeps samples when only one duplicate pii-type rule matches for a column") {
    val df = Seq("alpha@example.com").toDF("customer_email")
    val rules = Seq(
      PiiRule("email", "support@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}")
    )

    val matchCounts = DetectionAggregator.aggregate(df, rules)
    val samples = DetectionAggregator.sampleMatches(df, rules, matchCounts)

    assert(matchCounts.size == 1)
    assert(samples(matchCounts.head.metricAlias).sampleMatchedFragment == "alpha@example.com")
  }

  test("sampleMatches keeps distinct samples for duplicate pii-type rules on the same column") {
    val df = Seq(
      "support@example.com",
      "sales@example.com"
    ).toDF("customer_email")
    val rules = Seq(
      PiiRule("email", "support@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("email", "sales@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}")
    )

    val matchCounts = DetectionAggregator.aggregate(df, rules)
    val samples = DetectionAggregator.sampleMatches(df, rules, matchCounts)

    assert(matchCounts.size == 2)
    assert(matchCounts.flatMap(matchCount => samples.get(matchCount.metricAlias).map(_.sampleMatchedFragment)).toSet ==
      Set("support@example.com", "sales@example.com"))
  }

  test("safe dataset sample fallback batches raw value collection") {
    val df = spark.sparkContext.parallelize(Seq(
      ("alpha@example.com", "010-1234-5678", "noise", "noise"),
      ("noise", "noise", "beta@example.com", "010-9999-8888"),
      ("gamma@example.com", "010-2222-3333", "delta@example.com", "010-4444-5555"),
      ("noise", "noise", "noise", "noise")
    ), 2).toDF("email_1", "phone_1", "email_2", "phone_2")

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
    )

    val (rawValues, jobCount) = captureJobCount {
      invokeDatasetSafeRawCollector(df, rules)
    }

    assert(jobCount == 1, s"expected one Spark job for batched dataset safe sample collection, found $jobCount")
    assert(rawValues == Map(
      "m_0_0" -> "alpha@example.com",
      "m_1_1" -> "010-1234-5678",
      "m_2_0" -> "beta@example.com",
      "m_3_1" -> "010-9999-8888"
    ))
  }

  test("aggregateByFile matches legacy per-file behavior") {
    val df = Seq(
      ("alpha.csv", "alpha@example.com", "010-1234-5678"),
      ("alpha.csv", "noise", "none"),
      ("beta.csv", "beta@example.com", "010-9999-8888"),
      ("beta.csv", null.asInstanceOf[String], null.asInstanceOf[String])
    ).toDF("file_id", "c_email", "c_phone")

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
    )

    val actual = sortByFileKey(DetectionAggregator.aggregateByFile(df, "file_id", rules))
    val expected = sortByFileKey(legacyCountsByFile(df, "file_id", rules))

    assert(actual == expected)
  }

  test("aggregateByFile supports batch split and fallback path") {
    val df = Seq(
      ("alpha.csv", "alpha@example.com", "010-1234-5678"),
      ("beta.csv", "beta@example.com", "010-9999-8888")
    ).toDF("file_id", "c1", "c2")

    val rules = (1 to 10).map(i => PiiRule(s"email_rule_$i", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}")) ++
      (1 to 10).map(i => PiiRule(s"phone_rule_$i", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b"))

    val expected = sortByFileKey(legacyCountsByFile(df, "file_id", rules))

    val batched = DetectionAggregator.aggregateByFile(
      df,
      "file_id",
      rules,
      AggregationConfig(maxExpressionsPerAgg = 8, legacyFallbackThreshold = 10000)
    )
    assert(sortByFileKey(batched) == expected)

    val forcedFallback = DetectionAggregator.aggregateByFile(
      df,
      "file_id",
      rules,
      AggregationConfig(maxExpressionsPerAgg = 8, legacyFallbackThreshold = 1)
    )
    assert(sortByFileKey(forcedFallback) == expected)
  }

  test("aggregateByFile logs fallback when legacy path is selected") {
    val df = Seq(
      ("alpha.csv", "alpha@example.com", "010-1234-5678"),
      ("beta.csv", "beta@example.com", "010-9999-8888")
    ).toDF("file_id", "c1", "c2")

    val rules = (1 to 10).map(i => PiiRule(s"email_rule_$i", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}")) ++
      (1 to 10).map(i => PiiRule(s"phone_rule_$i", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b"))

    val logs = captureStderr {
      DetectionAggregator.aggregateByFile(
        df,
        "file_id",
        rules,
        AggregationConfig(maxExpressionsPerAgg = 8, legacyFallbackThreshold = 1)
      )
    }

    assert(logs.contains("detection_aggregation_fallback"))
    assert(logs.contains("scope=file"))
    assert(logs.contains("metric_threshold_exceeded(1)"))
  }

  test("logs file aggregation debug lifecycle") {
    val df = Seq(
      ("alpha.csv", "alpha@example.com", "010-1234-5678"),
      ("beta.csv", "beta@example.com", "010-9999-8888")
    ).toDF("file_id", "c1", "c2")

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
    )

    val logs = captureStderr {
      withDebugLoggingEnabled {
        DetectionAggregator.aggregateByFile(df, "file_id", rules)
      }
    }

    assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[DEBUG\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] detection_aggregation_start scope=file.*""")))
    assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[DEBUG\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] detection_aggregation_metrics_built scope=file.*""")))
    assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[DEBUG\]\[\d{4}-\d{2}-\d{2}T[^\]]+Z\] detection_aggregation_complete scope=file.*""")))
  }

  test("reports threshold fallback mode when threshold batch fallback succeeds") {
    val expected = Seq(MatchCount("customer_email", "email", 2L))

    val fallback = DetectionAggregator.executeThresholdFallback(
      "dataset",
      expressionCount = 4,
      threshold = 1,
      batchedFallback = expected,
      legacyFallback = Seq.empty
    )

    assert(fallback == ((expected, "threshold_fallback")))
  }

  test("reports legacy fallback mode when threshold batch fallback fails") {
    val expected = Seq(MatchCount("customer_email", "email", 2L))

    val fallback = DetectionAggregator.executeThresholdFallback(
      "dataset",
      expressionCount = 4,
      threshold = 1,
      batchedFallback = throw new RuntimeException("forced-threshold-fallback-failure"),
      legacyFallback = expected
    )

    assert(fallback == ((expected, "legacy_fallback")))
  }

  test("filters file metrics by column hints before aggregation") {
    val df = Seq(
      ("alpha.csv", "alpha@example.com", "010-1234-5678", "alpha@example.com"),
      ("beta.csv", "beta@example.com", "010-9999-8888", "010-9999-8888")
    ).toDF("file_id", "customer_email", "contact_phone", "notes")

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}", Seq("email", "mail")),
      PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b", Seq("phone", "mobile"))
    )

    val actual = sortByFileKey(DetectionAggregator.aggregateByFile(df, "file_id", rules))
    val expected = Seq(
      FileMatchCount("alpha.csv", "contact_phone", "phone", 1L),
      FileMatchCount("alpha.csv", "customer_email", "email", 1L),
      FileMatchCount("beta.csv", "contact_phone", "phone", 1L),
      FileMatchCount("beta.csv", "customer_email", "email", 1L)
    )

    assert(actual == sortByFileKey(expected))
  }

  test("sampleMatchesByFile extracts per-file samples with bounded Spark jobs per batch") {
    val df = Seq(
      ("alpha.csv", "alpha@example.com", "010-1234-5678"),
      ("alpha.csv", "noise", "none"),
      ("beta.csv", "beta@example.com", "010-9999-8888")
    ).toDF("file_id", "c_email", "c_phone")

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
    )

    val matchCounts = DetectionAggregator.aggregateByFile(df, "file_id", rules)
    val (samples, jobCount) = captureJobCount {
      DetectionAggregator.sampleMatchesByFile(df, "file_id", rules, matchCounts)
    }
    val alphaEmail = matchCounts.find(matchCount =>
      matchCount.fileIdentifier == "alpha.csv" && matchCount.columnName == "c_email" && matchCount.piiType == "email"
    ).get
    val betaPhone = matchCounts.find(matchCount =>
      matchCount.fileIdentifier == "beta.csv" && matchCount.columnName == "c_phone" && matchCount.piiType == "phone"
    ).get

    assert(jobCount == 2, s"expected two Spark jobs for batched per-file sample extraction, found $jobCount")
    assert(samples((alphaEmail.fileIdentifier, alphaEmail.metricAlias)).sampleMatchedFragment == "alpha@example.com")
    assert(samples((betaPhone.fileIdentifier, betaPhone.metricAlias)).sampleMatchedFragment == "010-9999-8888")
  }

  test("sampleMatchesByFile respects aggregation fallback config") {
    val df = Seq(
      ("alpha.csv", "alpha@example.com", "010-1234-5678"),
      ("alpha.csv", "noise", "none"),
      ("beta.csv", "beta@example.com", "010-9999-8888")
    ).toDF("file_id", "c_email", "c_phone")

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
    )

    val config = AggregationConfig(maxExpressionsPerAgg = 1, legacyFallbackThreshold = 1)
    val matchCounts = DetectionAggregator.aggregateByFile(df, "file_id", rules, config)
    val logs = captureStderr {
      val samples = DetectionAggregator.sampleMatchesByFile(df, "file_id", rules, matchCounts, config)
      val alphaEmail = matchCounts.find(matchCount =>
        matchCount.fileIdentifier == "alpha.csv" && matchCount.columnName == "c_email" && matchCount.piiType == "email"
      ).get
      val betaPhone = matchCounts.find(matchCount =>
        matchCount.fileIdentifier == "beta.csv" && matchCount.columnName == "c_phone" && matchCount.piiType == "phone"
      ).get
      assert(samples((alphaEmail.fileIdentifier, alphaEmail.metricAlias)).sampleMatchedFragment == "alpha@example.com")
      assert(samples((betaPhone.fileIdentifier, betaPhone.metricAlias)).sampleMatchedFragment == "010-9999-8888")
    }

    assert(logs.contains("detection_aggregation_fallback"))
    assert(logs.contains("scope=file_sample"))
    assert(logs.contains("metric_threshold_exceeded(1)"))
  }

  test("sampleMatchesByFile recovers with safe fallback when batched file sample extraction fails") {
    val df = Seq(
      ("alpha.csv", "alpha@example.com", "010-1234-5678"),
      ("alpha.csv", "noise", "none"),
      ("beta.csv", "beta@example.com", "010-9999-8888")
    ).toDF("file_id", "c_email", "c_phone")

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
    )

    val matchCounts = DetectionAggregator.aggregateByFile(df, "file_id", rules)
    val logs = captureStderr {
      val samples = DetectionAggregator.withForcedFileSampleBatchFailure {
        DetectionAggregator.sampleMatchesByFile(
          df,
          "file_id",
          rules,
          matchCounts,
          AggregationConfig(maxExpressionsPerAgg = 8, legacyFallbackThreshold = 10000)
        )
      }

      val alphaEmail = matchCounts.find(matchCount =>
        matchCount.fileIdentifier == "alpha.csv" && matchCount.columnName == "c_email" && matchCount.piiType == "email"
      ).get
      val betaPhone = matchCounts.find(matchCount =>
        matchCount.fileIdentifier == "beta.csv" && matchCount.columnName == "c_phone" && matchCount.piiType == "phone"
      ).get

      assert(samples((alphaEmail.fileIdentifier, alphaEmail.metricAlias)).sampleMatchedFragment == "alpha@example.com")
      assert(samples((betaPhone.fileIdentifier, betaPhone.metricAlias)).sampleMatchedFragment == "010-9999-8888")
    }

    assert(logs.contains("detection_aggregation_fallback"))
    assert(logs.contains("scope=file_sample"))
    assert(logs.contains("forced-file-sample-batch-failure"))
  }

  test("counts only full-value matches per file for full-column rules") {
    val df = Seq(
      ("alpha.csv", "alpha@example.com"),
      ("alpha.csv", "beta@example.com suffix"),
      ("beta.csv", "beta@example.com"),
      ("beta.csv", ""),
      ("beta.csv", null.asInstanceOf[String]),
      ("gamma.csv", "not-an-email")
    ).toDF("file_id", "customer_email")

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}", matchType = PiiRuleMatchType.FullColumn)
    )

    val actual = sortByFileKey(DetectionAggregator.aggregateByFile(df, "file_id", rules))
    val expected = sortByFileKey(
      Seq(
        FileMatchCount("alpha.csv", "customer_email", "email", 1L),
        FileMatchCount("beta.csv", "customer_email", "email", 1L)
      )
    )

    assert(actual == expected)
  }

  test("sampleMatchesByFile keeps distinct samples for duplicate pii-type rules on the same column") {
    val df = Seq(
      ("alpha.csv", "support@example.com"),
      ("alpha.csv", "sales@example.com")
    ).toDF("file_id", "customer_email")
    val rules = Seq(
      PiiRule("email", "support@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("email", "sales@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}")
    )

    val matchCounts = DetectionAggregator.aggregateByFile(df, "file_id", rules)
    val samples = DetectionAggregator.sampleMatchesByFile(df, "file_id", rules, matchCounts)

    assert(matchCounts.size == 2)
    assert(matchCounts.flatMap(matchCount => samples.get((matchCount.fileIdentifier, matchCount.metricAlias)).map(_.sampleMatchedFragment)).toSet ==
      Set("support@example.com", "sales@example.com"))
  }

  test("safe file sample fallback batches raw value collection") {
    val df = Seq(
      ("alpha.csv", "alpha@example.com", "010-1234-5678", "beta@example.com", "010-2222-3333"),
      ("beta.csv", "gamma@example.com", "010-9999-8888", "delta@example.com", "010-4444-5555"),
      ("gamma.csv", "noise", "noise", "noise", "noise")
    ).toDF("file_id", "email_1", "phone_1", "email_2", "phone_2")

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
    )

    val (rawValues, jobCount) = captureJobCount {
      invokeFileSafeRawCollector(df, "file_id", rules)
    }

    assert(jobCount == 2, s"expected two Spark jobs for batched file safe sample collection, found $jobCount")
    assert(rawValues == Map(
      ("alpha.csv", "m_0_0") -> "alpha@example.com",
      ("alpha.csv", "m_1_1") -> "010-1234-5678",
      ("alpha.csv", "m_2_0") -> "beta@example.com",
      ("alpha.csv", "m_3_1") -> "010-2222-3333",
      ("beta.csv", "m_0_0") -> "gamma@example.com",
      ("beta.csv", "m_1_1") -> "010-9999-8888",
      ("beta.csv", "m_2_0") -> "delta@example.com",
      ("beta.csv", "m_3_1") -> "010-4444-5555"
    ))
  }

  test("sampleMatches keeps driver license samples aligned with the matched regex fragment") {
    val rawValue = "이전 번호는 11-12-345678-90이고 현재 번호는 서울 07 - 111111 - 10 입니다"
    val df = Seq(rawValue).toDF("driver_license")
    val rules = Seq(
      PiiRule(
        "driver_license_number",
        "(?<![가-힣A-Za-z0-9])서울\\s*[0-9]{2}\\s*-\\s*[0-9]{6}\\s*-\\s*[0-9]{2}(?![가-힣A-Za-z0-9])"
      )
    )

    val matchCounts = DetectionAggregator.aggregate(df, rules)
    val samples = DetectionAggregator.sampleMatches(df, rules, matchCounts)

    assert(matchCounts.size == 1)
    assert(samples(matchCounts.head.metricAlias).sampleMatchedFragment == "서울 07 - 111111 - 10")
  }

  test("counts driver license numbers only when the regex-matched fragment is valid") {
    val rawValue = "이전 번호는 11-12-345678-90이고 현재 번호는 27-12-345678-90 입니다"
    val df = Seq(rawValue).toDF("driver_license")
    val rules = Seq(
      PiiRule("driver_license_number", "27-[0-9]{2}-[0-9]{6}-[0-9]{2}")
    )

    val actual = DetectionAggregator.aggregate(df, rules)

    assert(actual.isEmpty)
  }

  test("counts driver license numbers only when strict validator accepts the candidate") {
    val df = Seq(
      ("11-12-345678-90"),
      ("1212345678"),
      ("면허번호 11-12-345678-90"),
      ("구형 면허번호 서울 07 - 111111 - 10"),
      ("이전 번호 27-12-345678-90, 현재 번호 11-12-345678-90"),
      ("27-12-345678-90"),
      ("271234567890"),
      ("noise")
    ).toDF("driver_license")

    val rules = Seq(
      PiiRule(
        "driver_license_number",
        "(?:(?<![0-9])(?:[0-9]{2}-[0-9]{6}-[0-9]{2}|(?:1[1-9]|2[0-6]|28)-[0-9]{2}-[0-9]{6}-[0-9]{2}|(?:1[1-9]|2[0-6]|28)[0-9]{10})(?![0-9])|(?<![가-힣A-Za-z0-9])(?:서울|부산|경기|강원|충북|충남|전북|전남|경북|경남|제주|대구|인천|광주|대전|울산)\\s*(?:[0-9]{10}|[0-9]{2}\\s*-\\s*[0-9]{6}\\s*-\\s*[0-9]{2})(?![가-힣A-Za-z0-9]))"
      )
    )

    val actual = sortByKey(DetectionAggregator.aggregate(df, rules))
    val expected = Seq(MatchCount("driver_license", "driver_license_number", 5L))

    assert(actual == expected)
  }

  test("buildMetrics uses codegen-friendly SQL predicates for driver license detection") {
    val df = Seq(
      ("11-12-345678-90"),
      ("noise")
    ).toDF("driver_license")
    val rules = Seq(
      PiiRule(
        "driver_license_number",
        "(?:(?<![0-9])(?:[0-9]{2}-[0-9]{6}-[0-9]{2}|(?:1[1-9]|2[0-6]|28)-[0-9]{2}-[0-9]{6}-[0-9]{2}|(?:1[1-9]|2[0-6]|28)[0-9]{10})(?![0-9])|(?<![가-힣A-Za-z0-9])(?:서울|부산|경기|강원|충북|충남|전북|전남|경북|경남|제주|대구|인천|광주|대전|울산)\\s*(?:[0-9]{10}|[0-9]{2}\\s*-\\s*[0-9]{6}\\s*-\\s*[0-9]{2})(?![가-힣A-Za-z0-9]))"
      )
    )

    val metrics = invokeBuildMetrics(df.columns.toSeq, rules)
    val predicate = extractMetricPredicate(metrics.head)
    val expressionClassNames = df
      .filter(predicate)
      .queryExecution
      .analyzed
      .expressions
      .toSeq
      .flatMap(expression => treeNodeClassNames(expression.asInstanceOf[TreeNode[_]]))

    assert(!expressionClassNames.contains("ScalaUDF"), expressionClassNames.mkString(","))
  }

  test("batched driver license aggregation matches safe legacy fallback") {
    val df = Seq(
      ("11-12-345678-90", "서울 07 - 111111 - 10"),
      ("27-12-345678-90", "부산0711111110"),
      ("271234567890", "면허번호 서울 07 - 111111 - 10"),
      ("이전 번호 27-12-345678-90, 현재 번호 11-12-345678-90", "세종 07 - 111111 - 10"),
      ("noise", "noise")
    ).toDF("driver_license_partial", "driver_license_full")

    val rules = Seq(
      PiiRule(
        "driver_license_number",
        "(?:(?<![0-9])(?:[0-9]{2}-[0-9]{6}-[0-9]{2}|(?:1[1-9]|2[0-6]|28)-[0-9]{2}-[0-9]{6}-[0-9]{2}|(?:1[1-9]|2[0-6]|28)[0-9]{10})(?![0-9])|(?<![가-힣A-Za-z0-9])(?:서울|부산|경기|강원|충북|충남|전북|전남|경북|경남|제주|대구|인천|광주|대전|울산)\\s*(?:[0-9]{10}|[0-9]{2}\\s*-\\s*[0-9]{6}\\s*-\\s*[0-9]{2})(?![가-힣A-Za-z0-9]))"
      ),
      PiiRule(
        "driver_license_number",
        "(?:(?<![0-9])(?:[0-9]{2}-[0-9]{6}-[0-9]{2}|(?:1[1-9]|2[0-6]|28)-[0-9]{2}-[0-9]{6}-[0-9]{2}|(?:1[1-9]|2[0-6]|28)[0-9]{10})(?![0-9])|(?<![가-힣A-Za-z0-9])(?:서울|부산|경기|강원|충북|충남|전북|전남|경북|경남|제주|대구|인천|광주|대전|울산)\\s*(?:[0-9]{10}|[0-9]{2}\\s*-\\s*[0-9]{6}\\s*-\\s*[0-9]{2})(?![가-힣A-Za-z0-9]))",
        matchType = PiiRuleMatchType.FullColumn,
        columnHints = Seq("full")
      )
    )

    val batched = DetectionAggregator.aggregate(
      df,
      rules,
      AggregationConfig(maxExpressionsPerAgg = 8, legacyFallbackThreshold = 1000)
    )
    val fallback = DetectionAggregator.aggregate(
      df,
      rules,
      AggregationConfig(maxExpressionsPerAgg = 8, legacyFallbackThreshold = 1)
    )

    assert(sortByKey(batched) == sortByKey(fallback))
  }

  test("driver license aggregation accepts a later valid regex match after an earlier invalid one") {
    val df = Seq(
      "이전 번호 27-12-345678-90, 현재 번호 11-12-345678-90"
    ).toDF("driver_license")

    val rules = Seq(
      PiiRule("driver_license_number", "(?:27|11)-[0-9]{2}-[0-9]{6}-[0-9]{2}")
    )

    val actual = DetectionAggregator.aggregate(df, rules)
    val expected = Seq(MatchCount("driver_license", "driver_license_number", 1L))

    assert(sortByKey(actual) == sortByKey(expected))
  }

  test("counts Korean region-name driver license numbers for full-column rules") {
    val df = Seq(
      ("서울 07 - 111111 - 10"),
      ("부산0711111110"),
      ("1234567890"),
      ("면허번호 서울 07 - 111111 - 10"),
      ("세종 07 - 111111 - 10"),
      ("noise")
    ).toDF("driver_license")

    val rules = Seq(
      PiiRule(
        "driver_license_number",
        "(?:(?<![0-9])(?:[0-9]{2}-[0-9]{6}-[0-9]{2}|(?:1[1-9]|2[0-6]|28)-[0-9]{2}-[0-9]{6}-[0-9]{2}|(?:1[1-9]|2[0-6]|28)[0-9]{10})(?![0-9])|(?<![가-힣A-Za-z0-9])(?:서울|부산|경기|강원|충북|충남|전북|전남|경북|경남|제주|대구|인천|광주|대전|울산)\\s*(?:[0-9]{10}|[0-9]{2}\\s*-\\s*[0-9]{6}\\s*-\\s*[0-9]{2})(?![가-힣A-Za-z0-9]))",
        matchType = PiiRuleMatchType.FullColumn
      )
    )

    val actual = sortByKey(DetectionAggregator.aggregate(df, rules))
    val expected = Seq(MatchCount("driver_license", "driver_license_number", 2L))

    assert(actual == expected)
  }

  private def sortByKey(values: Seq[MatchCount]): Seq[MatchCount] = {
    values
      .map(value => value.copy(metricAlias = ""))
      .sortBy(v => (v.columnName, v.piiType, v.count))
  }

  private def sortByFileKey(values: Seq[FileMatchCount]): Seq[FileMatchCount] = {
    values
      .map(value => value.copy(metricAlias = ""))
      .sortBy(v => (v.fileIdentifier, v.columnName, v.piiType, v.count))
  }

  private def captureStderr[A](block: => A): String = {
    val output = new ByteArrayOutputStream()
    val originalErr = System.err
    val captureErr = new PrintStream(output, true, StandardCharsets.UTF_8.name())
    try {
      System.setErr(captureErr)
      block
    } finally {
      captureErr.flush()
      System.setErr(originalErr)
    }
    output.toString(StandardCharsets.UTF_8.name())
  }

  private def captureJobCount[A](block: => A): (A, Int) = {
    val jobCount = new AtomicInteger(0)
    val listener = new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        jobCount.incrementAndGet()
      }
    }

    spark.sparkContext.addSparkListener(listener)
    try {
      (block, jobCount.get())
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  private def withDebugLoggingEnabled[A](block: => A): A = {
    withDriverLogLevel("debug")(block)
  }

  private def withDriverLogLevel[A](level: String)(block: => A): A = {
    val previous = sys.props.get("privyspark.debug")
    DetectionAggregator.resetDebugCache()
    DriverLogger.resetCache()
    System.setProperty("privyspark.debug", level)
    try {
      block
    } finally {
      previous match {
        case Some(value) => System.setProperty("privyspark.debug", value)
        case None => System.clearProperty("privyspark.debug")
      }
      DetectionAggregator.resetDebugCache()
      DriverLogger.resetCache()
    }
  }

  private def invokeDatasetSafeRawCollector(df: DataFrame, rules: Seq[PiiRule]): Map[String, String] = {
    val metrics = invokeDetectionAggregatorPrivateMethod(
      "buildMetrics",
      df.columns.toSeq,
      rules
    )

    invokeDetectionAggregatorPrivateMethod(
      "collectSampleRawValuesSafely",
      df,
      metrics
    ).asInstanceOf[Map[String, String]]
  }

  private def invokeFileSafeRawCollector(
    df: DataFrame,
    fileIdentifierColumn: String,
    rules: Seq[PiiRule]
  ): Map[(String, String), String] = {
    val metrics = invokeDetectionAggregatorPrivateMethod(
      "buildMetrics",
      df.columns.toSeq.filterNot(_ == fileIdentifierColumn),
      rules
    )

    invokeDetectionAggregatorPrivateMethod(
      "collectSampleRawValuesByFileSafely",
      df,
      fileIdentifierColumn,
      metrics
    ).asInstanceOf[Map[(String, String), String]]
  }

  private def invokeDetectionAggregatorPrivateMethod(methodName: String, args: AnyRef*): AnyRef = {
    val method = DetectionAggregator.getClass.getDeclaredMethods
      .find(candidate => candidate.getName == methodName && candidate.getParameterCount == args.size)
      .getOrElse(fail(s"unable to find DetectionAggregator private method: $methodName/${args.size}"))
    method.setAccessible(true)
    method.invoke(DetectionAggregator, args: _*)
  }

  private def legacyCounts(df: DataFrame, rules: Seq[PiiRule]): Seq[MatchCount] = {
    df.columns.toSeq.flatMap { columnName =>
      val valueColumn = col(columnName).cast(StringType)
      rules.flatMap { rule =>
        if (shouldApplyRule(columnName, rule)) {
          val matchRegex = regexForRule(rule)
          val presentValuePredicate = valueColumn.isNotNull && trim(valueColumn) =!= ""
          val count = df.filter(presentValuePredicate && valueColumn.rlike(matchRegex)).count()
          if (count > 0L) Some(MatchCount(columnName, rule.piiType, count)) else None
        } else {
          None
        }
      }
    }
  }

  private def rowBasedExpected(df: DataFrame, rules: Seq[PiiRule]): Seq[MatchCount] = {
    val columns = df.columns.toSeq
    val compiledRules = rules.map(rule => rule -> regexForRule(rule).r)
    val rows = df.select(columns.map(name => col(name).cast(StringType).as(name)): _*).collect()

    columns.flatMap { columnName =>
      val columnIndex = columns.indexOf(columnName)
      compiledRules.flatMap {
        case (rule, regex) =>
          if (shouldApplyRule(columnName, rule)) {
            val presentValues = rows.flatMap { row =>
              if (row.isNullAt(columnIndex)) {
                None
              } else {
                val value = row.getString(columnIndex)
                if (rule.matchType == PiiRuleMatchType.FullColumn && value.trim.isEmpty) None else Some(value)
              }
            }
            val count = presentValues.count(value => regex.findFirstIn(value).nonEmpty)

            if (count > 0) Some(MatchCount(columnName, rule.piiType, count.toLong)) else None
          } else {
            None
          }
        }
    }
  }

  private def legacyCountsByFile(
    df: DataFrame,
    fileIdentifierColumn: String,
    rules: Seq[PiiRule]
  ): Seq[FileMatchCount] = {
    val dataColumns = df.columns.toSeq.filterNot(_ == fileIdentifierColumn)

    dataColumns.flatMap { columnName =>
      val valueColumn = col(columnName).cast(StringType)

      rules.flatMap { rule =>
        if (shouldApplyRule(columnName, rule)) {
          val matchRegex = regexForRule(rule)
          val presentValuePredicate = valueColumn.isNotNull && trim(valueColumn) =!= ""
          val groupedRows = df
            .filter(presentValuePredicate && valueColumn.rlike(matchRegex))
            .groupBy(col(fileIdentifierColumn))
            .count()
            .collect()

          groupedRows.flatMap { row =>
            val fileIdentifier = if (row.isNullAt(0)) null else row.getString(0)
            val count = if (row.isNullAt(1)) 0L else row.getLong(1)
            if (fileIdentifier == null || fileIdentifier.isEmpty || count <= 0L) {
              None
            } else {
              Some(FileMatchCount(fileIdentifier, columnName, rule.piiType, count))
            }
          }
        } else {
          None
        }
      }
    }
  }

  private def shouldApplyRule(columnName: String, rule: PiiRule): Boolean = {
    val normalizedColumnName = columnName.toLowerCase
    rule.columnHints.isEmpty || rule.columnHints.exists(hint => normalizedColumnName.contains(hint.toLowerCase))
  }

  private def regexForRule(rule: PiiRule): String = {
    rule.matchType match {
      case PiiRuleMatchType.FullColumn => s"\\A(?:${rule.regex})\\z"
      case _ => rule.regex
    }
  }

  private def invokeBuildMetrics(columns: Seq[String], rules: Seq[PiiRule]): Seq[AnyRef] = {
    val method = DetectionAggregator.getClass.getDeclaredMethods.find(_.getName == "buildMetrics").get
    method.setAccessible(true)
    method.invoke(DetectionAggregator, columns, rules).asInstanceOf[Seq[AnyRef]]
  }

  private def extractMetricPredicate(metric: AnyRef): org.apache.spark.sql.Column = {
    metric.getClass.getMethod("predicate").invoke(metric).asInstanceOf[org.apache.spark.sql.Column]
  }

  private def treeNodeClassNames(node: TreeNode[_]): Seq[String] = {
    node.getClass.getSimpleName +: node.children.toSeq.flatMap(child => treeNodeClassNames(child.asInstanceOf[TreeNode[_]]))
  }
}
