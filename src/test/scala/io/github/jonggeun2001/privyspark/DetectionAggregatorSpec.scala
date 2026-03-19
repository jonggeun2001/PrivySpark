package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.DetectionAggregator.{AggregationConfig, FileMatchCount, MatchCount}
import io.github.jonggeun2001.privyspark.model.{PiiRule, PiiRuleMatchType}
import io.github.jonggeun2001.privyspark.validator.KoreanNameValidator
import org.apache.spark.sql.functions.{col, trim, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets

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

    assert(logs.contains("[PrivySpark][DEBUG] detection_aggregation_start scope=dataset"))
    assert(logs.contains("[PrivySpark][DEBUG] detection_aggregation_metrics_built scope=dataset"))
    assert(logs.contains("[PrivySpark][DEBUG] detection_aggregation_complete scope=dataset"))
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

  test("applies validator-backed name rules without reintroducing common noun false positives") {
    val df = Seq(
      "김철수",
      "박지민",
      "소유진",
      "남궁민수",
      "담당자 김민수",
      "김민수의 이메일",
      "김민수나",
      "김민수하고",
      "김민수하고요",
      "김민수예요",
      "김민수씨는",
      "김민수씨예요",
      "김민수죠",
      "김민수랑",
      "김민수인가요",
      "김민수라고 합니다",
      "김민수라고요",
      "김민수라서",
      "김민수지만",
      "박지민이나",
      "박지민이에요",
      "박지민이랑",
      "박지민이랑은",
      "박지민이랑요",
      "박지민님도",
      "박지민이지만",
      "박지민이네요",
      "남궁민수에게서",
      "남궁민수에게는",
      "남궁민수씨입니다",
      "정민이",
      "이준은",
      "남궁민은",
      "전화영",
      "김치찌개",
      "이사회",
      "관리자",
      "전화",
      "전화영업",
      "전화영업 중입니다",
      "유리",
      "유진은행",
      "유진의자",
      "가구",
      "인형",
      "김민수나라",
      "박지민가요",
      "김민수도로",
      "김민수가구",
      "이별",
      "이별은 아쉽다",
      "연락은 전화로 주세요",
      "유리문 앞입니다"
    ).toDF("candidate")

    val rules = Seq(
      PiiRule("name", KoreanNameValidator.RuleRegex, validator = Some(KoreanNameValidator.ValidatorName))
    )

    val actual = sortByKey(DetectionAggregator.aggregate(df, rules))
    val expected = Seq(MatchCount("candidate", "name", 34L))

    assert(actual == sortByKey(expected))
  }

  test("applies validator only to substrings matched by the rule regex") {
    val df = Seq(
      "김치찌개 / 박지민",
      "김철수",
      "김민수"
    ).toDF("candidate")

    val rules = Seq(
      PiiRule("name", "김[가-힣]{1,2}", validator = Some(KoreanNameValidator.ValidatorName))
    )

    val actual = sortByKey(DetectionAggregator.aggregate(df, rules))
    val expected = Seq(MatchCount("candidate", "name", 2L))

    assert(actual == sortByKey(expected))
  }

  test("supports validator-backed rules whose regex consumes surrounding boundaries") {
    val df = Seq(
      " 김민수 ",
      "김민수",
      " 김치찌개 ",
      "박지민"
    ).toDF("candidate")

    val rules = Seq(
      PiiRule("name", "\\s김[가-힣]{1,2}\\s", validator = Some(KoreanNameValidator.ValidatorName))
    )

    val actual = sortByKey(DetectionAggregator.aggregate(df, rules))
    val expected = Seq(MatchCount("candidate", "name", 1L))

    assert(actual == sortByKey(expected))
  }

  test("detects full-column rules only when every non-null value matches the regex") {
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
    val expected = sortByKey(legacyCounts(df, rules))

    assert(actual == expected)
    assert(actual == Seq(MatchCount("strict_email", "email", 2L)))
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

    assert(logs.contains("[PrivySpark][DEBUG] detection_aggregation_start scope=file"))
    assert(logs.contains("[PrivySpark][DEBUG] detection_aggregation_metrics_built scope=file"))
    assert(logs.contains("[PrivySpark][DEBUG] detection_aggregation_complete scope=file"))
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

  test("detects full-column rules per file only when each file column has no mismatches") {
    val df = Seq(
      ("alpha.csv", "prefix alpha@example.com"),
      ("alpha.csv", "beta@example.com suffix"),
      ("beta.csv", "beta@example.com"),
      ("beta.csv", ""),
      ("beta.csv", null.asInstanceOf[String])
    ).toDF("file_id", "customer_email")

    val rules = Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}", matchType = PiiRuleMatchType.FullColumn)
    )

    val actual = sortByFileKey(DetectionAggregator.aggregateByFile(df, "file_id", rules))
    val expected = sortByFileKey(legacyCountsByFile(df, "file_id", rules))

    assert(actual == expected)
    assert(actual == Seq(FileMatchCount("beta.csv", "customer_email", "email", 1L)))
  }

  private def sortByKey(values: Seq[MatchCount]): Seq[MatchCount] = {
    values.sortBy(v => (v.columnName, v.piiType, v.count))
  }

  private def sortByFileKey(values: Seq[FileMatchCount]): Seq[FileMatchCount] = {
    values.sortBy(v => (v.fileIdentifier, v.columnName, v.piiType, v.count))
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

  private def withDebugLoggingEnabled[A](block: => A): A = {
    val previous = sys.props.get("privyspark.debug")
    DetectionAggregator.resetDebugCache()
    System.setProperty("privyspark.debug", "true")
    try {
      block
    } finally {
      previous match {
        case Some(value) => System.setProperty("privyspark.debug", value)
        case None => System.clearProperty("privyspark.debug")
      }
      DetectionAggregator.resetDebugCache()
    }
  }

  private def legacyCounts(df: DataFrame, rules: Seq[PiiRule]): Seq[MatchCount] = {
    df.columns.toSeq.flatMap { columnName =>
      val valueColumn = col(columnName).cast(StringType)
      val presentValuePredicate = valueColumn.isNotNull && trim(valueColumn) =!= ""

      rules.flatMap { rule =>
        if (shouldApplyRule(columnName, rule)) {
          val predicate = rulePredicate(valueColumn, rule)
          val count = df.filter(predicate).count()
          rule.matchType match {
            case PiiRuleMatchType.FullColumn =>
              val mismatchCount = df.filter(presentValuePredicate && !predicate).count()
              if (count > 0L && mismatchCount == 0L) Some(MatchCount(columnName, rule.piiType, count)) else None
            case _ =>
              if (count > 0L) Some(MatchCount(columnName, rule.piiType, count)) else None
          }
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
            val count = presentValues.count(value => matchesRule(value, rule, regex))
            val mismatchCount = presentValues.size - count

            rule.matchType match {
              case PiiRuleMatchType.FullColumn =>
                if (count > 0 && mismatchCount == 0) Some(MatchCount(columnName, rule.piiType, count.toLong)) else None
              case _ =>
                if (count > 0) Some(MatchCount(columnName, rule.piiType, count.toLong)) else None
            }
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
      val presentValuePredicate = valueColumn.isNotNull && trim(valueColumn) =!= ""

      rules.flatMap { rule =>
        if (shouldApplyRule(columnName, rule)) {
          val predicate = rulePredicate(valueColumn, rule)
          rule.matchType match {
            case PiiRuleMatchType.FullColumn =>
              val groupedRows = df
                .groupBy(col(fileIdentifierColumn))
                .agg(
                  org.apache.spark.sql.functions.sum(when(predicate, 1L).otherwise(0L)).cast("long").as("match_count"),
                  org.apache.spark.sql.functions.sum(when(presentValuePredicate && !predicate, 1L).otherwise(0L)).cast("long").as("mismatch_count")
                )
                .collect()

              groupedRows.flatMap { row =>
                val fileIdentifier = if (row.isNullAt(0)) null else row.getString(0)
                val count = if (row.isNullAt(1)) 0L else row.getLong(1)
                val mismatchCount = if (row.isNullAt(2)) 0L else row.getLong(2)
                if (fileIdentifier == null || fileIdentifier.isEmpty || count <= 0L || mismatchCount > 0L) {
                  None
                } else {
                  Some(FileMatchCount(fileIdentifier, columnName, rule.piiType, count))
                }
              }
            case _ =>
              val groupedRows = df
                .filter(predicate)
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

  private def rulePredicate(valueColumn: org.apache.spark.sql.Column, rule: PiiRule): org.apache.spark.sql.Column = {
    val matchRegex = regexForRule(rule)
    val regexPredicate = valueColumn.isNotNull && valueColumn.rlike(matchRegex)
    val basePredicate = rule.validator match {
      case Some(KoreanNameValidator.ValidatorName) =>
        regexPredicate && KoreanNameValidator.predicate(spark, valueColumn, matchRegex)
      case Some(unsupported) =>
        throw new IllegalArgumentException(s"Unsupported validator: $unsupported")
      case None =>
        regexPredicate
    }

    rule.matchType match {
      case PiiRuleMatchType.FullColumn => valueColumn.isNotNull && trim(valueColumn) =!= "" && basePredicate
      case _ => basePredicate
    }
  }

  private def matchesRule(
    value: String,
    rule: PiiRule,
    regex: scala.util.matching.Regex
  ): Boolean = {
    regex.findFirstIn(value).nonEmpty && rule.validator.forall {
      case KoreanNameValidator.ValidatorName => KoreanNameValidator.containsLikelyName(value, regex.pattern.pattern())
      case unsupported => throw new IllegalArgumentException(s"Unsupported validator: $unsupported")
    }
  }
}
