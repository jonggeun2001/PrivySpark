package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.DetectionAggregator.{AggregationConfig, FileMatchCount, MatchCount}
import io.github.jonggeun2001.privyspark.model.PiiRule
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

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

  private def sortByKey(values: Seq[MatchCount]): Seq[MatchCount] = {
    values.sortBy(v => (v.columnName, v.piiType, v.count))
  }

  private def sortByFileKey(values: Seq[FileMatchCount]): Seq[FileMatchCount] = {
    values.sortBy(v => (v.fileIdentifier, v.columnName, v.piiType, v.count))
  }

  private def legacyCounts(df: DataFrame, rules: Seq[PiiRule]): Seq[MatchCount] = {
    df.columns.toSeq.flatMap { columnName =>
      val valueColumn = col(columnName).cast(StringType)
      rules.flatMap { rule =>
        val count = df.filter(valueColumn.isNotNull && valueColumn.rlike(rule.regex)).count()
        if (count > 0L) Some(MatchCount(columnName, rule.piiType, count)) else None
      }
    }
  }

  private def rowBasedExpected(df: DataFrame, rules: Seq[PiiRule]): Seq[MatchCount] = {
    val columns = df.columns.toSeq
    val compiledRules = rules.map(rule => rule -> rule.regex.r)
    val rows = df.select(columns.map(name => col(name).cast(StringType).as(name)): _*).collect()

    columns.flatMap { columnName =>
      val columnIndex = columns.indexOf(columnName)
      compiledRules.flatMap {
        case (rule, regex) =>
          val count = rows.count { row =>
            if (row.isNullAt(columnIndex)) {
              false
            } else {
              regex.findFirstIn(row.getString(columnIndex)).nonEmpty
            }
          }

          if (count > 0) Some(MatchCount(columnName, rule.piiType, count.toLong)) else None
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
        val groupedRows = df
          .filter(valueColumn.isNotNull && valueColumn.rlike(rule.regex))
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
    }
  }
}
