package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.format.CsvDialectDetector
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CsvDialectDetectorSpec extends AnyFunSuite with BeforeAndAfterAll {
  private val spark = SparkSession.builder()
    .appName("CsvDialectDetectorSpec")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("detectDialectFromLines detects common single-character delimiters") {
    val cases = Seq(
      "," -> Seq("name,email", "alice,alice@example.com"),
      "\t" -> Seq("name\temail", "alice\talice@example.com"),
      ";" -> Seq("name;email", "alice;alice@example.com"),
      "|" -> Seq("name|email", "alice|alice@example.com"),
      ":" -> Seq("name:email", "alice:alice@example.com")
    )

    cases.foreach {
      case (delimiter, lines) =>
        assert(CsvDialectDetector.detectDialectFromLines(spark, lines).map(_.delimiter).contains(delimiter))
    }
  }

  test("detectDialectFromLines prefers a consistent multi-character delimiter over partial single-character splits") {
    val lines = Seq(
      "name|~|email",
      "alice|~|alice@example.com",
      "bob|~|bob@example.com"
    )

    val detected = CsvDialectDetector.detectDialectFromLines(spark, lines)
    assert(detected.map(_.delimiter).contains("|~|"), detected.toString)
  }

  test("detectDialectFromLines rejects single-column text") {
    val lines = Seq(
      "alice@example.com",
      "not an email",
      "bob@example.com"
    )

    assert(CsvDialectDetector.detectDialectFromLines(spark, lines).isEmpty)
  }

  test("detectDialectFromLines handles quoted delimiter characters") {
    val lines = Seq(
      "name|note|email",
      "alice|\"hello|there\"|alice@example.com",
      "bob|plain|bob@example.com"
    )

    assert(CsvDialectDetector.detectDialectFromLines(spark, lines).map(_.delimiter).contains("|"))
  }
}
