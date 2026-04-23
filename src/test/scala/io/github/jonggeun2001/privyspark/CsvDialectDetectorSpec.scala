package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.format.ByteProbe.TextFormat
import io.github.jonggeun2001.privyspark.format.CsvDialectDetector
import io.github.jonggeun2001.privyspark.model.ScanReadOptions
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.file.Files
import java.nio.charset.StandardCharsets

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
      ":" -> Seq("name:email", "alice:alice@example.com"),
      "\u001c" -> Seq("name\u001cemail", "alice\u001calice@example.com"),
      "\u001d" -> Seq("name\u001demail", "alice\u001dalice@example.com"),
      "\u001e" -> Seq("name\u001eemail", "alice\u001ealice@example.com"),
      "\u001f" -> Seq("name\u001femail", "alice\u001falice@example.com")
    )

    cases.foreach {
      case (delimiter, lines) =>
        assert(CsvDialectDetector.detectDialectFromLines(spark, lines).map(_.delimiter).contains(delimiter))
    }
  }

  test("detectDialectFromLines prefers a consistent multi-character delimiter over partial single-character splits") {
    val cases = Seq(
      "|~|" -> Seq(
        "name|~|email",
        "alice|~|alice@example.com",
        "bob|~|bob@example.com"
      ),
      "||" -> Seq(
        "name||email",
        "alice||alice@example.com",
        "bob||bob@example.com"
      )
    )

    cases.foreach {
      case (delimiter, lines) =>
        val detected = CsvDialectDetector.detectDialectFromLines(spark, lines)
        assert(detected.map(_.delimiter).contains(delimiter), detected.toString)
    }
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

  test("refineDetectedFormat falls back when dialect sample read fails") {
    val missingPath = Files.createTempDirectory("privyspark-missing-dialect").resolve("missing.data").toUri.toString
    val readOptions = ScanReadOptions()
    val refined = CsvDialectDetector.refineDetectedFormat(spark, missingPath, TextFormat, readOptions)

    assert(refined == ((TextFormat, readOptions)))
  }

  test("refineDetectedFormat keeps single-line delimiter-like text as text") {
    val filePath = writeTextFile("name,email\n")
    val (format, readOptions) = CsvDialectDetector.refineDetectedFormat(spark, filePath, TextFormat, ScanReadOptions())

    assert(format == TextFormat)
    assert(readOptions == ScanReadOptions())
  }

  test("refineDetectedFormat keeps log-like natural punctuation text as text") {
    val filePath = writeTextFile(
      "INFO: service started\n" +
        "WARN: retry scheduled\n" +
        "ERROR: failed after retry\n"
    )
    val (format, readOptions) = CsvDialectDetector.refineDetectedFormat(spark, filePath, TextFormat, ScanReadOptions())

    assert(format == TextFormat)
    assert(readOptions == ScanReadOptions())
  }

  test("refineDetectedFormat promotes structured comma-delimited text to csv") {
    val filePath = writeTextFile(
      "name,email\n" +
        "alice,alice@example.com\n" +
        "bob,bob@example.com\n"
    )
    val (format, readOptions) = CsvDialectDetector.refineDetectedFormat(spark, filePath, TextFormat, ScanReadOptions())

    assert(format == "csv")
    assert(readOptions.csvDialect.forall(_.delimiter == ","))
  }

  private def writeTextFile(contents: String): String = {
    val path = Files.createTempFile("privyspark-csv-dialect-detector-", ".data")
    path.toFile.deleteOnExit()
    Files.write(path, contents.getBytes(StandardCharsets.UTF_8))
    path.toUri.toString
  }
}
