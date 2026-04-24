package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.format.CsvInference
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class SpecialCharacterPathSpec extends AnyFunSuite with BeforeAndAfterAll {
  private val spark = SparkSession.builder()
    .appName("SpecialCharacterPathSpec")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("schema detection reads paths with spaces and glob-special filename characters") {
    val inputDir = Files.createTempDirectory("privyspark-special-path-")
    val file = inputDir.resolve("customer list [final] #1.json")

    try {
      Files.write(
        file,
        ("{\"name\":\"Alice\",\"email\":\"alice@example.com\"}\n").getBytes(StandardCharsets.UTF_8)
      )

      assert(CsvInference.inferSchemaSignature(spark, "json", file.toString) == Right("email|name"))
      assert(CsvInference.readSource(spark, "json", Seq(file.toString)).count() == 1L)
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("schema detection reads paths with spaces and hash characters") {
    val inputDir = Files.createTempDirectory("privyspark-hash-path-")
    val file = inputDir.resolve("customer list #1.json")

    try {
      Files.write(
        file,
        ("{\"name\":\"Alice\",\"email\":\"alice@example.com\"}\n").getBytes(StandardCharsets.UTF_8)
      )

      assert(CsvInference.inferSchemaSignature(spark, "json", file.toString) == Right("email|name"))
      assert(CsvInference.readSource(spark, "json", Seq(file.toString)).count() == 1L)
    } finally {
      deleteRecursively(inputDir)
    }
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      val stream = Files.walk(path)
      try {
        stream
          .iterator()
          .asScala
          .toSeq
          .reverse
          .foreach(Files.deleteIfExists)
      } finally {
        stream.close()
      }
    }
  }
}
