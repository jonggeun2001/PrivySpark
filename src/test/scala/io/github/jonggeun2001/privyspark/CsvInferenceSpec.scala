package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.format.CsvInference
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.file.Files

@RunWith(classOf[JUnitRunner])
class CsvInferenceSpec extends AnyFunSuite with PrivySparkSpecFixtures {
  test("inferSchemaSignature returns fixed text schema without reading the file") {
    val inputDir = Files.createTempDirectory("privyspark-text-schema-short-circuit-")

    try {
      val missingFile = inputDir.resolve("missing.log")

      assert(CsvInference.inferSchemaSignature(spark, "text", missingFile.toString) == Right("value"))
    } finally {
      deleteRecursively(inputDir)
    }
  }
}
