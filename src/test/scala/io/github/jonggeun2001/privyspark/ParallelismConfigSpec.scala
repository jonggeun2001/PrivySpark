package io.github.jonggeun2001.privyspark

import org.apache.spark.SparkConf
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ParallelismConfigSpec extends AnyFunSuite {
  test("applyCliParallelismOverrides applies configured CLI parallelism to Spark conf") {
    val conf = new SparkConf(loadDefaults = false)
    val config = CliConfig(
      inputPath = "/data/input",
      outputPath = "/data/output",
      groupParallelism = Some(7),
      fileParallelism = Some(5)
    )

    PrivySparkApp.applyCliParallelismOverrides(conf, config)

    assert(conf.getInt("spark.privyspark.groupParallelism", -1) == 7)
    assert(conf.getInt("spark.privyspark.fileParallelism", -1) == 5)
  }

  test("applyCliParallelismOverrides keeps existing Spark conf when CLI values are absent") {
    val conf = new SparkConf(loadDefaults = false)
      .set("spark.privyspark.groupParallelism", "2")
      .set("spark.privyspark.fileParallelism", "4")
    val config = CliConfig(
      inputPath = "/data/input",
      outputPath = "/data/output"
    )

    PrivySparkApp.applyCliParallelismOverrides(conf, config)

    assert(conf.getInt("spark.privyspark.groupParallelism", -1) == 2)
    assert(conf.getInt("spark.privyspark.fileParallelism", -1) == 4)
  }
}
