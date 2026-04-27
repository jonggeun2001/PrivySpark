package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.hive.HiveTableLookupIndex
import io.github.jonggeun2001.privyspark.model.{PiiRule, ScanGroup}
import io.github.jonggeun2001.privyspark.scan.GroupScanner
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

@RunWith(classOf[JUnitRunner])
class HiveScanIntegrationSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("HiveScanIntegrationSpec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override protected def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("scanGroup populates hive_table_fqn from broadcast lookup") {
    val tempDir = Files.createTempDirectory("privyspark-hive-scan-")
    val tableDir = tempDir.resolve("warehouse").resolve("customers")
    val csvPath = tableDir.resolve("part-00000.csv")

    try {
      Files.createDirectories(tableDir)
      Files.write(csvPath, "email\nalice@example.com\n".getBytes(StandardCharsets.UTF_8))

      val tableLocation = tableDir.toRealPath().toUri.toString
      val hiveLookup = spark.sparkContext.broadcast(HiveTableLookupIndex(Vector(tableLocation -> "mart.customers")))
      try {
        val group = ScanGroup(
          directoryPath = tableDir.toString,
          format = "csv",
          schemaSignature = "email",
          filePaths = Seq(csvPath.toString),
          csvHasHeader = true,
          physicalPathsByKey = Map(csvPath.toString -> csvPath.toRealPath().toString),
          logicalIdentifiersByKey = Map(csvPath.toString -> "part-00000.csv")
        )

        val (results, errors) = GroupScanner.scanGroup(
          spark,
          datasetPath = tempDir.toString,
          group = group,
          rules = Seq(PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+")),
          sampleRatio = 1.0,
          timestamp = "2026-04-27T00:00:00Z",
          hiveLookup = Some(hiveLookup)
        )

        assert(errors.isEmpty)
        assert(results.nonEmpty)
        withClue(results.map(result => s"${result.file_identifier}:${result.hive_table_fqn}").mkString(",")) {
          assert(results.forall(_.hive_table_fqn == "mart.customers"))
        }
      } finally {
        hiveLookup.destroy()
      }
    } finally {
      deleteRecursively(tempDir)
    }
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      if (Files.isDirectory(path)) {
        val children = Files.list(path)
        try {
          val iterator = children.iterator()
          while (iterator.hasNext) {
            deleteRecursively(iterator.next())
          }
        } finally {
          children.close()
        }
      }
      Files.deleteIfExists(path)
    }
  }
}
