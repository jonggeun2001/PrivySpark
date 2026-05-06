package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.config.RulesetLoader
import io.github.jonggeun2001.privyspark.model.{PiiRule, ScanError, ScanResult}
import io.github.jonggeun2001.privyspark.scan.{DirectoryScanner, GroupScanCoordinator}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.Comparator
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class SampleDatasetGeneratorSpec extends AnyFunSuite with BeforeAndAfterAll {
  private val spark = SparkSession.builder()
    .appName("SampleDatasetGeneratorSpec")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("generate creates scannable sample datasets for supported and edge input cases") {
    val outputRoot = Files.createTempDirectory("privyspark-sample-datasets-")

    try {
      SampleDatasetGenerator.generate(outputRoot, spark)

      val rules = RulesetLoader.load(outputRoot.resolve("sample-rules.yaml").toString)
      val scenarios = loadManifest(outputRoot.resolve("scenario-manifest.tsv"))

      assert(scenarios.nonEmpty)
      assert(rules.size == 2)
      assert(Files.exists(outputRoot.resolve("files")))

      scenarios.foreach { scenario =>
        val scanPath = outputRoot.resolve(scenario.relativePath)
        val (results, errors) = scanWithRules(scanPath.toString, scanPath.toString, rules, "2026-04-13T00:00:00Z")

        withClue(s"case=${scenario.caseId} ") {
          assert(results.size == scenario.expectedResultRows, s"results=$results errors=$errors")
          assert(errors.size == scenario.expectedErrorRows, s"results=$results errors=$errors")

          if (scenario.expectedIdentifierFragment.nonEmpty) {
            val identifiers = results.map(_.file_identifier) ++ errors.map(_.file_identifier)
            assert(identifiers.exists(_.contains(scenario.expectedIdentifierFragment)))
          }

          if (scenario.expectedErrorFragment.nonEmpty) {
            assert(errors.exists(_.error_message.contains(scenario.expectedErrorFragment)))
          }
        }
      }
    } finally {
      deleteRecursively(outputRoot)
    }
  }

  test("checked-in sample bundle paths stay aligned with manifest") {
    val outputRoot = Paths.get("samples", "input-cases")
    val scenarios = loadManifest(outputRoot.resolve("scenario-manifest.tsv"))

    assert(Files.exists(outputRoot.resolve("sample-rules.yaml")))
    assert(Files.isDirectory(outputRoot.resolve("files")))
    assert(scenarios.nonEmpty)

    scenarios.foreach { scenario =>
      val samplePath = outputRoot.resolve(scenario.relativePath)
      assert(Files.exists(samplePath), s"case=${scenario.caseId} path=$samplePath")
    }
  }

  private def loadManifest(path: Path): Seq[SampleDatasetGenerator.Scenario] = {
    val lines = Files.readAllLines(path, StandardCharsets.UTF_8).asScala.toSeq
    lines.drop(1).filter(_.trim.nonEmpty).map { line =>
      val parts = line.split("\t", -1)
      SampleDatasetGenerator.Scenario(
        caseId = parts(0),
        relativePath = parts(1),
        expectedResultRows = parts(2).toInt,
        expectedErrorRows = parts(3).toInt,
        expectedIdentifierFragment = parts(4),
        expectedErrorFragment = parts(5)
      )
    }
  }

  private def scanWithRules(
    inputPath: String,
    datasetPath: String,
    rules: Seq[PiiRule],
    timestamp: String
  ): (Seq[ScanResult], Seq[ScanError]) = {
    val plan = DirectoryScanner.scanDirectoryStructure(
      spark,
      inputPath,
      datasetPath,
      timestamp
    )

    val results = ArrayBuffer.empty[ScanResult]
    val errors = ArrayBuffer.empty[ScanError] ++ plan.errors

    plan.groups.foreach { group =>
      val (groupResults, groupErrors) = GroupScanCoordinator.scanGroup(
        spark,
        datasetPath,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp
      )
      results ++= groupResults
      errors ++= groupErrors
    }

    (results.toSeq, errors.toSeq)
  }

  private def deleteRecursively(path: Path): Unit = {
    if (!Files.exists(path)) {
      return
    }

    val walk = Files.walk(path)
    try {
      walk.sorted(Comparator.reverseOrder()).forEach(p => Files.deleteIfExists(p))
    } finally {
      walk.close()
    }
  }
}
