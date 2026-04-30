package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.PrivySparkSpecFixtures
import io.github.jonggeun2001.privyspark.model.PiiRule
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.file.Files

@RunWith(classOf[JUnitRunner])
class GroupScannerFacadeSpec extends AnyFunSuite with PrivySparkSpecFixtures {
  test("selectSampledFileKeys keeps at least one deterministic file in original order") {
    val fileKeys = Seq("a.csv", "b.csv", "c.csv", "d.csv")

    val sampledKeys = GroupScanner.selectSampledFileKeys(fileKeys, 0.01, "fixture")

    assert(sampledKeys.size == 1)
    assert(fileKeys.contains(sampledKeys.head))
  }

  test("selectSampledFileKeys uses ceiling for sampled file count") {
    val sampledKeys = GroupScanner.selectSampledFileKeys(Seq("a.csv", "b.csv", "c.csv", "d.csv"), 0.51)

    assert(sampledKeys.size == 3)
  }

  test("selectSampledFileKeys is stable across repeated calls and input ordering") {
    val fileKeys = Seq("users-a.csv", "users-b.csv", "users-c.csv", "users-d.csv", "users-e.csv")

    val sampledRuns = (1 to 20).map(_ => GroupScanner.selectSampledFileKeys(fileKeys, 0.4, "reviews"))
    val reversedInputSample = GroupScanner.selectSampledFileKeys(fileKeys.reverse, 0.4, "reviews").toSet

    assert(sampledRuns.distinct.size == 1)
    assert(sampledRuns.head.toSet == reversedInputSample)
    assert(sampledRuns.head == fileKeys.filter(reversedInputSample.contains))
  }

  test("scanGroup facade matches GroupScanCoordinator scanGroup results") {
    val inputDir = Files.createTempDirectory("privyspark-groupscanner-facade-")

    try {
      writeText(inputDir.resolve("customers.csv"),
        "name,email,phone\n" +
          "alice,alice@example.com,010-1234-5678\n" +
          "bob,bob@example.com,not-phone\n")

      val timestamp = "2024-01-01T00:00:00Z"
      val group = DirectoryScanner
        .scanDirectoryStructure(spark, inputDir.toString, inputDir.toString, timestamp)
        .groups
        .head
      val rules = detectionRules

      val facadeResult = GroupScanner.scanGroup(
        spark,
        inputDir.toString,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp,
        csvHeadCache = new CsvHeadCache()
      )
      val coordinatorResult = GroupScanCoordinator.scanGroup(
        spark,
        inputDir.toString,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp,
        csvHeadCache = new CsvHeadCache()
      )

      assert(normalizeResults(facadeResult._1) == normalizeResults(coordinatorResult._1))
      assert(normalizeErrors(facadeResult._2) == normalizeErrors(coordinatorResult._2))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroups facade matches GroupScanCoordinator scanGroups outcomes") {
    val inputDir = Files.createTempDirectory("privyspark-groupscanner-facade-all-")

    try {
      writeText(inputDir.resolve("customers.csv"),
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(inputDir.resolve("members.csv"),
        "name,email\n" +
          "bob,bob@example.com\n")

      val timestamp = "2024-01-01T00:00:00Z"
      val groups = DirectoryScanner
        .scanDirectoryStructure(spark, inputDir.toString, inputDir.toString, timestamp)
        .groups
      val rules = detectionRules

      val facadeOutcomes = GroupScanner.scanGroups(
        spark,
        inputDir.toString,
        groups,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp,
        retainPayloads = true,
        csvHeadCache = new CsvHeadCache()
      )
      val coordinatorOutcomes = GroupScanCoordinator.scanGroups(
        spark,
        inputDir.toString,
        groups,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp,
        retainPayloads = true,
        csvHeadCache = new CsvHeadCache()
      )

      assert(normalizeOutcomeResults(facadeOutcomes) == normalizeOutcomeResults(coordinatorOutcomes))
      assert(normalizeOutcomeErrors(facadeOutcomes) == normalizeOutcomeErrors(coordinatorOutcomes))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanGroupBatch facade matches GroupScanCoordinator scanGroupBatch results") {
    val inputDir = Files.createTempDirectory("privyspark-groupscanner-batch-facade-")

    try {
      writeText(inputDir.resolve("customers-a.csv"),
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(inputDir.resolve("customers-b.csv"),
        "name,email\n" +
          "bob,bob@example.com\n")

      val timestamp = "2024-01-01T00:00:00Z"
      val group = DirectoryScanner
        .scanDirectoryStructure(spark, inputDir.toString, inputDir.toString, timestamp)
        .groups
        .head
      val rules = detectionRules

      val facadeResults = GroupScanner.scanGroupBatch(
        spark,
        inputDir.toString,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp
      )
      val coordinatorResults = GroupScanCoordinator.scanGroupBatch(
        spark,
        inputDir.toString,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp
      )

      assert(normalizeResults(facadeResults) == normalizeResults(coordinatorResults))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  private def detectionRules: Seq[PiiRule] =
    Seq(
      PiiRule("email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),
      PiiRule("phone", "\\b\\d{2,3}-\\d{3,4}-\\d{4}\\b")
    )
}
