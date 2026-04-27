package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.model.{MatchCount, SampleValue}
import io.github.jonggeun2001.privyspark.scan.ScanResultBuilder
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ScanResultBuilderSpec extends AnyFunSuite {
  test("buildScanResults defaults hive_table_fqn to blank") {
    val results = ScanResultBuilder.buildScanResults(
      datasetPath = "/data",
      scanTimestamp = "2026-04-27T00:00:00Z",
      fileIdentifier = "customers.csv",
      sampledRowCount = 10L,
      nonEmptyValueCounts = Map("email" -> 10L),
      matchCounts = Seq(MatchCount("email", "email", 2L, "email")),
      sampleValues = Map("email" -> SampleValue("alice@example.com", "alice@example.com"))
    )

    assert(results.size == 1)
    assert(results.head.hive_table_fqn == "")
  }

  test("comparableResultPayloads ignores hive_table_fqn") {
    val baseResults = ScanResultBuilder.buildScanResults(
      datasetPath = "/data",
      scanTimestamp = "2026-04-27T00:00:00Z",
      fileIdentifier = "customers.csv",
      sampledRowCount = 10L,
      nonEmptyValueCounts = Map("email" -> 10L),
      matchCounts = Seq(MatchCount("email", "email", 2L, "email"))
    )
    val hiveResults = baseResults.map(_.copy(hive_table_fqn = "mart.customers"))

    assert(ScanResultBuilder.comparableResultPayloads(baseResults) == ScanResultBuilder.comparableResultPayloads(hiveResults))
  }
}
