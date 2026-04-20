package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.cli.{CliConfig, ReviewApplyCliConfig}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.util.concurrent.atomic.AtomicReference

@RunWith(classOf[JUnitRunner])
class PrivySparkAppCliDispatchSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var testSpark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    testSpark = SparkSession.builder()
      .appName("PrivySparkAppCliDispatchSpec")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.allowMultipleContexts", "true")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (testSpark != null) {
      testSpark.stop()
    }
    super.afterAll()
  }

  test("runMain dispatches review apply without requiring scan path arguments") {
    val executedScan = new AtomicReference[Option[CliConfig]](None)
    val executedReview = new AtomicReference[Option[ReviewApplyCliConfig]](None)

    PrivySparkApp.runMain(
      Array(
        "review",
        "apply",
        "--scan-results",
        "/data/output/excel/scan_results.xlsx",
        "--input-root",
        "/data/input",
        "--allowlist",
        "/data/review/allowlist.jsonl",
        "--reviewer",
        "reviewer@example.com"
      ),
      createSparkSession = () => testSpark,
      exitWith = _ => (),
      runScanCommand = (_, config) => executedScan.set(Some(config)),
      runReviewApplyCommand = (_, config) => executedReview.set(Some(config))
    )

    assert(executedScan.get().isEmpty)
    assert(executedReview.get().nonEmpty)
    assert(executedReview.get().exists(_.reviewer == "reviewer@example.com"))
  }
}
