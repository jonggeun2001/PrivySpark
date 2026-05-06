package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.cli.{CliConfig, ReviewApplyCliConfig, ReviewCollectCliConfig}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable.ArrayBuffer

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
    val executedCollect = new AtomicReference[Option[ReviewCollectCliConfig]](None)

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
      runReviewApplyCommand = (_, config) => executedReview.set(Some(config)),
      runReviewCollectCommand = (_, config) => executedCollect.set(Some(config))
    )

    assert(executedScan.get().isEmpty)
    assert(executedReview.get().nonEmpty)
    assert(executedCollect.get().isEmpty)
    assert(executedReview.get().exists(_.reviewer == "reviewer@example.com"))
  }

  test("runMain dispatches review collect without requiring scan path arguments") {
    val executedScan = new AtomicReference[Option[CliConfig]](None)
    val executedReview = new AtomicReference[Option[ReviewApplyCliConfig]](None)
    val executedCollect = new AtomicReference[Option[ReviewCollectCliConfig]](None)

    PrivySparkApp.runMain(
      Array(
        "review",
        "collect",
        "--review-state-root",
        "/data/review-state"
      ),
      createSparkSession = () => testSpark,
      exitWith = _ => (),
      runScanCommand = (_, config) => executedScan.set(Some(config)),
      runReviewApplyCommand = (_, config) => executedReview.set(Some(config)),
      runReviewCollectCommand = (_, config) => executedCollect.set(Some(config))
    )

    assert(executedScan.get().isEmpty)
    assert(executedReview.get().isEmpty)
    assert(executedCollect.get().exists(_.reviewStateRoot == "/data/review-state"))
  }

  test("runMain collects review state before scan when review-state-root is configured") {
    val calls = ArrayBuffer.empty[String]
    val executedScan = new AtomicReference[Option[CliConfig]](None)
    val executedCollect = new AtomicReference[Option[ReviewCollectCliConfig]](None)

    PrivySparkApp.runMain(
      Array(
        "--path",
        "/data/input",
        "--output",
        "/data/output",
        "--review-state-root",
        "/data/review-state"
      ),
      createSparkSession = () => testSpark,
      exitWith = _ => (),
      runScanCommand = (_, config) => {
        calls += "scan"
        executedScan.set(Some(config))
      },
      runReviewCollectCommand = (_, config) => {
        calls += "collect"
        executedCollect.set(Some(config))
      }
    )

    assert(calls.toSeq == Seq("collect", "scan"))
    assert(executedCollect.get().exists(_.reviewStateRoot == "/data/review-state"))
    assert(executedScan.get().exists(_.reviewStateRoot.contains("/data/review-state")))
  }

  test("runMain fails scan before execution when automatic review collect fails") {
    val exitCode = new AtomicReference[Option[Int]](None)
    val executedScan = new AtomicReference[Option[CliConfig]](None)

    PrivySparkApp.runMain(
      Array(
        "--path",
        "/data/input",
        "--output",
        "/data/output",
        "--review-state-root",
        "/data/review-state"
      ),
      createSparkSession = () => testSpark,
      exitWith = code => exitCode.set(Some(code)),
      runScanCommand = (_, config) => executedScan.set(Some(config)),
      runReviewCollectCommand = (_, _) => throw new IllegalStateException("Rejected review responses")
    )

    assert(exitCode.get().contains(1))
    assert(executedScan.get().isEmpty)
  }

  test("runMain rejects relative review html directory before creating Spark session") {
    val exitCode = new AtomicReference[Option[Int]](None)
    val executedScan = new AtomicReference[Option[CliConfig]](None)

    PrivySparkApp.runMain(
      Array(
        "--path",
        "/data/input",
        "--output",
        "/data/output",
        "--review-state-root",
        "/data/review-state",
        "--review-html-dir",
        "review"
      ),
      createSparkSession = () => fail("Spark session should not be created for invalid path arguments"),
      exitWith = code => exitCode.set(Some(code)),
      runScanCommand = (_, config) => executedScan.set(Some(config))
    )

    assert(exitCode.get().contains(2))
    assert(executedScan.get().isEmpty)
  }
}
