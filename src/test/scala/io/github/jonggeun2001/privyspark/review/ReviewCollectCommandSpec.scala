package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.cli.ReviewCollectCliConfig
import io.github.jonggeun2001.privyspark.model.ScanResult
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.charset.StandardCharsets
import java.nio.file.Files

@RunWith(classOf[JUnitRunner])
class ReviewCollectCommandSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .appName("ReviewCollectCommandSpec")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.allowMultipleContexts", "true")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  test("collect accepts response JSON and writes cumulative allowlist and action plan state") {
    val sparkSession = spark
    import sparkSession.implicits._

    val scanRoot = Files.createTempDirectory("privyspark-review-scan-")
    val stateRoot = Files.createTempDirectory("privyspark-review-state-")
    val scanResultsPath = scanRoot.resolve("parquet/scan_results")
    Files.createDirectories(scanResultsPath.getParent)
    Files.createDirectories(stateRoot.resolve("inbox"))

    val exactFingerprint = RecordedFileFingerprint(
      fileIdentifier = "customers/part-000.parquet",
      fileSize = 128L,
      fileMtimeEpochMs = 1710000000000L,
      fileChecksumAlgo = "CRC32",
      fileChecksum = "abcd1234"
    )
    val falsePositive = scanResult(
      columnName = "email",
      piiType = "email",
      sample = "alice@example.com",
      scopeFingerprints = ReviewScopeFingerprintCodec.encode(Seq(exactFingerprint))
    )
    val truePositive = scanResult(
      columnName = "phone",
      piiType = "phone_number",
      sample = "010-1234-5678",
      scopeFingerprints = ReviewScopeFingerprintCodec.encode(Seq(exactFingerprint))
    )
    val patternFalsePositive = scanResult(
      columnName = "temp_driver_no",
      piiType = "driver_license_number",
      sample = "D1234567",
      scopeFingerprints = ReviewScopeFingerprintCodec.encode(Seq(exactFingerprint))
    )

    Seq(falsePositive, truePositive, patternFalsePositive).toDS().toDF().write.mode("overwrite").parquet(scanResultsPath.toString)

    val findings = ReviewFindingBuilder.fromScanResults(Seq(falsePositive, truePositive, patternFalsePositive))
    val scanFingerprint = ReviewFindingBuilder.scanResultsFingerprint(findings)
    val emailFinding = findings.find(_.columnName == "email").get
    val phoneFinding = findings.find(_.columnName == "phone").get
    val patternFinding = findings.find(_.columnName == "temp_driver_no").get
    val responseJson =
      s"""{"schema_version":1,"scan_path":"/data/project","scan_results_fingerprint":"$scanFingerprint","responder":"owner@example.com","responded_at":"2026-04-27T10:00:00Z","responses":[{"finding_key":"${emailFinding.findingKey}","finding_hash":"${emailFinding.findingHash}","decision":"false_positive","false_positive_reason":"dummy email","allowlist_scope":"exact","file_identifier_pattern":null,"column_name_pattern":null,"pii_type_pattern":null,"expires_at":null,"action_plan":null,"action_due_date":null},{"finding_key":"${patternFinding.findingKey}","finding_hash":"${patternFinding.findingHash}","decision":"false_positive","false_positive_reason":"temporary generated identifier","allowlist_scope":"pattern","file_identifier_pattern":"customers/*","column_name_pattern":"temp_*","pii_type_pattern":"driver_license_number","expires_at":"2999-12-31","action_plan":null,"action_due_date":null},{"finding_key":"${phoneFinding.findingKey}","finding_hash":"${phoneFinding.findingHash}","decision":"true_positive","false_positive_reason":null,"allowlist_scope":null,"file_identifier_pattern":null,"column_name_pattern":null,"pii_type_pattern":null,"expires_at":null,"action_plan":"mask column","action_due_date":"2999-12-31"}]}"""
    Files.write(stateRoot.resolve("inbox/owner-response.json"), responseJson.getBytes(StandardCharsets.UTF_8))

    ReviewCollectCommand.run(
      spark,
      ReviewCollectCliConfig(scanResultsPath.toString, stateRoot.toString)
    )

    val allowlist = read(stateRoot.resolve("current/allowlist.jsonl"))
    val actionPlan = read(stateRoot.resolve("current/action_plan.jsonl"))
    val findingStatus = read(stateRoot.resolve("current/finding_status.jsonl"))
    val ledger = read(stateRoot.resolve("current/response_ledger.jsonl"))
    val rejected = stateRoot.resolve("rejected/rejected_responses.jsonl")

    assert(allowlist.contains("customers/part-000.parquet"))
    assert(allowlist.contains("abcd1234"))
    assert(allowlist.contains("dummy email"))
    assert(allowlist.contains("file_identifier_pattern"))
    assert(allowlist.contains("customers/*"))
    assert(allowlist.contains("temp_*"))
    assert(actionPlan.contains("phone_number"))
    assert(actionPlan.contains("mask column"))
    assert(actionPlan.contains("hive_database"))
    assert(actionPlan.contains("mart"))
    assert(findingStatus.contains("remediation_planned"))
    assert(findingStatus.contains("false_positive"))
    assert(ledger.contains(emailFinding.findingKey))
    assert(!Files.exists(rejected) || read(rejected).trim.isEmpty)

    val updatedEmailResponseJson =
      s"""{"schema_version":1,"scan_path":"/data/project","scan_results_fingerprint":"$scanFingerprint","responder":"owner@example.com","responded_at":"2026-04-28T10:00:00Z","responses":[{"finding_key":"${emailFinding.findingKey}","finding_hash":"${emailFinding.findingHash}","decision":"true_positive","false_positive_reason":null,"allowlist_scope":null,"file_identifier_pattern":null,"column_name_pattern":null,"pii_type_pattern":null,"expires_at":null,"action_plan":"mask email","action_due_date":"2999-12-31"}]}"""
    Files.write(stateRoot.resolve("inbox/email-true-positive.json"), updatedEmailResponseJson.getBytes(StandardCharsets.UTF_8))

    ReviewCollectCommand.run(
      spark,
      ReviewCollectCliConfig(scanResultsPath.toString, stateRoot.toString)
    )

    val updatedAllowlist = read(stateRoot.resolve("current/allowlist.jsonl"))
    val updatedActionPlan = read(stateRoot.resolve("current/action_plan.jsonl"))

    assert(!updatedAllowlist.contains("dummy email"))
    assert(!updatedAllowlist.contains("\"pii_type\":\"email\""))
    assert(updatedActionPlan.contains("mask email"))
  }

  private def scanResult(
    columnName: String,
    piiType: String,
    sample: String,
    scopeFingerprints: String
  ): ScanResult =
    ScanResult(
      dataset_path = "/data/project",
      scan_timestamp = "2026-04-27T10:00:00Z",
      file_identifier = "customers/part-000.parquet",
      column_name = columnName,
      pii_type = piiType,
      match_count = 1L,
      sampled_row_count = 10L,
      match_ratio = 0.1,
      non_empty_match_ratio = 0.1,
      confidence = 0.01,
      sample_raw_value = sample,
      sample_matched_fragment = sample,
      file_size = 128L,
      file_mtime_epoch_ms = 1710000000000L,
      hive_table_fqn = "mart.customers",
      review_scope_file_fingerprints = scopeFingerprints
    )

  private def read(path: java.nio.file.Path): String =
    new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
}
