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
      s"""{"schema_version":1,"scan_path":"/data/project","scan_results_fingerprint":"$scanFingerprint","responder":"owner@example.com","responded_at":"2026-04-27T10:00:00.001Z","responses":[{"finding_key":"${emailFinding.findingKey}","finding_hash":"${emailFinding.findingHash}","decision":"true_positive","false_positive_reason":null,"allowlist_scope":null,"file_identifier_pattern":null,"column_name_pattern":null,"pii_type_pattern":null,"expires_at":null,"action_plan":"mask email","action_due_date":"2999-12-31"}]}"""
    Files.write(stateRoot.resolve("inbox/email-true-positive.json"), updatedEmailResponseJson.getBytes(StandardCharsets.UTF_8))
    Files.move(stateRoot.resolve("current/allowlist.jsonl"), stateRoot.resolve("current/allowlist.jsonl.bak"))
    Files.move(stateRoot.resolve("current/action_plan.jsonl"), stateRoot.resolve("current/action_plan.jsonl.bak"))

    ReviewCollectCommand.run(
      spark,
      ReviewCollectCliConfig(scanResultsPath.toString, stateRoot.toString)
    )

    val updatedAllowlist = read(stateRoot.resolve("current/allowlist.jsonl"))
    val updatedActionPlan = read(stateRoot.resolve("current/action_plan.jsonl"))

    assert(!updatedAllowlist.contains("dummy email"))
    assert(!updatedAllowlist.contains("\"pii_type\":\"email\""))
    assert(updatedActionPlan.contains("mask column"))
    assert(updatedActionPlan.contains("mask email"))
  }

  test("collect accepts compact response JSON without unused null fields") {
    val sparkSession = spark
    import sparkSession.implicits._

    val scanRoot = Files.createTempDirectory("privyspark-review-scan-compact-")
    val stateRoot = Files.createTempDirectory("privyspark-review-state-compact-")
    val scanResultsPath = scanRoot.resolve("parquet/scan_results")
    Files.createDirectories(scanResultsPath.getParent)
    Files.createDirectories(stateRoot.resolve("inbox"))

    val fingerprint = RecordedFileFingerprint(
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
      scopeFingerprints = ReviewScopeFingerprintCodec.encode(Seq(fingerprint))
    )
    val truePositive = scanResult(
      columnName = "phone",
      piiType = "phone_number",
      sample = "010-1234-5678",
      scopeFingerprints = ReviewScopeFingerprintCodec.encode(Seq(fingerprint))
    )
    Seq(falsePositive, truePositive).toDS().toDF().write.mode("overwrite").parquet(scanResultsPath.toString)

    val findings = ReviewFindingBuilder.fromScanResults(Seq(falsePositive, truePositive))
    val scanFingerprint = ReviewFindingBuilder.scanResultsFingerprint(findings)
    val emailFinding = findings.find(_.columnName == "email").get
    val phoneFinding = findings.find(_.columnName == "phone").get
    val responseJson =
      s"""{"schema_version":1,"scan_path":"/data/project","scan_results_fingerprint":"$scanFingerprint","responder":"owner@example.com","responded_at":"2026-04-27T10:00:00Z","responses":[{"finding_key":"${emailFinding.findingKey}","finding_hash":"${emailFinding.findingHash}","decision":"false_positive","false_positive_reason":"dummy email","allowlist_scope":"exact"},{"finding_key":"${phoneFinding.findingKey}","finding_hash":"${phoneFinding.findingHash}","decision":"true_positive","action_plan":"mask column","action_due_date":"2999-12-31"}]}"""
    Files.write(stateRoot.resolve("inbox/owner-response.json"), responseJson.getBytes(StandardCharsets.UTF_8))

    ReviewCollectCommand.run(
      spark,
      ReviewCollectCliConfig(scanResultsPath.toString, stateRoot.toString)
    )

    val allowlist = read(stateRoot.resolve("current/allowlist.jsonl"))
    val actionPlan = read(stateRoot.resolve("current/action_plan.jsonl"))
    val rejected = stateRoot.resolve("rejected/rejected_responses.jsonl")

    assert(allowlist.contains("dummy email"))
    assert(actionPlan.contains("mask column"))
    assert(!Files.exists(rejected) || read(rejected).trim.isEmpty)
  }

  test("collect keys unified review state by scan path and file identifier") {
    val sparkSession = spark
    import sparkSession.implicits._

    val scanRoot = Files.createTempDirectory("privyspark-review-scan-unified-")
    val stateRoot = Files.createTempDirectory("privyspark-review-state-unified-")
    val scanResultsPath = scanRoot.resolve("parquet/scan_results")
    Files.createDirectories(scanResultsPath.getParent)
    Files.createDirectories(stateRoot.resolve("inbox"))

    val firstFingerprint = RecordedFileFingerprint(
      fileIdentifier = "customers/a.parquet",
      fileSize = 128L,
      fileMtimeEpochMs = 1710000000000L,
      fileChecksumAlgo = "CRC32",
      fileChecksum = "aaaa1111"
    )
    val secondFingerprint = RecordedFileFingerprint(
      fileIdentifier = "customers/b.parquet",
      fileSize = 256L,
      fileMtimeEpochMs = 1710000000001L,
      fileChecksumAlgo = "CRC32",
      fileChecksum = "bbbb2222"
    )
    val firstFindingResult = scanResult(
      columnName = "email",
      piiType = "email",
      sample = "alice@example.com",
      scopeFingerprints = ReviewScopeFingerprintCodec.encode(Seq(firstFingerprint)),
      fileIdentifier = "customers/a.parquet"
    )
    val secondFindingResult = scanResult(
      columnName = "email",
      piiType = "email",
      sample = "bob@example.com",
      scopeFingerprints = ReviewScopeFingerprintCodec.encode(Seq(secondFingerprint)),
      fileIdentifier = "customers/b.parquet"
    )

    Seq(firstFindingResult, secondFindingResult).toDS().toDF().write.mode("overwrite").parquet(scanResultsPath.toString)

    val findings = ReviewFindingBuilder.fromScanResults(Seq(firstFindingResult, secondFindingResult))
    assert(findings.size == 2)
    assert(findings.map(_.findingKey).distinct.size == 2)

    val scanFingerprint = ReviewFindingBuilder.scanResultsFingerprint(findings)
    val firstFinding = findings.find(_.evidence.exists(_.fileIdentifier == "customers/a.parquet")).get
    val secondFinding = findings.find(_.evidence.exists(_.fileIdentifier == "customers/b.parquet")).get
    val responseJson =
      s"""{"schema_version":1,"scan_path":"/data/project","scan_results_fingerprint":"$scanFingerprint","responder":"owner@example.com","responded_at":"2026-04-27T11:00:00Z","responses":[{"finding_key":"${firstFinding.findingKey}","finding_hash":"${firstFinding.findingHash}","decision":"false_positive","false_positive_reason":"dummy email in owner file","allowlist_scope":"exact","file_identifier_pattern":null,"column_name_pattern":null,"pii_type_pattern":null,"expires_at":null,"action_plan":null,"action_due_date":null},{"finding_key":"${secondFinding.findingKey}","finding_hash":"${secondFinding.findingHash}","decision":"true_positive","false_positive_reason":null,"allowlist_scope":null,"file_identifier_pattern":null,"column_name_pattern":null,"pii_type_pattern":null,"expires_at":null,"action_plan":"mask b email","action_due_date":"2999-12-31"}]}"""
    Files.write(stateRoot.resolve("inbox/owner-response.json"), responseJson.getBytes(StandardCharsets.UTF_8))

    ReviewCollectCommand.run(
      spark,
      ReviewCollectCliConfig(scanResultsPath.toString, stateRoot.toString)
    )

    val allowlist = read(stateRoot.resolve("current/allowlist.jsonl"))
    val actionPlan = read(stateRoot.resolve("current/action_plan.jsonl"))
    val findingStatus = read(stateRoot.resolve("current/finding_status.jsonl"))
    val ledger = read(stateRoot.resolve("current/response_ledger.jsonl"))

    assert(allowlist.contains("customers/a.parquet"))
    assert(!allowlist.contains("customers/b.parquet"))
    assert(actionPlan.contains("customers/b.parquet"))
    assert(findingStatus.contains("customers/a.parquet"))
    assert(findingStatus.contains("customers/b.parquet"))
    assert(ledger.contains("customers/a.parquet"))
    assert(ledger.contains("customers/b.parquet"))
    assert(!Files.exists(stateRoot.resolve("rejected")))
    assert(!Files.exists(stateRoot.resolve("versions")))
  }

  test("collect removes legacy pattern allowlist entries that cover a newly reviewed finding") {
    val sparkSession = spark
    import sparkSession.implicits._

    val scanRoot = Files.createTempDirectory("privyspark-review-scan-pattern-")
    val stateRoot = Files.createTempDirectory("privyspark-review-state-pattern-")
    val scanResultsPath = scanRoot.resolve("parquet/scan_results")
    Files.createDirectories(scanResultsPath.getParent)
    Files.createDirectories(stateRoot.resolve("inbox"))
    Files.createDirectories(stateRoot.resolve("current"))

    val fingerprint = RecordedFileFingerprint(
      fileIdentifier = "customers/a.parquet",
      fileSize = 128L,
      fileMtimeEpochMs = 1710000000000L,
      fileChecksumAlgo = "CRC32",
      fileChecksum = "aaaa1111"
    )
    val findingResult = scanResult(
      columnName = "email",
      piiType = "email",
      sample = "alice@example.com",
      scopeFingerprints = ReviewScopeFingerprintCodec.encode(Seq(fingerprint)),
      fileIdentifier = "customers/a.parquet"
    )
    Seq(findingResult).toDS().toDF().write.mode("overwrite").parquet(scanResultsPath.toString)

    val legacyPattern =
      """{"entry_type":"pattern","dataset_path":"/data/project","file_identifier_pattern":"customers/*","column_name_pattern":"email","pii_type_pattern":"email","reason":"legacy broad false positive","reviewer":"owner@example.com","reviewed_at":"2026-04-20T00:00:00Z","expires_at":"2999-12-31","source_finding_key":"legacy-hive-table-key"}"""
    Files.write(stateRoot.resolve("current/allowlist.jsonl"), s"$legacyPattern\n".getBytes(StandardCharsets.UTF_8))

    val findings = ReviewFindingBuilder.fromScanResults(Seq(findingResult))
    val scanFingerprint = ReviewFindingBuilder.scanResultsFingerprint(findings)
    val finding = findings.head
    val responseJson =
      s"""{"schema_version":1,"scan_path":"/data/project","scan_results_fingerprint":"$scanFingerprint","responder":"owner@example.com","responded_at":"2026-04-27T12:00:00Z","responses":[{"finding_key":"${finding.findingKey}","finding_hash":"${finding.findingHash}","decision":"true_positive","false_positive_reason":null,"allowlist_scope":null,"file_identifier_pattern":null,"column_name_pattern":null,"pii_type_pattern":null,"expires_at":null,"action_plan":"mask email","action_due_date":"2999-12-31"}]}"""
    Files.write(stateRoot.resolve("inbox/owner-response.json"), responseJson.getBytes(StandardCharsets.UTF_8))

    ReviewCollectCommand.run(
      spark,
      ReviewCollectCliConfig(scanResultsPath.toString, stateRoot.toString)
    )

    val allowlist = read(stateRoot.resolve("current/allowlist.jsonl"))
    val actionPlan = read(stateRoot.resolve("current/action_plan.jsonl"))

    assert(!allowlist.contains("legacy broad false positive"))
    assert(!allowlist.contains("customers/*"))
    assert(actionPlan.contains("customers/a.parquet"))
    assert(actionPlan.contains("mask email"))
  }

  test("collect removes legacy pattern allowlist entries that cover directory evidence") {
    val sparkSession = spark
    import sparkSession.implicits._

    val scanRoot = Files.createTempDirectory("privyspark-review-scan-pattern-directory-")
    val stateRoot = Files.createTempDirectory("privyspark-review-state-pattern-directory-")
    val scanResultsPath = scanRoot.resolve("parquet/scan_results")
    Files.createDirectories(scanResultsPath.getParent)
    Files.createDirectories(stateRoot.resolve("inbox"))
    Files.createDirectories(stateRoot.resolve("current"))

    val fingerprints = Seq(
      RecordedFileFingerprint(
        fileIdentifier = "customers/a.parquet",
        fileSize = 128L,
        fileMtimeEpochMs = 1710000000000L,
        fileChecksumAlgo = "CRC32",
        fileChecksum = "aaaa1111"
      ),
      RecordedFileFingerprint(
        fileIdentifier = "customers/b.parquet",
        fileSize = 256L,
        fileMtimeEpochMs = 1710000000001L,
        fileChecksumAlgo = "CRC32",
        fileChecksum = "bbbb2222"
      )
    )
    val findingResult = scanResult(
      columnName = "email",
      piiType = "email",
      sample = "alice@example.com",
      scopeFingerprints = ReviewScopeFingerprintCodec.encode(fingerprints),
      fileIdentifier = "customers"
    )
    Seq(findingResult).toDS().toDF().write.mode("overwrite").parquet(scanResultsPath.toString)

    val legacyPattern =
      """{"entry_type":"pattern","dataset_path":"/data/project","file_identifier_pattern":"customers/*","column_name_pattern":"email","pii_type_pattern":"email","reason":"legacy directory false positive","reviewer":"owner@example.com","reviewed_at":"2026-04-20T00:00:00Z","expires_at":"2999-12-31","source_finding_key":"legacy-hive-table-key"}"""
    Files.write(stateRoot.resolve("current/allowlist.jsonl"), s"$legacyPattern\n".getBytes(StandardCharsets.UTF_8))

    val findings = ReviewFindingBuilder.fromScanResults(Seq(findingResult))
    val scanFingerprint = ReviewFindingBuilder.scanResultsFingerprint(findings)
    val finding = findings.head
    val responseJson =
      s"""{"schema_version":1,"scan_path":"/data/project","scan_results_fingerprint":"$scanFingerprint","responder":"owner@example.com","responded_at":"2026-04-27T12:15:00Z","responses":[{"finding_key":"${finding.findingKey}","finding_hash":"${finding.findingHash}","decision":"true_positive","false_positive_reason":null,"allowlist_scope":null,"file_identifier_pattern":null,"column_name_pattern":null,"pii_type_pattern":null,"expires_at":null,"action_plan":"mask directory email","action_due_date":"2999-12-31"}]}"""
    Files.write(stateRoot.resolve("inbox/owner-response.json"), responseJson.getBytes(StandardCharsets.UTF_8))

    ReviewCollectCommand.run(
      spark,
      ReviewCollectCliConfig(scanResultsPath.toString, stateRoot.toString)
    )

    val allowlist = read(stateRoot.resolve("current/allowlist.jsonl"))
    val actionPlan = read(stateRoot.resolve("current/action_plan.jsonl"))

    assert(!allowlist.contains("legacy directory false positive"))
    assert(!allowlist.contains("customers/*"))
    assert(actionPlan.contains("customers"))
    assert(actionPlan.contains("mask directory email"))
  }

  test("collect removes legacy action plans that cover a newly reviewed finding") {
    val sparkSession = spark
    import sparkSession.implicits._

    val scanRoot = Files.createTempDirectory("privyspark-review-scan-action-")
    val stateRoot = Files.createTempDirectory("privyspark-review-state-action-")
    val scanResultsPath = scanRoot.resolve("parquet/scan_results")
    Files.createDirectories(scanResultsPath.getParent)
    Files.createDirectories(stateRoot.resolve("inbox"))
    Files.createDirectories(stateRoot.resolve("current"))

    val fingerprint = RecordedFileFingerprint(
      fileIdentifier = "customers/a.parquet",
      fileSize = 128L,
      fileMtimeEpochMs = 1710000000000L,
      fileChecksumAlgo = "CRC32",
      fileChecksum = "aaaa1111"
    )
    val findingResult = scanResult(
      columnName = "email",
      piiType = "email",
      sample = "alice@example.com",
      scopeFingerprints = ReviewScopeFingerprintCodec.encode(Seq(fingerprint)),
      fileIdentifier = "customers/a.parquet"
    )
    Seq(findingResult).toDS().toDF().write.mode("overwrite").parquet(scanResultsPath.toString)

    val legacyActionPlan =
      """{"finding_key":"legacy-hive-table-key","scan_path":"/data/project","hive_database":"","hive_table":"","hive_table_fqn":"","column_name":"email","pii_type":"email","action_plan":"legacy mask email","action_due_date":"2999-12-31","responder":"owner@example.com","responded_at":"2026-04-20T00:00:00Z","status":"remediation_planned"}"""
    Files.write(stateRoot.resolve("current/action_plan.jsonl"), s"$legacyActionPlan\n".getBytes(StandardCharsets.UTF_8))

    val findings = ReviewFindingBuilder.fromScanResults(Seq(findingResult))
    val scanFingerprint = ReviewFindingBuilder.scanResultsFingerprint(findings)
    val finding = findings.head
    val responseJson =
      s"""{"schema_version":1,"scan_path":"/data/project","scan_results_fingerprint":"$scanFingerprint","responder":"owner@example.com","responded_at":"2026-04-27T12:30:00Z","responses":[{"finding_key":"${finding.findingKey}","finding_hash":"${finding.findingHash}","decision":"true_positive","false_positive_reason":null,"allowlist_scope":null,"file_identifier_pattern":null,"column_name_pattern":null,"pii_type_pattern":null,"expires_at":null,"action_plan":"new mask email","action_due_date":"2999-12-31"}]}"""
    Files.write(stateRoot.resolve("inbox/owner-response.json"), responseJson.getBytes(StandardCharsets.UTF_8))

    ReviewCollectCommand.run(
      spark,
      ReviewCollectCliConfig(scanResultsPath.toString, stateRoot.toString)
    )

    val actionPlan = read(stateRoot.resolve("current/action_plan.jsonl"))
    val findingStatus = read(stateRoot.resolve("current/finding_status.jsonl"))

    assert(!actionPlan.contains("legacy mask email"))
    assert(actionPlan.contains("new mask email"))
    assert(actionPlan.contains("customers/a.parquet"))
    assert(!findingStatus.contains("remediated_candidate"))
  }

  private def scanResult(
    columnName: String,
    piiType: String,
    sample: String,
    scopeFingerprints: String,
    datasetPath: String = "/data/project",
    fileIdentifier: String = "customers/part-000.parquet"
  ): ScanResult =
    ScanResult(
      dataset_path = datasetPath,
      scan_timestamp = "2026-04-27T10:00:00Z",
      file_identifier = fileIdentifier,
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
