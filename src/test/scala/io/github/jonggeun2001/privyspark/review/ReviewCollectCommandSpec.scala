package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.cli.ReviewCollectCliConfig
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

  test("collect reads response JSON without scan results and writes recurring allowlist state") {
    val stateRoot = Files.createTempDirectory("privyspark-review-state-recurring-")
    Files.createDirectories(stateRoot.resolve("inbox"))

    Files.write(
      stateRoot.resolve("inbox/owner-response.json"),
      responseEnvelope(
        scanPath = "/data/project",
        responses = Seq(
          falsePositiveResponse(
            findingKey = "finding-email",
            columnName = "email",
            piiType = "email",
            reason = "daily dummy account column"
          ),
          truePositiveResponse(
            findingKey = "finding-phone",
            columnName = "phone",
            piiType = "phone_number",
            actionPlan = "mask column"
          )
        )
      ).getBytes(StandardCharsets.UTF_8)
    )

    ReviewCollectCommand.run(
      spark,
      ReviewCollectCliConfig(reviewStateRoot = stateRoot.toString)
    )

    val allowlist = read(stateRoot.resolve("current/allowlist.jsonl"))
    val actionPlan = read(stateRoot.resolve("current/action_plan.jsonl"))
    val findingStatus = read(stateRoot.resolve("current/finding_status.jsonl"))
    val ledger = read(stateRoot.resolve("current/response_ledger.jsonl"))

    assert(allowlist.contains(""""entry_type":"recurring""""))
    assert(allowlist.contains("mart.customers"))
    assert(allowlist.contains("\"file_identifier_pattern\":\"\""))
    assert(allowlist.contains("daily dummy account column"))
    assert(allowlist.contains(""""sample_row_count":1000"""))
    assert(!allowlist.contains("file_checksum"))
    assert(!allowlist.contains(""""entry_type":"pattern""""))
    assert(actionPlan.contains("mask column"))
    assert(actionPlan.contains("phone_number"))
    assert(findingStatus.contains("false_positive"))
    assert(findingStatus.contains("remediation_planned"))
    assert(ledger.contains("finding-email"))
    assert(ledger.contains("finding-phone"))
  }

  test("collect accepts response JSON with a leading UTF-8 BOM") {
    val stateRoot = Files.createTempDirectory("privyspark-review-state-bom-")
    Files.createDirectories(stateRoot.resolve("inbox"))

    Files.write(
      stateRoot.resolve("inbox/owner-response.json"),
      ("\uFEFF" + responseEnvelope(
        scanPath = "/data/project",
        responses = Seq(falsePositiveResponse(
          findingKey = "finding-email-bom",
          columnName = "email",
          piiType = "email",
          reason = "owner confirmed recurring false positive"
        ))
      )).getBytes(StandardCharsets.UTF_8)
    )

    ReviewCollectCommand.run(
      spark,
      ReviewCollectCliConfig(reviewStateRoot = stateRoot.toString)
    )

    val allowlist = read(stateRoot.resolve("current/allowlist.jsonl"))
    val ledger = read(stateRoot.resolve("current/response_ledger.jsonl"))

    assert(allowlist.contains("finding-email-bom"))
    assert(allowlist.contains("owner confirmed recurring false positive"))
    assert(ledger.contains("finding-email-bom"))
  }

  test("collect rejects exact allowlist scope") {
    val stateRoot = Files.createTempDirectory("privyspark-review-state-exact-reject-")
    Files.createDirectories(stateRoot.resolve("inbox"))

    val response =
      falsePositiveResponse(
        findingKey = "finding-email",
        columnName = "email",
        piiType = "email",
        reason = "legacy exact",
        extra = ",\"allowlist_scope\":\"exact\""
      )
    Files.write(
      stateRoot.resolve("inbox/owner-response.json"),
      responseEnvelope("/data/project", Seq(response)).getBytes(StandardCharsets.UTF_8)
    )

    val thrown = intercept[IllegalStateException] {
      ReviewCollectCommand.run(
        spark,
        ReviewCollectCliConfig(reviewStateRoot = stateRoot.toString)
      )
    }

    assert(thrown.getMessage.contains("Rejected review responses"))
    assert(thrown.getMessage.contains("unsupported allowlist_scope: exact"))
    assert(!Files.exists(stateRoot.resolve("current/allowlist.jsonl")))
    assert(!Files.exists(stateRoot.resolve(".collect.lock")))
  }

  test("collect rejects wildcard column or pii fields in new recurring false positive responses") {
    val stateRoot = Files.createTempDirectory("privyspark-review-state-field-wildcard-reject-")
    Files.createDirectories(stateRoot.resolve("inbox"))

    val wildcardColumn =
      falsePositiveResponse(
        findingKey = "finding-email-column",
        columnName = "temp_*",
        piiType = "email",
        reason = "unsafe wildcard"
      )
    val wildcardPii =
      falsePositiveResponse(
        findingKey = "finding-email-pii",
        columnName = "email",
        piiType = "*",
        reason = "unsafe wildcard"
      )
    Files.write(
      stateRoot.resolve("inbox/owner-response.json"),
      responseEnvelope("/data/project", Seq(wildcardColumn, wildcardPii)).getBytes(StandardCharsets.UTF_8)
    )

    val thrown = intercept[IllegalStateException] {
      ReviewCollectCommand.run(
        spark,
        ReviewCollectCliConfig(reviewStateRoot = stateRoot.toString)
      )
    }

    assert(thrown.getMessage.contains("Rejected review responses"))
    assert(thrown.getMessage.contains("column_name and pii_type must be exact values without wildcard '*'"))
    assert(!Files.exists(stateRoot.resolve("current/allowlist.jsonl")))
    assert(!Files.exists(stateRoot.resolve(".collect.lock")))
  }

  test("collect fails while review state collect lock exists") {
    val stateRoot = Files.createTempDirectory("privyspark-review-state-lock-held-")
    Files.createDirectories(stateRoot.resolve("inbox"))
    Files.write(stateRoot.resolve(".collect.lock"), "locked by another run\n".getBytes(StandardCharsets.UTF_8))

    val thrown = intercept[IllegalStateException] {
      ReviewCollectCommand.run(
        spark,
        ReviewCollectCliConfig(reviewStateRoot = stateRoot.toString)
      )
    }

    assert(thrown.getMessage.contains("Review collect lock already exists"))
    assert(Files.exists(stateRoot.resolve(".collect.lock")))
    assert(!Files.exists(stateRoot.resolve("current/allowlist.jsonl")))
  }

  test("collect removes recurring allowlist entry when same finding is later reviewed as true positive") {
    val stateRoot = Files.createTempDirectory("privyspark-review-state-replace-")
    Files.createDirectories(stateRoot.resolve("inbox"))
    Files.createDirectories(stateRoot.resolve("current"))

    val recurring =
      """{"entry_type":"recurring","scan_path":"/data/project","hive_table_fqn":"mart.customers","file_identifier_pattern":"","column_name":"email","pii_type":"email","reason":"old false positive","reviewer":"owner@example.com","reviewed_at":"2026-04-20T00:00:00Z","expires_at":"2999-12-31","source_finding_key":"old-finding","sample_row_count":1000,"match_count":12,"non_empty_match_ratio":0.12}"""
    Files.write(stateRoot.resolve("current/allowlist.jsonl"), s"$recurring\n".getBytes(StandardCharsets.UTF_8))

    Files.write(
      stateRoot.resolve("inbox/owner-response.json"),
      responseEnvelope(
        scanPath = "/data/project",
        responses = Seq(truePositiveResponse(
          findingKey = "finding-email",
          columnName = "email",
          piiType = "email",
          actionPlan = "mask email"
        ))
      ).getBytes(StandardCharsets.UTF_8)
    )

    ReviewCollectCommand.run(
      spark,
      ReviewCollectCliConfig(reviewStateRoot = stateRoot.toString)
    )

    val allowlist = read(stateRoot.resolve("current/allowlist.jsonl"))
    val actionPlan = read(stateRoot.resolve("current/action_plan.jsonl"))

    assert(!allowlist.contains("old false positive"))
    assert(actionPlan.contains("mask email"))
  }

  test("collect removes wildcard recurring allowlist entry when covered finding is later true positive") {
    val stateRoot = Files.createTempDirectory("privyspark-review-state-wildcard-replace-")
    Files.createDirectories(stateRoot.resolve("inbox"))
    Files.createDirectories(stateRoot.resolve("current"))

    val recurring =
      """{"entry_type":"recurring","scan_path":"/data/project","hive_table_fqn":"","file_identifier_pattern":"customers/*","column_name":"email","pii_type":"email","reason":"old broad false positive","reviewer":"owner@example.com","reviewed_at":"2026-04-20T00:00:00Z","expires_at":"2999-12-31","source_finding_key":"old-finding","sample_row_count":1000,"match_count":12,"non_empty_match_ratio":0.12}"""
    Files.write(stateRoot.resolve("current/allowlist.jsonl"), s"$recurring\n".getBytes(StandardCharsets.UTF_8))

    val truePositive =
      """{"finding_key":"finding-email","finding_hash":"hash-finding-email","file_identifier":"customers/a.parquet","column_name":"email","pii_type":"email","sample_row_count":1000,"match_count":830,"non_empty_match_ratio":0.83,"decision":"true_positive","action_plan":"mask email","action_due_date":"2999-12-31"}"""
    Files.write(
      stateRoot.resolve("inbox/owner-response.json"),
      responseEnvelope(scanPath = "/data/project", responses = Seq(truePositive)).getBytes(StandardCharsets.UTF_8)
    )

    ReviewCollectCommand.run(
      spark,
      ReviewCollectCliConfig(reviewStateRoot = stateRoot.toString)
    )

    val allowlist = read(stateRoot.resolve("current/allowlist.jsonl"))
    val actionPlan = read(stateRoot.resolve("current/action_plan.jsonl"))

    assert(!allowlist.contains("old broad false positive"))
    assert(!allowlist.contains("customers/*"))
    assert(actionPlan.contains("mask email"))
  }

  test("collect removes wildcard recurring allowlist entry when directory finding is later true positive") {
    val stateRoot = Files.createTempDirectory("privyspark-review-state-wildcard-directory-replace-")
    Files.createDirectories(stateRoot.resolve("inbox"))
    Files.createDirectories(stateRoot.resolve("current"))

    val recurring =
      """{"entry_type":"recurring","scan_path":"/data/project","hive_table_fqn":"","file_identifier_pattern":"customers/*","column_name":"email","pii_type":"email","reason":"old broad false positive","reviewer":"owner@example.com","reviewed_at":"2026-04-20T00:00:00Z","expires_at":"2999-12-31","source_finding_key":"old-finding","sample_row_count":1000,"match_count":12,"non_empty_match_ratio":0.12}"""
    Files.write(stateRoot.resolve("current/allowlist.jsonl"), s"$recurring\n".getBytes(StandardCharsets.UTF_8))

    val truePositive =
      """{"finding_key":"finding-email","finding_hash":"hash-finding-email","file_identifier":"customers","column_name":"email","pii_type":"email","sample_row_count":1000,"match_count":830,"non_empty_match_ratio":0.83,"decision":"true_positive","action_plan":"mask customer emails","action_due_date":"2999-12-31"}"""
    Files.write(
      stateRoot.resolve("inbox/owner-response.json"),
      responseEnvelope(scanPath = "/data/project", responses = Seq(truePositive)).getBytes(StandardCharsets.UTF_8)
    )

    ReviewCollectCommand.run(
      spark,
      ReviewCollectCliConfig(reviewStateRoot = stateRoot.toString)
    )

    val allowlist = read(stateRoot.resolve("current/allowlist.jsonl"))
    val actionPlan = read(stateRoot.resolve("current/action_plan.jsonl"))

    assert(!allowlist.contains("old broad false positive"))
    assert(!allowlist.contains("customers/*"))
    assert(actionPlan.contains("mask customer emails"))
  }

  test("collect removes legacy wildcard field pattern when covered finding is later true positive") {
    val stateRoot = Files.createTempDirectory("privyspark-review-state-legacy-field-wildcard-replace-")
    Files.createDirectories(stateRoot.resolve("inbox"))
    Files.createDirectories(stateRoot.resolve("current"))

    val pattern =
      """{"entry_type":"pattern","dataset_path":"/data/project","file_identifier":"reviews/*","column_name":"temp_*","pii_type":"email","reason":"old temporary false positive","reviewer":"owner@example.com","reviewed_at":"2026-04-20T00:00:00Z","expires_at":"2999-12-31","source_finding_key":"old-finding"}"""
    Files.write(stateRoot.resolve("current/allowlist.jsonl"), s"$pattern\n".getBytes(StandardCharsets.UTF_8))

    val truePositive =
      """{"finding_key":"finding-temp-email","finding_hash":"hash-finding-temp-email","file_identifier":"reviews/part-000.parquet","column_name":"temp_email","pii_type":"email","sample_row_count":1000,"match_count":830,"non_empty_match_ratio":0.83,"decision":"true_positive","action_plan":"mask temp email","action_due_date":"2999-12-31"}"""
    Files.write(
      stateRoot.resolve("inbox/owner-response.json"),
      responseEnvelope(scanPath = "/data/project", responses = Seq(truePositive)).getBytes(StandardCharsets.UTF_8)
    )

    ReviewCollectCommand.run(
      spark,
      ReviewCollectCliConfig(reviewStateRoot = stateRoot.toString)
    )

    val allowlist = read(stateRoot.resolve("current/allowlist.jsonl"))
    val actionPlan = read(stateRoot.resolve("current/action_plan.jsonl"))

    assert(!allowlist.contains("old temporary false positive"))
    assert(!allowlist.contains("temp_*"))
    assert(actionPlan.contains("mask temp email"))
  }

  test("collect preserves legacy field wildcard marker for retained pattern entries") {
    val stateRoot = Files.createTempDirectory("privyspark-review-state-legacy-field-wildcard-retain-")
    Files.createDirectories(stateRoot.resolve("inbox"))
    Files.createDirectories(stateRoot.resolve("current"))

    val pattern =
      """{"entry_type":"pattern","dataset_path":"/data/project","file_identifier":"reviews/*","column_name":"temp_*","pii_type":"email","reason":"old temporary false positive","reviewer":"owner@example.com","reviewed_at":"2026-04-20T00:00:00Z","expires_at":"2999-12-31","source_finding_key":"old-finding"}"""
    Files.write(stateRoot.resolve("current/allowlist.jsonl"), s"$pattern\n".getBytes(StandardCharsets.UTF_8))

    Files.write(
      stateRoot.resolve("inbox/owner-response.json"),
      responseEnvelope(
        scanPath = "/data/project",
        responses = Seq(falsePositiveResponse(
          findingKey = "finding-phone",
          columnName = "phone",
          piiType = "phone_number",
          reason = "known sample phone"
        ))
      ).getBytes(StandardCharsets.UTF_8)
    )

    ReviewCollectCommand.run(
      spark,
      ReviewCollectCliConfig(reviewStateRoot = stateRoot.toString)
    )

    val allowlist = read(stateRoot.resolve("current/allowlist.jsonl"))

    assert(allowlist.contains("old temporary false positive"))
    assert(allowlist.contains(""""legacy_field_wildcards":true"""))
    assert(allowlist.contains("temp_*"))
    assert(AllowlistMatcher
      .load(spark.sparkContext.hadoopConfiguration, stateRoot.resolve("current/allowlist.jsonl").toString)
      .evaluate("/data/project", "", "reviews/part-000.parquet", "temp_email", "email", Seq.empty)
      .shouldSuppress)
  }

  test("collect preserves leading spaces in non-hive recurring file identifiers") {
    val stateRoot = Files.createTempDirectory("privyspark-review-state-spaced-identifier-")
    Files.createDirectories(stateRoot.resolve("inbox"))

    val falsePositive =
      """{"finding_key":"finding-spaced-email","finding_hash":"hash-finding-spaced-email","file_identifier":" reviews/a.csv","column_name":"email","pii_type":"email","sample_row_count":1000,"match_count":12,"non_empty_match_ratio":0.12,"decision":"false_positive","false_positive_reason":"known spaced identifier","expires_at":"2999-12-31"}"""
    Files.write(
      stateRoot.resolve("inbox/owner-response.json"),
      responseEnvelope(scanPath = "/data/project", responses = Seq(falsePositive)).getBytes(StandardCharsets.UTF_8)
    )

    ReviewCollectCommand.run(
      spark,
      ReviewCollectCliConfig(reviewStateRoot = stateRoot.toString)
    )

    val allowlistPath = stateRoot.resolve("current/allowlist.jsonl")
    val allowlist = read(allowlistPath)

    assert(allowlist.contains(""""file_identifier_pattern":" reviews/a.csv""""))
    assert(AllowlistMatcher
      .load(spark.sparkContext.hadoopConfiguration, allowlistPath.toString)
      .evaluate("/data/project", "", " reviews/a.csv", "email", "email", Seq.empty)
      .shouldSuppress)
  }

  test("collect preserves leading spaces in recurring column names") {
    val stateRoot = Files.createTempDirectory("privyspark-review-state-spaced-column-")
    Files.createDirectories(stateRoot.resolve("inbox"))

    val falsePositive =
      """{"finding_key":"finding-spaced-column","finding_hash":"hash-finding-spaced-column","file_identifier":"customers/a.csv","column_name":" email","pii_type":"email","sample_row_count":1000,"match_count":12,"non_empty_match_ratio":0.12,"decision":"false_positive","false_positive_reason":"known spaced column","expires_at":"2999-12-31"}"""
    Files.write(
      stateRoot.resolve("inbox/owner-response.json"),
      responseEnvelope(scanPath = "/data/project", responses = Seq(falsePositive)).getBytes(StandardCharsets.UTF_8)
    )

    ReviewCollectCommand.run(
      spark,
      ReviewCollectCliConfig(reviewStateRoot = stateRoot.toString)
    )

    val allowlistPath = stateRoot.resolve("current/allowlist.jsonl")
    val allowlist = read(allowlistPath)

    assert(allowlist.contains(""""column_name":" email""""))
    assert(AllowlistMatcher
      .load(spark.sparkContext.hadoopConfiguration, allowlistPath.toString)
      .evaluate("/data/project", "", "customers/a.csv", " email", "email", Seq.empty)
      .shouldSuppress)
  }

  test("collect keeps hdfs slash variants normalized when replacing recurring state") {
    val stateRoot = Files.createTempDirectory("privyspark-review-state-hdfs-")
    Files.createDirectories(stateRoot.resolve("inbox"))
    Files.createDirectories(stateRoot.resolve("current"))

    val recurring =
      """{"entry_type":"recurring","scan_path":"hdfs:////user/username","hive_table_fqn":"mart.customers","file_identifier_pattern":"","column_name":"email","pii_type":"email","reason":"old false positive","reviewer":"owner@example.com","reviewed_at":"2026-04-20T00:00:00Z","expires_at":"2999-12-31","source_finding_key":"old-finding","sample_row_count":1000,"match_count":12,"non_empty_match_ratio":0.12}"""
    Files.write(stateRoot.resolve("current/allowlist.jsonl"), s"$recurring\n".getBytes(StandardCharsets.UTF_8))

    Files.write(
      stateRoot.resolve("inbox/owner-response.json"),
      responseEnvelope(
        scanPath = "hdfs:///user/username",
        responses = Seq(truePositiveResponse(
          findingKey = "finding-email",
          columnName = "email",
          piiType = "email",
          actionPlan = "mask email"
        ))
      ).getBytes(StandardCharsets.UTF_8)
    )

    ReviewCollectCommand.run(
      spark,
      ReviewCollectCliConfig(reviewStateRoot = stateRoot.toString)
    )

    val allowlist = read(stateRoot.resolve("current/allowlist.jsonl"))
    val actionPlan = read(stateRoot.resolve("current/action_plan.jsonl"))

    assert(!allowlist.contains("old false positive"))
    assert(actionPlan.contains("hdfs:///user/username"))
  }

  private def responseEnvelope(scanPath: String, responses: Seq[String]): String =
    s"""{"schema_version":1,"scan_path":"$scanPath","responder":"owner1","responded_at":"2026-04-30T10:00:00Z","responses":[${responses.mkString(",")}]}"""

  private def falsePositiveResponse(
    findingKey: String,
    columnName: String,
    piiType: String,
    reason: String,
    extra: String = ""
  ): String =
    s"""{"finding_key":"$findingKey","finding_hash":"hash-$findingKey","file_identifier":"customers/part-000.parquet","hive_database":"mart","hive_table":"customers","hive_table_fqn":"mart.customers","column_name":"$columnName","pii_type":"$piiType","sample_row_count":1000,"match_count":12,"non_empty_match_ratio":0.12,"decision":"false_positive","false_positive_reason":"$reason","expires_at":"2999-12-31"$extra}"""

  private def truePositiveResponse(
    findingKey: String,
    columnName: String,
    piiType: String,
    actionPlan: String
  ): String =
    s"""{"finding_key":"$findingKey","finding_hash":"hash-$findingKey","file_identifier":"customers/part-000.parquet","hive_database":"mart","hive_table":"customers","hive_table_fqn":"mart.customers","column_name":"$columnName","pii_type":"$piiType","sample_row_count":1000,"match_count":830,"non_empty_match_ratio":0.83,"decision":"true_positive","action_plan":"$actionPlan","action_due_date":"2999-12-31"}"""

  private def read(path: java.nio.file.Path): String =
    new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
}
