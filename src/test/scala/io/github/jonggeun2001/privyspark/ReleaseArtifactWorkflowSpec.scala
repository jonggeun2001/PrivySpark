package io.github.jonggeun2001.privyspark

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

@RunWith(classOf[JUnitRunner])
class ReleaseArtifactWorkflowSpec extends AnyFunSuite {
  private val workflowPath = Paths.get(".github", "workflows", "release-artifact.yml")

  test("release workflow uploads default-rules.yaml as a release asset") {
    val workflow = readText(workflowPath)

    assert(workflow.contains("config/rules/default.yaml"), "release workflow should stage the default ruleset file")
    assert(workflow.contains("default-rules.yaml"), "release workflow should publish a default-rules.yaml asset")
    assert(workflow.contains("asset_rules"), "release workflow should expose the rules asset as a workflow output")
  }

  test("release workflow publishes offline review response HTML example") {
    val workflow = readText(workflowPath)

    assert(workflow.contains("review_response_example=\"samples/offline-review/review-response-example.html\""))
    assert(workflow.contains("asset_review_response_example=\"build/distributions/privyspark-${{ steps.meta.outputs.tag }}-review-response-example.html\""))
    assert(workflow.contains("${{ steps.assets.outputs.asset_review_response_example }}"))
  }

  test("release workflow publishes offline review response JSON viewer") {
    val workflow = readText(workflowPath)

    assert(workflow.contains("review_response_viewer=\"samples/offline-review/review-response-viewer.html\""))
    assert(workflow.contains("asset_review_response_viewer=\"build/distributions/privyspark-${{ steps.meta.outputs.tag }}-review-response-viewer.html\""))
    assert(workflow.contains("${{ steps.assets.outputs.asset_review_response_viewer }}"))
  }

  test("offline review response HTML example is self-contained and downloads response JSON") {
    val html = readText("samples/offline-review/review-response-example.html")

    assert(html.contains("<!doctype html>"))
    assert(html.contains("const REVIEW_DATA ="))
    assert(html.contains("\"schema_version\": 1"))
    assert(html.contains("\"scan_results_fingerprint\""))
    assert(html.contains("\"finding_key\""))
    assert(html.contains("\"finding_hash\""))
    assert(html.contains("\"fingerprint_complete\""))
    assert(html.contains("\"has_multiple_file_evidence\""))
    assert(html.contains("exact: 이 finding만 제외"))
    assert(html.contains("pattern: 반복 오탐을 넓게 제외"))
    assert(html.contains("<button type=\"button\" id=\"downloadResponse\">응답 파일 생성</button>"))
    assert(html.contains("function formatResponseTimestamp"))
    assert(html.contains("link.download = `response-${formatResponseTimestamp(new Date())}.json`;"))
    assert(!html.contains("privyspark-response.json 다운로드"))
    assert(!html.contains("response.json 다운로드"))
    assert(!html.contains("link.download = 'privyspark-response.json'"))
    assert(!html.contains("owner@example.test"))
    assert(html.contains("""<th scope="col" data-sort-key="path" aria-sort="none"><button type="button" class="sort-button">경로 / Hive <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="column" aria-sort="none"><button type="button" class="sort-button">컬럼 / PII <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="metrics" aria-sort="none"><button type="button" class="sort-button">지표 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="sample" aria-sort="none"><button type="button" class="sort-button">샘플 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="decision" aria-sort="none"><button type="button" class="sort-button">판정 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="scope" aria-sort="none"><button type="button" class="sort-button">Allowlist Scope <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="reason" aria-sort="none"><button type="button" class="sort-button">사유 / 계획 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("let sortState = { key: null, direction: 'asc' };"))
    assert(html.contains("function collectFormValues()"))
    assert(html.contains("function renderFindings(savedValues = {})"))
    assert(html.contains("function sortRows(rows, values)"))
    assert(html.contains("<input id=\"bulkDeleteDueDate\" type=\"date\">"))
    assert(html.contains("<button type=\"button\" id=\"applyBulkDeletePlan\">일괄 삭제 계획 등록</button>"))
    assert(html.contains("const BulkDeleteActionPlan = '삭제 처리';"))
    assert(html.contains("function applyDecisionVisibility(row)"))
    assert(html.contains("function applyBulkDeletePlan()"))
    assert(html.contains("function sanitizeResponse(response)"))
    assert(html.contains("data-decision-section=\"false_positive\""))
    assert(html.contains("data-decision-section=\"true_positive\""))
    assert(html.contains("document.querySelectorAll('#findingsTable th[data-sort-key] button').forEach(button => {"))
    assert(!html.contains("<th>Path / Hive</th>"))
    assert(!html.contains("<th>Column / PII</th>"))
    assert(!html.contains("http://"))
    assert(!html.contains("https://"))
  }

  test("offline review response JSON viewer is self-contained and loads local response files") {
    val html = readText("samples/offline-review/review-response-viewer.html")

    assert(html.contains("<!doctype html>"))
    assert(html.contains("type=\"file\""))
    assert(html.contains("accept=\"application/json,.json\""))
    assert(html.contains("FileReader"))
    assert(html.contains("JSON.parse"))
    assert(html.contains("schema_version"))
    assert(html.contains("scan_results_fingerprint"))
    assert(html.contains("response-YYYYMMDD-HHMMSS.json"))
    assert(!html.contains("privyspark-response.json"))
    assert(html.contains("오프라인 응답 JSON 확인"))
    assert(html.contains("responder 값이 필요합니다."))
    assert(html.contains("responded_at 값은 ISO-8601 instant여야 합니다."))
    assert(html.contains("responses 배열은 비어 있을 수 없습니다."))
    assert(html.contains("const textValue ="))
    assert(html.contains("typeof value === 'string'"))
    assert(html.contains("Date.parse(value)"))
    assert(html.contains("getUTCFullYear"))
    assert(html.contains("false_positive_reason 값이 필요합니다."))
    assert(html.contains("unsupported allowlist_scope"))
    assert(html.contains("expires_at 값이 필요합니다."))
    assert(html.contains("expires_at 값은 YYYY-MM-DD 형식이어야 합니다."))
    assert(html.contains("pattern allowlist에는 pattern 필드가 하나 이상 필요합니다."))
    assert(html.contains("다중 파일 finding은 collector에서 file_identifier_pattern을 요구할 수 있습니다."))
    assert(!html.contains("file_identifier_pattern 값이 필요합니다."))
    assert(html.contains("pii_type_pattern=* 값은 허용되지 않습니다."))
    assert(html.contains("action_plan과 action_due_date 값이 필요합니다."))
    assert(html.contains("action_due_date 값은 YYYY-MM-DD 형식이어야 합니다."))
    assert(html.contains("response는 객체여야 합니다."))
    assert(html.contains("const isDateOnly ="))
    assert(html.contains("const clearRenderedState ="))
    assert(html.contains("data-field=\"decision\""))
    assert(!html.contains("http://"))
    assert(!html.contains("https://"))
  }

  test("shadow jar does not bundle the MariaDB JDBC driver") {
    val buildScript = readText("build.gradle.kts")

    assert(
      buildScript.contains("""compileOnly("org.mariadb.jdbc:mariadb-java-client:"""),
      "MariaDB JDBC driver must stay out of runtimeClasspath so Shadow JAR does not package it"
    )
    assert(
      !buildScript.contains("""implementation("org.mariadb.jdbc:mariadb-java-client:"""),
      "MariaDB JDBC driver must not be an implementation dependency"
    )
  }

  test("submit script can pass external JDBC jars to spark-submit") {
    val submitScript = readText(Paths.get("bin", "privyspark-submit"))

    assert(submitScript.contains("PRIVYSPARK_JARS"), "submit script should expose an env hook for external driver jars")
    assert(submitScript.contains("--jars"), "submit script should pass external driver jars to spark-submit")
  }

  private def readText(path: String): String =
    readText(Paths.get(path))

  private def readText(path: java.nio.file.Path): String =
    new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
}
