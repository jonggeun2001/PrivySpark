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
    assert(html.contains("검토 안내"))
    assert(html.contains("오탐 제외 범위는 동일 fingerprint만 제외"))
    assert(html.contains("개인정보 유형 패턴 <code>운전면허번호</code>"))
    assert(html.contains("<button type=\"button\" id=\"downloadResponse\">응답 파일 생성</button>"))
    assert(html.contains("function formatResponseTimestamp"))
    assert(html.contains("link.download = `response-${formatResponseTimestamp(new Date())}.json`;"))
    assert(!html.contains("privyspark-response.json 다운로드"))
    assert(!html.contains("response.json 다운로드"))
    assert(!html.contains("link.download = 'privyspark-response.json'"))
    assert(!html.contains("owner@example.test"))
    assert(html.contains("""<th scope="col" data-sort-key="path" aria-sort="none"><button type="button" class="sort-button">경로 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="hive" aria-sort="none"><button type="button" class="sort-button">Hive 테이블 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="column" aria-sort="none"><button type="button" class="sort-button">컬럼명 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="pii" aria-sort="none"><button type="button" class="sort-button">개인정보 유형 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="sampled_row_count" aria-sort="none"><button type="button" class="sort-button">샘플 행 수 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="match_count" aria-sort="none"><button type="button" class="sort-button">검출 건수 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="non_empty_match_ratio" aria-sort="none"><button type="button" class="sort-button">검출 비율 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="sample" aria-sort="none"><button type="button" class="sort-button">검출 샘플 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="decision" aria-sort="none"><button type="button" class="sort-button">판정 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="scope" aria-sort="none"><button type="button" class="sort-button">오탐 제외 범위 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="false_positive_reason" aria-sort="none"><button type="button" class="sort-button">오탐 사유 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="file_identifier_pattern" aria-sort="none"><button type="button" class="sort-button">경로 패턴 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="column_name_pattern" aria-sort="none"><button type="button" class="sort-button">컬럼명 패턴 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="pii_type_pattern" aria-sort="none"><button type="button" class="sort-button">개인정보 유형 패턴 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="expires_at" aria-sort="none"><button type="button" class="sort-button">패턴 만료일 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="action_plan" aria-sort="none"><button type="button" class="sort-button">정탐 조치 계획 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="action_due_date" aria-sort="none"><button type="button" class="sort-button">조치 예정일 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<span hidden data-finding-key="${escapeHtml(finding.finding_key)}">${escapeHtml(finding.finding_key)}</span>"""))
    assert(html.contains("""<td class="metric-cell">${escapeHtml(finding.sampled_row_count)}</td>"""))
    assert(html.contains("""<td class="metric-cell">${escapeHtml(finding.match_count)}</td>"""))
    assert(html.contains("""<td class="metric-cell">${escapeHtml(finding.non_empty_match_ratio)}</td>"""))
    assert(!html.contains("confidence=${escapeHtml(finding.confidence)}"))
    assert(html.contains("let sortState = { key: null, direction: 'asc' };"))
    assert(html.contains("const formState = new Map();"))
    assert(html.contains("const collator = new Intl.Collator('ko-KR'"))
    assert(html.contains("const hydratedRows = new Map();"))
    assert(html.contains("const PiiTypeLabels = {"))
    assert(html.contains("driver_license_number: '운전면허번호'"))
    assert(html.contains("function displayPiiType(value)"))
    assert(html.contains("function normalizePiiTypePattern(value)"))
    assert(html.contains("function defaultFormState(finding)"))
    assert(html.contains("function collectFormValues()"))
    assert(html.contains("function formValuesSnapshot()"))
    assert(html.contains("function renderFindings()"))
    assert(html.contains("function renderPlaceholderRow(index)"))
    assert(html.contains("function hydrateRow(row)"))
    assert(html.contains("function dehydrateRow(row)"))
    assert(html.contains("function sortRows(rows)"))
    assert(html.contains("tbody.replaceChildren(fragment);"))
    assert(!html.contains("document.querySelectorAll('[data-index]').forEach(input => {"))
    assert(!html.contains("tbody.innerHTML = '';"))
    assert(html.contains("<input id=\"bulkDeleteDueDate\" type=\"date\">"))
    assert(html.contains("<button type=\"button\" id=\"applyBulkDeletePlan\">일괄 삭제 계획 등록</button>"))
    assert(html.contains("const BulkDeleteActionPlan = '삭제 처리';"))
    assert(html.contains("const bulkSortKeys = new Set(['action_plan', 'action_due_date']);"))
    assert(html.contains("if (changed && shouldRefreshSort) {"))
    assert(html.contains("function applyDecisionVisibility(row)"))
    assert(html.contains("function applyBulkDeletePlan()"))
    assert(html.contains("function sanitizeResponse(response)"))
    assert(html.contains("data-decision-section=\"false_positive\""))
    assert(html.contains("data-decision-section=\"true_positive\""))
    assert(html.contains("document.querySelectorAll('#findingsTable th[data-sort-key] button').forEach(button => {"))
    assert(!html.contains("<th>Path / Hive</th>"))
    assert(!html.contains("<th>Column / PII</th>"))
    assert(!html.contains("경로 / Hive"))
    assert(!html.contains("컬럼 / PII"))
    assert(!html.contains("사유 / 계획"))
    assert(!html.contains("function buildFindingGroups()"))
    assert(!html.contains("const FINDING_GROUPS = buildFindingGroups();"))
    assert(html.contains("REVIEW_DATA.findings.map((finding, index) =>"))
    assert(html.contains("response.pii_type_pattern = normalizePiiTypePattern(response.pii_type_pattern);"))
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
