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

  test("review renderer resources are available on the runtime classpath") {
    Seq("review/review.html.template", "review/review.js", "review/review_export.bas", "review/vbaProject.bin").foreach { resourceName =>
      assert(
        Option(getClass.getClassLoader.getResource(resourceName)).nonEmpty,
        s"$resourceName should be packaged with application resources"
      )
    }
  }

  test("review workbook export macro creates collector-compatible response JSON") {
    val macroSource = readResource("review/review_export.bas")

    assert(macroSource.contains("Sub say_hello()"))
    assert(macroSource.contains("""InitialFileName:="review.json""""))
    assert(macroSource.contains("""file_identifier_pattern"""))
    assert(macroSource.contains("allowlist_scope"))
    assert(macroSource.contains("recurring"))
    assert(macroSource.contains("CellDateIso"))
    assert(macroSource.contains("""DateAdd("d", 30, Date)"""))
    assert(macroSource.contains("TimeZoneOffsetIso"))
    assert(macroSource.contains("GetTimeZoneInformation"))
    assert(!macroSource.contains("""TimeZoneOffsetIso = "Z""""))
    assert(macroSource.contains("textStream.Position = 3"))
    assert(macroSource.contains("textStream.CopyTo binaryStream"))
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
    assert(html.contains("오탐은 다음 스캔에서 제외하고, 정탐은 제외하지 않고 조치 계획만 남깁니다."))
    assert(html.contains("오탐 사유 예: <code>거래일시 포맷이 운전면허번호 규칙과 충돌</code>"))
    assert(!html.contains("오탐 만료일은 필수이며 만료일이 지난 항목은 다음 스캔에서 다시 검토 대상이 됩니다."))
    assert(html.contains("정탐 조치 계획 예: <code>삭제 처리</code>, <code>컬럼 마스킹</code>."))
    assert(html.contains("정탐 조치 예정일은 오늘부터 30일 이내만 선택할 수 있습니다."))
    assert(!html.contains("오탐 응답은 <code>exact</code> 범위로만 생성합니다."))
    assert(!html.contains("checksum 등 fingerprint metadata가 부족한 row는 exact 오탐으로 수집할 수 없어 오탐 선택을 비활성화합니다."))
    assert(!html.contains("개인정보 유형은 화면에 한글로 표시합니다."))
    assert(!html.contains("개인정보 유형 패턴 <code>운전면허번호</code>"))
    assert(!html.contains("여러 파일 증거가 있는 finding을 <code>pattern</code> 오탐으로 처리할 때는 경로 패턴이 필수입니다."))
    assert(html.contains("<button type=\"button\" id=\"downloadResponse\">응답 파일 생성</button>"))
    assert(html.contains("function formatResponseTimestamp"))
    assert(html.contains("function formatResponseScanPath"))
    assert(html.contains("""replace(/[\\/:*?"<>|]+/g, '-')"""))
    assert(html.contains("""replace(/\s+/g, '-')"""))
    assert(html.contains("link.download = `response-${formatResponseScanPath(REVIEW_DATA.scan_path)}-${formatResponseTimestamp(new Date())}.json`;"))
    assert(html.contains("""<details class="review-guide" open aria-label="검토 안내">"""))
    assert(html.contains("<summary>검토 안내</summary>"))
    assert(!html.contains("<section class=\"review-guide\""))
    assert(html.contains("table-layout: fixed;"))
    assert(html.contains("""<colgroup>"""))
    assert(html.contains("""<col class="col-path">"""))
    assert(html.contains("""<col class="col-existing-action">"""))
    assert(html.contains("""<col class="col-due-date">"""))
    assert(html.contains(".col-path { width: 260px; }"))
    assert(html.contains(".col-sample { width: 280px; }"))
    assert(html.contains("thead th { position: sticky; top: 0; z-index: 10; }"))
    assert(html.contains(".table-wrap { overflow: auto; max-height: 70vh; }"))
    assert(html.contains(".invalid-cell"))
    assert(html.contains("aria-invalid"))
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
    assert(html.contains("""<th scope="col" data-sort-key="non_empty_match_ratio" aria-sort="none"><button type="button" class="sort-button">검출비율(%) <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="sample" aria-sort="none"><button type="button" class="sort-button">검출샘플(검출값/데이터) <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(!html.contains("""<button type="button" class="sort-button">검출 비율 <span class="sort-indicator" aria-hidden="true"></span></button>"""))
    assert(!html.contains("""<button type="button" class="sort-button">검출 샘플 <span class="sort-indicator" aria-hidden="true"></span></button>"""))
    assert(html.contains("""<th scope="col" data-sort-key="decision" aria-sort="none"><button type="button" class="sort-button">판정 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="existing_action_status" aria-sort="none"><button type="button" class="sort-button">기존 조치 상태 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="false_positive_reason" aria-sort="none"><button type="button" class="sort-button">오탐 사유 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(!html.contains("""data-sort-key="file_identifier_pattern""""))
    assert(!html.contains("""data-sort-key="column_name_pattern""""))
    assert(!html.contains("""data-sort-key="pii_type_pattern""""))
    assert(!html.contains("""data-sort-key="expires_at""""))
    assert(!html.contains("오탐 만료일"))
    assert(html.contains("""<th scope="col" data-sort-key="action_plan" aria-sort="none"><button type="button" class="sort-button">정탐 조치 계획 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="action_due_date" aria-sort="none"><button type="button" class="sort-button">조치 예정일 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<span hidden data-finding-key="${escapeHtml(finding.finding_key)}">${escapeHtml(finding.finding_key)}</span>"""))
    assert(html.contains("""<td colspan="13" class="placeholder-cell">"""))
    assert(html.contains(".placeholder-summary { display: block; max-height: 120px; overflow: hidden; }"))
    assert(html.contains("""<td class="metric-cell">${escapeHtml(finding.sampled_row_count)}</td>"""))
    assert(html.contains("""<td class="metric-cell">${escapeHtml(finding.match_count)}</td>"""))
    assert(html.contains("""<td class="metric-cell">${escapeHtml(formatPercent(finding.non_empty_match_ratio))}</td>"""))
    assert(html.contains("function formatPercent(value)"))
    assert(html.contains("return (numeric * 100).toFixed(2);"))
    assert(html.contains("escapeHtml(sample.sample_matched_fragment) + '\\n' +"))
    assert(!html.contains("escapeHtml(sample.sample_matched_fragment) + '\\\\n' +"))
    assert(!html.contains("confidence=${escapeHtml(finding.confidence)}"))
    assert(html.contains("let sortState = { key: null, direction: 'asc' };"))
    assert(html.contains("const formState = new Map();"))
    assert(html.contains("const collator = new Intl.Collator('ko-KR'"))
    assert(html.contains("const hydratedRows = new Map();"))
    assert(html.contains("function renderExistingActionCell(finding)"))
    assert(html.contains("function existingActionSortText(finding)"))
    assert(html.contains("삭제 조치 필요"))
    assert(html.contains("\"action_plan_state\""))
    assert(html.contains("const PiiTypeLabels = {"))
    assert(html.contains("driver_license_number: '운전면허번호'"))
    assert(html.contains("function displayPiiType(value)"))
    assert(html.contains("function defaultFormState()"))
    assert(!html.contains("allowlist_scope: 'exact'"))
    assert(html.contains("const PermanentFalsePositiveExpiresAt = '9999-12-31';"))
    assert(html.contains("expires_at: PermanentFalsePositiveExpiresAt"))
    assert(html.contains("""data-decision-button="false_positive" aria-pressed="false">오탐</button>"""))
    assert(!html.contains("fingerprint metadata가 부족한 finding은 exact 오탐으로 수집할 수 없습니다."))
    assert(!html.contains("response.decision === 'false_positive' && !finding.fingerprint_complete"))
    assert(!html.contains("data-sort-key=\"scope\""))
    assert(!html.contains("data-field=\"allowlist_scope\""))
    assert(!html.contains("data-sort-key=\"file_identifier_pattern\""))
    assert(!html.contains("data-sort-key=\"column_name_pattern\""))
    assert(!html.contains("data-sort-key=\"pii_type_pattern\""))
    assert(!html.contains("data-sort-key=\"expires_at\""))
    assert(!html.contains("data-field=\"file_identifier_pattern\""))
    assert(!html.contains("data-field=\"column_name_pattern\""))
    assert(!html.contains("data-field=\"pii_type_pattern\""))
    assert(!html.contains("data-field=\"expires_at\""))
    assert(!html.contains("const validationErrors = [];"))
    assert(html.contains("const responseValidationErrors = validateResponses(responses);"))
    assert(html.contains("setValidationState(responseValidationErrors);"))
    assert(html.contains("focusFirstValidationError(firstValidationErrorInDisplayOrder(responseValidationErrors));"))
    assert(html.contains("field: 'decision'"))
    assert(html.contains("field: 'false_positive_reason'"))
    assert(!html.contains("field: 'expires_at'"))
    assert(html.contains("field: 'action_plan'"))
    assert(html.contains("field: 'action_due_date'"))
    assert(html.contains("const ActionDueDateWindowDays = 30;"))
    assert(html.contains("function todayDateOnly()"))
    assert(html.contains("function maxActionDueDate()"))
    assert(html.contains("function isActionDueDateWithinWindow(value)"))
    assert(html.contains("applyActionDueDateLimits(document.getElementById('bulkTruePositiveDueDate'));"))
    assert(html.contains("조치 예정일은 오늘부터 30일 이내여야 합니다."))
    assert(!html.contains("""<input data-index="${index}" data-field="expires_at" type="date" aria-label="오탐 만료일" placeholder="YYYY-MM-DD" min="${todayDateOnly()}">"""))
    assert(html.contains("""<input data-index="${index}" data-field="action_due_date" type="date" aria-label="조치 예정일" placeholder="YYYY-MM-DD" min="${todayDateOnly()}" max="${maxActionDueDate()}">"""))
    assert(!html.contains("여러 파일 증거가 있는 pattern 오탐은 경로 패턴이 필요합니다."))
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
    assert(html.contains("<textarea id=\"bulkTruePositivePlan\""))
    assert(html.contains("<input id=\"bulkTruePositiveDueDate\" type=\"date\">"))
    assert(html.contains("<button type=\"button\" id=\"applyBulkTruePositivePlan\">정탐 사유/예정일 일괄 등록</button>"))
    assert(html.contains("<textarea id=\"bulkFalsePositiveReason\""))
    assert(html.contains("<button type=\"button\" id=\"applyBulkFalsePositiveReason\">오탐 사유 일괄 등록</button>"))
    assert(!html.contains("id=\"bulkDeleteDueDate\""))
    assert(!html.contains("id=\"applyBulkDeletePlan\""))
    assert(!html.contains("const BulkDeleteActionPlan = '삭제 처리';"))
    assert(html.contains("const bulkSortKeys = new Set(['action_plan', 'action_due_date']);"))
    assert(html.contains("if (changed && shouldRefreshSort) {"))
    assert(html.contains("function applyDecisionVisibility(row)"))
    assert(html.contains("function applyScopeVisibility(row)"))
    assert(html.contains("function updateDecisionButtons(row, index)"))
    assert(html.contains("function validateResponses(responses)"))
    assert(html.contains("function setValidationState(errors)"))
    assert(html.contains("function applyValidationState(row, index)"))
    assert(html.contains("function focusFirstValidationError(error)"))
    assert(html.contains("function firstValidationErrorInDisplayOrder(errors)"))
    assert(html.contains("function clearValidationField(index, field)"))
    assert(html.contains("scrollTarget.scrollIntoView({ block: 'center', inline: 'center' });"))
    assert(html.contains("target.focus();"))
    assert(!html.contains("target.focus({ preventScroll: true });"))
    assert(html.contains("function applyBulkTruePositivePlan()"))
    assert(html.contains("function applyBulkFalsePositiveReason()"))
    assert(!html.contains("function applyBulkDeletePlan()"))
    assert(html.contains("function sanitizeResponse(response)"))
    assert(html.contains("data-decision-section=\"false_positive\""))
    assert(html.contains("data-decision-section=\"true_positive\""))
    assert(html.contains("data-decision-button=\"false_positive\""))
    assert(html.contains("data-decision-button=\"true_positive\""))
    assert(html.contains("data-validation-field=\"decision\""))
    assert(html.contains("""data-decision-button="false_positive" aria-pressed="false">오탐</button>"""))
    assert(!html.contains("""data-field="decision""""))
    assert(!html.contains("""<select data-index="${index}" data-field="decision">"""))
    assert(!html.contains("data-scope-section=\"pattern\""))
    assert(html.contains("document.querySelectorAll('#findingsTable th[data-sort-key] button').forEach(button => {"))
    assert(!html.contains("<th>Path / Hive</th>"))
    assert(!html.contains("<th>Column / PII</th>"))
    assert(!html.contains("경로 / Hive"))
    assert(!html.contains("컬럼 / PII"))
    assert(!html.contains("사유 / 계획"))
    assert(!html.contains("function buildFindingGroups()"))
    assert(!html.contains("const FINDING_GROUPS = buildFindingGroups();"))
    assert(html.contains("REVIEW_DATA.findings.map((finding, index) =>"))
    assert(!html.contains("response.pii_type_pattern = normalizePiiTypePattern(response.pii_type_pattern);"))
    assert(!html.contains("sample.file_identifier,"))
    assert(!html.contains("escapeHtml(sample.file_identifier) +"))
    assert(!html.contains(".finding-summary"))
    assert(!html.contains("""return `<div class="finding-summary">"""))
    assert(!html.contains("finding.column_name,\n        displayPiiType(finding.pii_type),\n        sample.sample_matched_fragment"))
    assert(html.contains("return samples;"))
    assert(!html.contains("case 'file_identifier_pattern':"))
    assert(!html.contains("case 'column_name_pattern':"))
    assert(!html.contains("case 'pii_type_pattern':"))
    assert(!html.contains("case 'expires_at':"))
    assert(!html.contains("class=\"pattern-cell\""))
    assert(!html.contains("http://"))
    assert(!html.contains("https://"))
  }

  test("offline review response JSON viewer is self-contained and loads local response files") {
    val html = readText("samples/offline-review/review-response-viewer.html")

    assert(html.contains("<!doctype html>"))
    assert(html.contains("type=\"file\""))
    assert(html.contains("accept=\"application/json,.json\""))
    assert(html.contains("id=\"dropZone\""))
    assert(html.contains("function loadResponseFile(file)"))
    assert(html.contains("addEventListener('dragover'"))
    assert(html.contains("addEventListener('dragleave'"))
    assert(html.contains("addEventListener('drop'"))
    assert(html.contains("event.dataTransfer.files"))
    assert(html.contains("FileReader"))
    assert(html.contains("JSON.parse"))
    assert(html.contains("schema_version"))
    assert(!html.contains("scan_results_fingerprint 값이 필요합니다."))
    assert(html.contains("response-스캔경로-YYYYMMDD-HHMMSS.json"))
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
    assert(!html.contains("pattern allowlist에는 pattern 필드가 하나 이상 필요합니다."))
    assert(!html.contains("다중 파일 finding은 collector에서 file_identifier_pattern을 요구할 수 있습니다."))
    assert(html.contains("file_identifier_pattern 값이 필요합니다."))
    assert(!html.contains("pii_type_pattern=* 값은 허용되지 않습니다."))
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

  private def readResource(name: String): String = {
    val stream = getClass.getClassLoader.getResourceAsStream(name)
    assert(stream != null, s"$name should exist on the runtime classpath")
    try {
      scala.io.Source.fromInputStream(stream, StandardCharsets.UTF_8.name()).mkString
    } finally {
      stream.close()
    }
  }
}
