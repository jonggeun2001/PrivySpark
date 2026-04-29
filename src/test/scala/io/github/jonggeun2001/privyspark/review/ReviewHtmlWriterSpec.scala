package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.model.ScanResult
import org.apache.hadoop.conf.Configuration
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.charset.StandardCharsets
import java.nio.file.Files

@RunWith(classOf[JUnitRunner])
class ReviewHtmlWriterSpec extends AnyFunSuite {
  test("write creates only review.html under the scan output review directory and includes masked samples") {
    val outputRoot = Files.createTempDirectory("privyspark-review-html-")
    val result = ScanResult(
      dataset_path = "/data/project",
      scan_timestamp = "2026-04-27T10:00:00Z",
      file_identifier = "customers/part-000.parquet",
      column_name = "email",
      pii_type = "email",
      match_count = 3L,
      sampled_row_count = 10L,
      match_ratio = 0.3,
      non_empty_match_ratio = 0.3,
      confidence = 0.1,
      sample_raw_value = "owner=alice@example.com",
      sample_matched_fragment = "alice@example.com",
      file_size = 128L,
      file_mtime_epoch_ms = 1710000000000L,
      hive_table_fqn = "mart.customers"
    )

    ReviewHtmlWriter.write(
      new Configuration(),
      outputRoot.toString,
      "/data/project",
      Seq(result),
      sampleMode = "masked"
    )

    val reviewDir = outputRoot.resolve("review")
    val htmlPath = reviewDir.resolve("review.html")
    val html = new String(Files.readAllBytes(htmlPath), StandardCharsets.UTF_8)

    assert(Files.exists(htmlPath))
    assert(!Files.exists(reviewDir.resolve("responses")))
    assert(!Files.exists(reviewDir.resolve("state")))
    assert(html.contains("mart"))
    assert(html.contains("customers"))
    assert(html.contains("email"))
    assert(html.contains("finding_key"))
    assert(html.contains("responses"))
    assert(html.contains("id=\"responder\""))
    assert(html.contains("<button type=\"button\" id=\"downloadResponse\">응답 파일 생성</button>"))
    assert(html.contains("function formatResponseTimestamp"))
    assert(html.contains("link.download = `response-${formatResponseTimestamp(new Date())}.json`;"))
    assert(html.contains("""<details class="review-guide" open aria-label="검토 안내">"""))
    assert(html.contains("<summary>검토 안내</summary>"))
    assert(!html.contains("<section class=\"review-guide\""))
    assert(html.contains("thead th { position: sticky; top: 0; z-index: 10; }"))
    assert(html.contains(".table-wrap { overflow: auto; max-height: 70vh; }"))
    assert(html.contains(".invalid-cell"))
    assert(html.contains("aria-invalid"))
    assert(!html.contains("response.json 다운로드"))
    assert(!html.contains("privyspark-response.json 다운로드"))
    assert(!html.contains("link.download = 'privyspark-response.json'"))
    assert(!html.contains("placeholder=\"owner@example.com\""))
    assert(html.contains("""<th scope="col" data-sort-key="path" aria-sort="none"><button type="button" class="sort-button">경로 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="hive" aria-sort="none"><button type="button" class="sort-button">Hive 테이블 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="column" aria-sort="none"><button type="button" class="sort-button">컬럼명 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="pii" aria-sort="none"><button type="button" class="sort-button">개인정보 유형 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="sampled_row_count" aria-sort="none"><button type="button" class="sort-button">샘플 행 수 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="match_count" aria-sort="none"><button type="button" class="sort-button">검출 건수 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="non_empty_match_ratio" aria-sort="none"><button type="button" class="sort-button">검출 비율 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="sample" aria-sort="none"><button type="button" class="sort-button">검출 샘플 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="decision" aria-sort="none"><button type="button" class="sort-button">판정 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="false_positive_reason" aria-sort="none"><button type="button" class="sort-button">오탐 사유 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(!html.contains("""data-sort-key="file_identifier_pattern""""))
    assert(!html.contains("""data-sort-key="column_name_pattern""""))
    assert(!html.contains("""data-sort-key="pii_type_pattern""""))
    assert(!html.contains("""data-sort-key="expires_at""""))
    assert(html.contains("""<th scope="col" data-sort-key="action_plan" aria-sort="none"><button type="button" class="sort-button">정탐 조치 계획 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<th scope="col" data-sort-key="action_due_date" aria-sort="none"><button type="button" class="sort-button">조치 예정일 <span class="sort-indicator" aria-hidden="true"></span></button></th>"""))
    assert(html.contains("""<span hidden data-finding-key="${escapeHtml(finding.finding_key)}">${escapeHtml(finding.finding_key)}</span>"""))
    assert(html.contains("""<td colspan="12" class="placeholder-cell">"""))
    assert(html.contains("""<td class="metric-cell">${escapeHtml(finding.sampled_row_count)}</td>"""))
    assert(html.contains("""<td class="metric-cell">${escapeHtml(finding.match_count)}</td>"""))
    assert(html.contains("""<td class="metric-cell">${escapeHtml(finding.non_empty_match_ratio)}</td>"""))
    assert(!html.contains("confidence=${escapeHtml(finding.confidence)}"))
    assert(!html.contains("<small>${escapeHtml(finding.finding_key)}</small>"))
    assert(html.contains("let sortState = { key: null, direction: 'asc' };"))
    assert(html.contains("const formState = new Map();"))
    assert(html.contains("const collator = new Intl.Collator('ko-KR'"))
    assert(html.contains("const hydratedRows = new Map();"))
    assert(html.contains("const PiiTypeLabels = {"))
    assert(html.contains("driver_license_number: '운전면허번호'"))
    assert(html.contains("function displayPiiType(value)"))
    assert(html.contains("function defaultFormState()"))
    assert(html.contains("REVIEW_DATA.findings.forEach((finding, index) =>"))
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
    assert(html.contains("""${finding.fingerprint_complete ? '' : ' disabled'}>오탐</button>"""))
    assert(!html.contains("""data-field="decision""""))
    assert(!html.contains("""<select data-index="$${index}" data-field="decision">"""))
    assert(html.contains("fingerprint metadata가 부족한 finding은 exact 오탐으로 수집할 수 없습니다."))
    assert(html.contains("response.decision === 'false_positive' && !finding.fingerprint_complete"))
    assert(html.contains("document.querySelectorAll('#findingsTable th[data-sort-key] button').forEach(button => {"))
    assert(!html.contains("<th>Path / Hive</th>"))
    assert(!html.contains("<th>Column / PII</th>"))
    assert(!html.contains("경로 / Hive"))
    assert(!html.contains("컬럼 / PII"))
    assert(!html.contains("사유 / 계획"))
    assert(!html.contains("function buildFindingGroups()"))
    assert(!html.contains("const FINDING_GROUPS = buildFindingGroups();"))
    assert(html.contains("REVIEW_DATA.findings.map((finding, index) =>"))
    assert(html.contains("allowlist_scope: 'exact'"))
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
    assert(!html.contains("sample.file_identifier,"))
    assert(!html.contains("escapeHtml(sample.file_identifier) +"))
    assert(!html.contains(".finding-summary"))
    assert(!html.contains("""return `<div class="finding-summary">"""))
    assert(!html.contains("finding.column_name,\n        displayPiiType(finding.pii_type),\n        sample.sample_matched_fragment"))
    assert(html.contains("return samples;"))
    assert(!html.contains("response.pii_type_pattern = normalizePiiTypePattern(response.pii_type_pattern);"))
    assert(!html.contains("case 'file_identifier_pattern':"))
    assert(!html.contains("case 'column_name_pattern':"))
    assert(!html.contains("case 'pii_type_pattern':"))
    assert(!html.contains("case 'expires_at':"))
    assert(!html.contains("class=\"pattern-cell\""))
    assert(html.contains("검토 안내"))
    assert(html.contains("오탐 응답은 <code>exact</code> 범위로만 생성합니다."))
    assert(!html.contains("개인정보 유형 패턴 <code>운전면허번호</code>"))
    assert(!html.contains("개인정보 유형 패턴은 한글명 또는 원본 pii_type 값 모두 입력할 수 있습니다."))
    assert(!html.contains("여러 파일 증거가 있는 finding을 <code>pattern</code> 오탐으로 처리할 때는 경로 패턴이 필수입니다."))
    assert(html.contains("const validationErrors = [];"))
    assert(html.contains("const responseValidationErrors = validateResponses(responses);"))
    assert(html.contains("setValidationState(responseValidationErrors);"))
    assert(html.contains("focusFirstValidationError(firstValidationErrorInDisplayOrder(responseValidationErrors));"))
    assert(html.contains("field: 'decision'"))
    assert(html.contains("field: 'false_positive_reason'"))
    assert(html.contains("field: 'action_plan'"))
    assert(html.contains("field: 'action_due_date'"))
    assert(html.contains("""<input data-index="${index}" data-field="action_due_date" type="date" aria-label="조치 예정일" placeholder="YYYY-MM-DD">"""))
    assert(!html.contains("여러 파일 증거가 있는 pattern 오탐은 경로 패턴이 필요합니다."))
    assert(!html.contains("alice@example.com"))
    assert(html.contains("a***e@example.com"))
  }

  test("write can place review.html under a configured directory outside scan output") {
    val outputRoot = Files.createTempDirectory("privyspark-review-html-output-")
    val customRoot = Files.createTempDirectory("privyspark-review-html-custom-")
    val customHtmlPath = customRoot.resolve("review.html")
    val result = ScanResult(
      dataset_path = "/data/project",
      scan_timestamp = "2026-04-27T10:00:00Z",
      file_identifier = "customers/part-000.parquet",
      column_name = "email",
      pii_type = "email",
      match_count = 1L,
      sampled_row_count = 2L,
      match_ratio = 0.5,
      non_empty_match_ratio = 0.5,
      confidence = 0.2,
      sample_raw_value = "owner=bob@example.com",
      sample_matched_fragment = "bob@example.com",
      file_size = 128L,
      file_mtime_epoch_ms = 1710000000000L
    )

    ReviewHtmlWriter.write(
      new Configuration(),
      outputRoot.toString,
      "/data/project",
      Seq(result),
      sampleMode = "none",
      reviewHtmlDir = Some(customRoot.toString)
    )

    val defaultHtmlPath = outputRoot.resolve("review").resolve("review.html")
    val html = new String(Files.readAllBytes(customHtmlPath), StandardCharsets.UTF_8)

    assert(Files.exists(customHtmlPath))
    assert(!Files.exists(defaultHtmlPath))
    assert(html.contains("PrivySpark Review"))
    assert(!html.contains("bob@example.com"))
  }

  test("write fixes false-positive allowlist scope to exact without rendering a scope column") {
    val outputRoot = Files.createTempDirectory("privyspark-review-scope-hint-")
    val fingerprints = ReviewScopeFingerprintCodec.encode(Seq(
      RecordedFileFingerprint(
        fileIdentifier = "customers/part-000.parquet",
        fileSize = 128L,
        fileMtimeEpochMs = 1710000000000L,
        fileChecksumAlgo = "sha256",
        fileChecksum = "aaa"
      ),
      RecordedFileFingerprint(
        fileIdentifier = "customers/part-001.parquet",
        fileSize = 256L,
        fileMtimeEpochMs = 1710000001000L,
        fileChecksumAlgo = "sha256",
        fileChecksum = "bbb"
      )
    ))
    val result = ScanResult(
      dataset_path = "/data/project",
      scan_timestamp = "2026-04-27T10:00:00Z",
      file_identifier = "customers",
      column_name = "temp_driver_no",
      pii_type = "driver_license_number",
      match_count = 2L,
      sampled_row_count = 10L,
      match_ratio = 0.2,
      non_empty_match_ratio = 0.2,
      confidence = 0.1,
      sample_raw_value = "991231-1234567",
      sample_matched_fragment = "991231-1234567",
      file_size = 128L,
      file_mtime_epoch_ms = 1710000000000L,
      review_scope_file_fingerprints = fingerprints
    )

    ReviewHtmlWriter.write(
      new Configuration(),
      outputRoot.toString,
      "/data/project",
      Seq(result),
      sampleMode = "masked"
    )

    val htmlPath = outputRoot.resolve("review").resolve("review.html")
    val html = new String(Files.readAllBytes(htmlPath), StandardCharsets.UTF_8)

    assert(html.contains("\"fingerprint_complete\":true"))
    assert(html.contains("\"has_multiple_file_evidence\":true"))
    assert(html.contains("검토 안내"))
    assert(html.contains("오탐 응답은 <code>exact</code> 범위로만 생성합니다."))
    assert(html.contains("allowlist_scope: 'exact'"))
    assert(html.contains("""${finding.fingerprint_complete ? '' : ' disabled'}>오탐</button>"""))
    assert(html.contains("data-decision-button=\"false_positive\""))
    assert(!html.contains("""data-field="decision""""))
    assert(html.contains("fingerprint metadata가 부족한 finding은 exact 오탐으로 수집할 수 없습니다."))
    assert(!html.contains("""<th scope="col" data-sort-key="scope""""))
    assert(!html.contains("class=\"scope-cell\""))
    assert(!html.contains("data-field=\"allowlist_scope\""))
    assert(!html.contains("<option value=\"pattern\">pattern</option>"))
    assert(!html.contains("data-field=\"file_identifier_pattern\""))
    assert(!html.contains("class=\"hint\""))
  }
}
