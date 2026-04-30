package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.model.ScanResult
import io.github.jonggeun2001.privyspark.report.JsonCodec.jsonString
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.util.Locale

private[privyspark] object ReviewHtmlWriter {
  val DefaultSampleMode = "masked"
  val SupportedSampleModes: Set[String] = Set("raw", "masked", "none")

  private val ResponseScanPathScript =
    """    function formatResponseScanPath(scanPath) {
      const safePath = String(scanPath || 'scan')
        .trim()
        .replace(/[\\/:*?"<>|]+/g, '-')
        .replace(/\s+/g, '-')
        .replace(/-+/g, '-')
        .replace(/^-+|-+$/g, '');
      return safePath || 'scan';
    }
"""

  def normalizeSampleMode(value: String): Option[String] = {
    val normalized = Option(value).map(_.trim.toLowerCase(Locale.ROOT)).getOrElse("")
    if (SupportedSampleModes.contains(normalized)) Some(normalized) else None
  }

  def write(
    conf: Configuration,
    outputRoot: String,
    scanPath: String,
    results: Seq[ScanResult],
    sampleMode: String
  ): Unit =
    write(conf, outputRoot, scanPath, results, sampleMode, None)

  def write(
    conf: Configuration,
    outputRoot: String,
    scanPath: String,
    results: Seq[ScanResult],
    sampleMode: String,
    reviewHtmlDir: Option[String]
  ): Unit = {
    val normalizedSampleMode = normalizeSampleMode(sampleMode).getOrElse(DefaultSampleMode)
    val findings = ReviewFindingBuilder.fromScanResultsIterator(
      results.iterator,
      ReviewFindingBuilder.DefaultMaxEvidenceSamples
    )
    writeFindings(conf, outputRoot, scanPath, findings, normalizedSampleMode, reviewHtmlDir)
  }

  def write(
    conf: Configuration,
    outputRoot: String,
    scanPath: String,
    resultDf: DataFrame,
    sampleMode: String
  ): Unit =
    write(conf, outputRoot, scanPath, resultDf, sampleMode, None)

  def write(
    conf: Configuration,
    outputRoot: String,
    scanPath: String,
    resultDf: DataFrame,
    sampleMode: String,
    reviewHtmlDir: Option[String]
  ): Unit = {
    val normalizedSampleMode = normalizeSampleMode(sampleMode).getOrElse(DefaultSampleMode)
    val findings = ReviewFindingBuilder.fromScanResultsIterator(
      ScanResultsReader.iterateScanResults(resultDf, ordered = true),
      ReviewFindingBuilder.DefaultMaxEvidenceSamples
    )
    writeFindings(conf, outputRoot, scanPath, findings, normalizedSampleMode, reviewHtmlDir)
  }

  private def writeFindings(
    conf: Configuration,
    outputRoot: String,
    scanPath: String,
    findings: Seq[ReviewFinding],
    sampleMode: String,
    reviewHtmlDir: Option[String]
  ): Unit = {
    val scanResultsFingerprint = ReviewFindingBuilder.scanResultsFingerprint(findings)
    val html = renderHtml(scanPath, scanResultsFingerprint, findings, sampleMode)
    val htmlPath = resolveHtmlPath(outputRoot, reviewHtmlDir)
    val fs = htmlPath.getFileSystem(conf)
    Option(htmlPath.getParent).foreach(fs.mkdirs)
    val writer = new BufferedWriter(new OutputStreamWriter(fs.create(htmlPath, true), StandardCharsets.UTF_8))
    try {
      writer.write(html)
    } finally {
      writer.close()
    }
  }

  private def resolveHtmlPath(outputRoot: String, reviewHtmlDir: Option[String]): Path =
    reviewHtmlDir
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(directory => new Path(new Path(directory), "review.html"))
      .getOrElse(new Path(new Path(outputRoot), "review/review.html"))

  private def renderHtml(
    scanPath: String,
    scanResultsFingerprint: String,
    findings: Seq[ReviewFinding],
    sampleMode: String
  ): String = {
    val findingJson = findings.map(findingToJson(_, sampleMode)).mkString("[", ",", "]")
    val reviewData =
      s"""{"schema_version":1,"scan_path":${jsonString(scanPath)},"scan_results_fingerprint":${jsonString(scanResultsFingerprint)},"findings":$findingJson}"""
    val safeReviewData = reviewData.replace("</", "<\\/")
    s"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <title>PrivySpark Review</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px; color: #17202a; }
    table { border-collapse: collapse; table-layout: fixed; width: 100%; min-width: 2340px; }
    col { box-sizing: border-box; }
    .col-path { width: 260px; }
    .col-hive { width: 180px; }
    .col-column { width: 180px; }
    .col-pii { width: 140px; }
    .col-sampled { width: 110px; }
    .col-match { width: 110px; }
    .col-ratio { width: 110px; }
    .col-sample { width: 280px; }
    .col-decision { width: 130px; }
    .col-reason { width: 240px; }
    .col-fp-expires { width: 180px; }
    .col-plan { width: 240px; }
    .col-due-date { width: 180px; }
    th, td { border: 1px solid #d5d8dc; padding: 8px; vertical-align: top; overflow-wrap: anywhere; }
    th { background: #f4f6f7; text-align: left; }
    thead th { position: sticky; top: 0; z-index: 10; }
    .sort-button { all: unset; cursor: pointer; display: inline-flex; align-items: center; gap: 4px; font-weight: 600; }
    .sort-button:focus-visible { outline: 2px solid #1f6feb; outline-offset: 2px; }
    .sort-indicator { min-width: 1em; }
    textarea, input, select { width: 100%; box-sizing: border-box; }
    .table-wrap { overflow: auto; max-height: 70vh; }
    .sample { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; white-space: pre-wrap; overflow-wrap: anywhere; }
    .field { display: block; margin-bottom: 8px; }
    .field > span { display: block; font-weight: 600; margin-bottom: 4px; }
    .decision-toggle { display: flex; gap: 6px; flex-wrap: wrap; }
    .decision-button { border: 1px solid #aeb6bf; background: #fff; border-radius: 4px; cursor: pointer; font-weight: 600; padding: 6px 10px; }
    .decision-button[aria-pressed="true"] { background: #1f6feb; border-color: #1f6feb; color: #fff; }
    .decision-button:focus-visible { outline: 2px solid #1f6feb; outline-offset: 2px; }
    .decision-button:disabled { background: #f4f6f7; color: #85929e; cursor: not-allowed; }
    .invalid-cell { background: #fff5f5; box-shadow: inset 0 0 0 2px #d93025; }
    .invalid-cell input, .invalid-cell textarea, .decision-toggle[aria-invalid="true"] .decision-button { border-color: #d93025; }
    .invalid-cell input:focus, .invalid-cell textarea:focus, .decision-toggle[aria-invalid="true"] .decision-button:focus { outline: 2px solid #d93025; outline-offset: 2px; }
    .review-guide { background: #f8fafc; border: 1px solid #d5d8dc; padding: 12px 16px; margin: 16px 0; line-height: 1.5; }
    .review-guide summary { cursor: pointer; font-weight: 700; }
    .review-guide ul { margin: 8px 0 0; padding-left: 20px; }
    .review-guide code { background: #eef2f7; padding: 1px 4px; border-radius: 3px; }
    .metric-cell { text-align: right; white-space: nowrap; }
    .date-cell { min-width: 180px; }
    .reason-cell, .plan-cell { min-width: 220px; }
    .placeholder-cell { min-height: 120px; color: #566573; background: #fbfcfc; }
    .placeholder-summary { display: block; max-height: 120px; overflow: hidden; }
    .bulk-actions { display: flex; gap: 12px; align-items: end; flex-wrap: wrap; margin: 16px 0; }
    .bulk-actions label { display: inline-flex; flex-direction: column; gap: 4px; font-weight: 600; }
    .bulk-actions textarea { min-height: 3em; min-width: 240px; }
    .decision-fields[hidden], [data-decision-section][hidden] { display: none; }
  </style>
</head>
<body>
  <h1>PrivySpark Review</h1>
  <p>Scan path: <code id="scanPath"></code></p>
  <p><label>응답자 <input id="responder"></label></p>
  <div class="bulk-actions">
    <label>정탐 사유/계획 <textarea id="bulkTruePositivePlan" rows="2"></textarea></label>
    <label>조치 예정일 <input id="bulkTruePositiveDueDate" type="date"></label>
    <button type="button" id="applyBulkTruePositivePlan">정탐 사유/예정일 일괄 등록</button>
    <label>오탐 사유 <textarea id="bulkFalsePositiveReason" rows="2"></textarea></label>
    <button type="button" id="applyBulkFalsePositiveReason">오탐 사유 일괄 등록</button>
  </div>
  <details class="review-guide" open aria-label="검토 안내">
    <summary>검토 안내</summary>
    <ul>
      <li>오탐은 다음 스캔에서 제외하고, 정탐은 제외하지 않고 조치 계획만 남깁니다.</li>
      <li>오탐은 Hive 테이블이 있으면 테이블/컬럼/개인정보 유형 기준, Hive 테이블이 없으면 경로/파일 식별자/컬럼/개인정보 유형 기준으로 반복 제외합니다.</li>
      <li>오탐 만료일은 필수이며 만료일이 지난 항목은 다음 스캔에서 다시 검토 대상이 됩니다.</li>
      <li>오탐 사유 예: <code>거래일시 포맷이 운전면허번호 규칙과 충돌</code>, <code>테스트 데이터의 더미 식별자</code>, <code>마스킹된 샘플 값 오인식</code>.</li>
      <li>정탐 조치 계획 예: <code>삭제 처리</code>, <code>컬럼 마스킹</code>.</li>
      <li>정탐 조치 예정일은 오늘부터 30일 이내만 선택할 수 있습니다.</li>
    </ul>
  </details>
  <div class="table-wrap">
  <table id="findingsTable">
    <colgroup>
      <col class="col-path">
      <col class="col-hive">
      <col class="col-column">
      <col class="col-pii">
      <col class="col-sampled">
      <col class="col-match">
      <col class="col-ratio">
      <col class="col-sample">
      <col class="col-decision">
      <col class="col-reason">
      <col class="col-fp-expires">
      <col class="col-plan">
      <col class="col-due-date">
    </colgroup>
    <thead>
      <tr>
        <th scope="col" data-sort-key="path" aria-sort="none"><button type="button" class="sort-button">경로 <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="hive" aria-sort="none"><button type="button" class="sort-button">Hive 테이블 <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="column" aria-sort="none"><button type="button" class="sort-button">컬럼명 <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="pii" aria-sort="none"><button type="button" class="sort-button">개인정보 유형 <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="sampled_row_count" aria-sort="none"><button type="button" class="sort-button">샘플 행 수 <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="match_count" aria-sort="none"><button type="button" class="sort-button">검출 건수 <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="non_empty_match_ratio" aria-sort="none"><button type="button" class="sort-button">검출 비율 <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="sample" aria-sort="none"><button type="button" class="sort-button">검출 샘플 <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="decision" aria-sort="none"><button type="button" class="sort-button">판정 <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="false_positive_reason" aria-sort="none"><button type="button" class="sort-button">오탐 사유 <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="expires_at" aria-sort="none"><button type="button" class="sort-button">오탐 만료일 <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="action_plan" aria-sort="none"><button type="button" class="sort-button">정탐 조치 계획 <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="action_due_date" aria-sort="none"><button type="button" class="sort-button">조치 예정일 <span class="sort-indicator" aria-hidden="true"></span></button></th>
      </tr>
    </thead>
    <tbody></tbody>
  </table>
  </div>
  <p><button type="button" id="downloadResponse">응답 파일 생성</button></p>
  <script>
    const REVIEW_DATA = $safeReviewData;
    document.getElementById('scanPath').textContent = REVIEW_DATA.scan_path;
    const tbody = document.querySelector('#findingsTable tbody');
    const escapeHtml = value => String(value ?? '').replace(/[&<>"']/g, ch => ({
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      '"': '&quot;',
      "'": '&#39;'
    }[ch]));
    const PiiTypeLabels = {
      phone_number: '전화번호',
      email: '이메일',
      resident_registration_number: '주민등록번호',
      foreign_registration_number: '외국인등록번호',
      driver_license_number: '운전면허번호',
      address: '주소',
      bank_account_number: '계좌번호',
      credit_card_number: '신용카드번호',
      passport_number: '여권번호',
      ip_address: 'IP 주소'
    };
    function displayPiiType(value) {
      return PiiTypeLabels[value] || value;
    }
    function formatResponseTimestamp(date) {
      const pad = value => String(value).padStart(2, '0');
      return String(date.getFullYear()) +
        pad(date.getMonth() + 1) +
        pad(date.getDate()) +
        '-' +
        pad(date.getHours()) +
        pad(date.getMinutes()) +
        pad(date.getSeconds());
    }
${ResponseScanPathScript}
    const FormFieldDefaults = {
      decision: '',
      false_positive_reason: '',
      expires_at: '',
      action_plan: '',
      action_due_date: ''
    };
    const FormFieldNames = Object.keys(FormFieldDefaults);
    const formState = new Map();
    const hydratedRows = new Map();
    const validationState = new Map();
    const collator = new Intl.Collator('ko-KR', { numeric: true, sensitivity: 'base' });
    const ActionDueDateWindowDays = 30;
    function dateOnlyFromLocal(date) {
      const pad = value => String(value).padStart(2, '0');
      return String(date.getFullYear()) + '-' + pad(date.getMonth() + 1) + '-' + pad(date.getDate());
    }
    function addLocalDays(date, days) {
      const next = new Date(date.getFullYear(), date.getMonth(), date.getDate());
      next.setDate(next.getDate() + days);
      return next;
    }
    function todayDateOnly() {
      return dateOnlyFromLocal(new Date());
    }
    function maxActionDueDate() {
      return dateOnlyFromLocal(addLocalDays(new Date(), ActionDueDateWindowDays));
    }
    function isDateOnly(value) {
      if (!/^[0-9]{4}-[0-9]{2}-[0-9]{2}$$/.test(value)) {
        return false;
      }
      const parts = value.split('-').map(part => Number(part));
      const date = new Date(parts[0], parts[1] - 1, parts[2]);
      return date.getFullYear() === parts[0] &&
        date.getMonth() === parts[1] - 1 &&
        date.getDate() === parts[2];
    }
    function isActionDueDateWithinWindow(value) {
      if (!isDateOnly(value)) {
        return false;
      }
      return value >= todayDateOnly() && value <= maxActionDueDate();
    }
    function applyActionDueDateLimits(input) {
      if (!input) {
        return;
      }
      input.min = todayDateOnly();
      input.max = maxActionDueDate();
    }
    applyActionDueDateLimits(document.getElementById('bulkTruePositiveDueDate'));
    function defaultFormState() {
      return Object.assign({}, FormFieldDefaults);
    }
    REVIEW_DATA.findings.forEach((finding, index) => {
      formState.set(index, defaultFormState());
    });
    let sortState = { key: null, direction: 'asc' };
    let rowObserver = null;
    function getFormState(index) {
      const numericIndex = Number(index);
      if (!formState.has(numericIndex)) {
        formState.set(numericIndex, defaultFormState());
      }
      return formState.get(numericIndex);
    }
    function updateFormState(index, field, value) {
      if (!FormFieldNames.includes(field)) {
        return;
      }
      getFormState(index)[field] = value;
    }
    function isBlank(value) {
      return String(value ?? '').trim() === '';
    }
    function formValuesSnapshot() {
      const values = {};
      formState.forEach((state, index) => {
        values[index] = Object.assign({}, state);
      });
      return values;
    }
    function collectFormValues() {
      return formValuesSnapshot();
    }
    function sampleSortText(finding) {
      return finding.evidence_samples.map(sample => [
        sample.sample_matched_fragment,
        sample.sample_raw_value
      ].join(' ')).join(' ');
    }
    function formSortText(index, fields) {
      const rowValues = getFormState(index);
      return fields.map(field => rowValues[field] || '').join(' ');
    }
    function getSortValue(index) {
      const finding = REVIEW_DATA.findings[index];
      switch (sortState.key) {
        case 'path':
          return finding.file_identifier;
        case 'hive':
          return finding.hive_table_fqn;
        case 'column':
          return finding.column_name;
        case 'pii':
          return displayPiiType(finding.pii_type);
        case 'sampled_row_count':
          return Number(finding.sampled_row_count) || 0;
        case 'match_count':
          return Number(finding.match_count) || 0;
        case 'non_empty_match_ratio':
          return Number(finding.non_empty_match_ratio) || 0;
        case 'sample':
          return sampleSortText(finding);
        case 'decision':
          return formSortText(index, ['decision']);
        case 'false_positive_reason':
        case 'expires_at':
        case 'action_plan':
        case 'action_due_date':
          return formSortText(index, [sortState.key]);
        default:
          return index;
      }
    }
    function compareSortValues(left, right) {
      if (Array.isArray(left) || Array.isArray(right)) {
        const leftArray = Array.isArray(left) ? left : [left];
        const rightArray = Array.isArray(right) ? right : [right];
        const length = Math.max(leftArray.length, rightArray.length);
        for (let i = 0; i < length; i += 1) {
          const result = compareSortValues(leftArray[i] ?? '', rightArray[i] ?? '');
          if (result !== 0) {
            return result;
          }
        }
        return 0;
      }
      if (typeof left === 'number' && typeof right === 'number') {
        return left - right;
      }
      return collator.compare(String(left ?? ''), String(right ?? ''));
    }
    function sortRows(rows) {
      if (!sortState.key) {
        return rows.slice();
      }
      const direction = sortState.direction === 'desc' ? -1 : 1;
      const sortKeys = new Map(rows.map(index => [index, getSortValue(index)]));
      return rows.slice().sort((left, right) => {
        const result = compareSortValues(sortKeys.get(left), sortKeys.get(right));
        return result === 0 ? left - right : result * direction;
      });
    }
    function setFieldValues(row, index) {
      Object.entries(getFormState(index)).forEach(([field, value]) => {
        const input = row.querySelector('[data-field="' + field + '"]');
        if (input) {
          input.value = value;
        }
      });
      updateDecisionButtons(row, index);
    }
    function updateDecisionButtons(row, index) {
      const decision = getFormState(index).decision;
      row.querySelectorAll('[data-decision-button]').forEach(button => {
        button.setAttribute(
          'aria-pressed',
          String(button.getAttribute('data-decision-button') === decision)
        );
      });
    }
    function applyDecisionVisibility(row) {
      if (!row) {
        return;
      }
      const decision = getFormState(Number(row.getAttribute('data-index'))).decision;
      row.querySelectorAll('[data-decision-section]').forEach(section => {
        section.hidden = section.getAttribute('data-decision-section') !== decision;
      });
    }
    function applyScopeVisibility(row) {
      return row;
    }
    function validationTarget(row, field) {
      return field === 'decision'
        ? row.querySelector('[data-validation-field="decision"]')
        : row.querySelector('[data-field="' + field + '"]');
    }
    function validationFocusTarget(row, field) {
      if (field === 'decision') {
        return row.querySelector('[data-decision-button]:not(:disabled)') || row.querySelector('[data-decision-button]');
      }
      return row.querySelector('[data-field="' + field + '"]');
    }
    function applyValidationState(row, index) {
      if (!row) {
        return;
      }
      row.querySelectorAll('.invalid-cell').forEach(cell => cell.classList.remove('invalid-cell'));
      row.querySelectorAll('[aria-invalid="true"]').forEach(field => field.setAttribute('aria-invalid', 'false'));
      const fields = validationState.get(Number(index));
      if (!fields) {
        return;
      }
      fields.forEach(field => {
        const target = validationTarget(row, field);
        if (!target) {
          return;
        }
        target.setAttribute('aria-invalid', 'true');
        const cell = target.closest('td') || target;
        cell.classList.add('invalid-cell');
      });
    }
    function setValidationState(errors) {
      validationState.clear();
      errors.forEach(error => {
        const fields = validationState.get(error.index) || new Set();
        fields.add(error.field);
        validationState.set(error.index, fields);
      });
      hydratedRows.forEach((row, index) => applyValidationState(row, index));
    }
    function clearValidationField(index, field) {
      const numericIndex = Number(index);
      const fields = validationState.get(numericIndex);
      if (!fields) {
        return;
      }
      fields.delete(field);
      if (fields.size === 0) {
        validationState.delete(numericIndex);
      }
      applyValidationState(hydratedRows.get(numericIndex), numericIndex);
    }
    function validateResponses(responses) {
      const errors = [];
      responses.forEach((response, index) => {
        if (!response.decision) {
          errors.push({ index, field: 'decision', message: '판정을 선택하세요.' });
        } else if (response.decision === 'false_positive') {
          if (isBlank(response.false_positive_reason)) {
            errors.push({ index, field: 'false_positive_reason', message: '오탐 사유를 입력하세요.' });
          }
          if (isBlank(response.expires_at)) {
            errors.push({ index, field: 'expires_at', message: '오탐 만료일을 입력하세요.' });
          } else if (!isDateOnly(response.expires_at)) {
            errors.push({ index, field: 'expires_at', message: '오탐 만료일은 YYYY-MM-DD 형식이어야 합니다.' });
          }
        } else if (response.decision === 'true_positive') {
          if (isBlank(response.action_plan)) {
            errors.push({ index, field: 'action_plan', message: '정탐 조치 계획을 입력하세요.' });
          }
          if (isBlank(response.action_due_date)) {
            errors.push({ index, field: 'action_due_date', message: '조치 예정일을 입력하세요.' });
          } else if (!isActionDueDateWithinWindow(response.action_due_date)) {
            errors.push({ index, field: 'action_due_date', message: '조치 예정일은 오늘부터 30일 이내여야 합니다.' });
          }
        }
      });
      return errors;
    }
    function focusFirstValidationError(error) {
      if (!error) {
        return;
      }
      const row = tbody.querySelector('tr[data-index="' + error.index + '"]');
      if (!row) {
        return;
      }
      hydrateRow(row);
      applyValidationState(row, error.index);
      const target = validationFocusTarget(row, error.field);
      const scrollTarget = validationTarget(row, error.field) || target || row;
      scrollTarget.scrollIntoView({ block: 'center', inline: 'center' });
      if (target && typeof target.focus === 'function') {
        target.focus();
      }
    }
    function firstValidationErrorInDisplayOrder(errors) {
      const rows = Array.from(tbody.querySelectorAll('tr[data-index]'));
      for (const row of rows) {
        const index = Number(row.getAttribute('data-index'));
        const error = errors.find(candidate => candidate.index === index);
        if (error) {
          return error;
        }
      }
      return errors[0];
    }
    function applyBulkTruePositivePlan() {
      const actionPlan = document.getElementById('bulkTruePositivePlan').value;
      const dueDate = document.getElementById('bulkTruePositiveDueDate').value;
      if (!actionPlan && !dueDate) {
        return;
      }
      const bulkSortKeys = new Set(['action_plan', 'action_due_date']);
      const shouldRefreshSort = bulkSortKeys.has(sortState.key);
      let changed = false;
      formState.forEach((values, index) => {
        if (values.decision === 'true_positive') {
          if (actionPlan) {
            values.action_plan = actionPlan;
            clearValidationField(index, 'action_plan');
          }
          if (dueDate) {
            values.action_due_date = dueDate;
            clearValidationField(index, 'action_due_date');
          }
          changed = true;
          if (!shouldRefreshSort) {
            updateHydratedRow(index);
          }
        }
      });
      if (changed && shouldRefreshSort) {
        renderFindings();
      }
    }
    function applyBulkFalsePositiveReason() {
      const reason = document.getElementById('bulkFalsePositiveReason').value;
      if (!reason) {
        return;
      }
      const bulkSortKeys = new Set(['false_positive_reason']);
      const shouldRefreshSort = bulkSortKeys.has(sortState.key);
      let changed = false;
      formState.forEach((values, index) => {
        if (values.decision === 'false_positive') {
          values.false_positive_reason = reason;
          clearValidationField(index, 'false_positive_reason');
          changed = true;
          if (!shouldRefreshSort) {
            updateHydratedRow(index);
          }
        }
      });
      if (changed && shouldRefreshSort) {
        renderFindings();
      }
    }
    function compactResponseFields(response) {
      return Object.fromEntries(Object.entries(response).filter(([, value]) =>
        value !== null && value !== undefined && value !== ''
      ));
    }
    function responseBase(response) {
      return {
        finding_key: response.finding_key,
        finding_hash: response.finding_hash,
        file_identifier: response.file_identifier,
        file_identifier_pattern: response.file_identifier_pattern,
        hive_database: response.hive_database,
        hive_table: response.hive_table,
        hive_table_fqn: response.hive_table_fqn,
        column_name: response.column_name,
        pii_type: response.pii_type,
        sample_row_count: response.sample_row_count,
        match_count: response.match_count,
        non_empty_match_ratio: response.non_empty_match_ratio,
        decision: response.decision
      };
    }
    function sanitizeResponse(response) {
      if (response.decision === 'false_positive') {
        return compactResponseFields(Object.assign(responseBase(response), {
          false_positive_reason: response.false_positive_reason,
          expires_at: response.expires_at
        }));
      }
      if (response.decision === 'true_positive') {
        return compactResponseFields(Object.assign(responseBase(response), {
          action_plan: response.action_plan,
          action_due_date: response.action_due_date
        }));
      }
      return compactResponseFields(response);
    }
    function updateSortHeaders() {
      document.querySelectorAll('#findingsTable th[data-sort-key]').forEach(th => {
        const isActive = th.getAttribute('data-sort-key') === sortState.key;
        th.setAttribute('aria-sort', isActive ? (sortState.direction === 'asc' ? 'ascending' : 'descending') : 'none');
        const indicator = th.querySelector('.sort-indicator');
        if (indicator) {
          indicator.textContent = isActive ? (sortState.direction === 'asc' ? '▲' : '▼') : '';
        }
      });
    }
    function renderPlaceholderRow(index) {
      const finding = REVIEW_DATA.findings[index];
      const summary = [
        finding.file_identifier,
        finding.hive_table_fqn,
        finding.column_name,
        displayPiiType(finding.pii_type)
      ].filter(Boolean).join(' / ');
      return `<td colspan="13" class="placeholder-cell"><span hidden data-finding-key="$${escapeHtml(finding.finding_key)}">$${escapeHtml(finding.finding_key)}</span><span class="placeholder-summary">$${escapeHtml(summary)}</span></td>`;
    }
    function renderSampleCell(finding) {
      const samples = finding.evidence_samples.map(sample =>
        escapeHtml(sample.sample_matched_fragment) + '\\n' +
        escapeHtml(sample.sample_raw_value)
      ).join('\\n---\\n');
      return samples;
    }
    function renderFindingCells(finding, index) {
      return `
        <td>$${escapeHtml(finding.file_identifier)}<span hidden data-finding-key="$${escapeHtml(finding.finding_key)}">$${escapeHtml(finding.finding_key)}</span></td>
        <td>$${escapeHtml(finding.hive_table_fqn)}</td>
        <td>$${escapeHtml(finding.column_name)}</td>
        <td>$${escapeHtml(displayPiiType(finding.pii_type))}</td>
        <td class="metric-cell">$${escapeHtml(finding.sampled_row_count)}</td>
        <td class="metric-cell">$${escapeHtml(finding.match_count)}</td>
        <td class="metric-cell">$${escapeHtml(finding.non_empty_match_ratio)}</td>
        <td class="sample">$${renderSampleCell(finding)}</td>
        <td>
          <div class="decision-toggle" role="group" aria-label="판정" data-validation-field="decision" aria-invalid="false">
            <button type="button" class="decision-button" data-index="$${index}" data-decision-button="false_positive" aria-pressed="false">오탐</button>
            <button type="button" class="decision-button" data-index="$${index}" data-decision-button="true_positive" aria-pressed="false">정탐</button>
          </div>
        </td>
        <td class="reason-cell">
          <div class="decision-fields" data-decision-section="false_positive">
            <textarea data-index="$${index}" data-field="false_positive_reason" aria-label="오탐 사유" placeholder="필수"></textarea>
          </div>
        </td>
        <td class="date-cell">
          <div class="decision-fields" data-decision-section="false_positive">
            <input data-index="$${index}" data-field="expires_at" type="date" aria-label="오탐 만료일" placeholder="YYYY-MM-DD" min="$${todayDateOnly()}">
          </div>
        </td>
        <td class="plan-cell">
          <div class="decision-fields" data-decision-section="true_positive">
            <textarea data-index="$${index}" data-field="action_plan" aria-label="정탐 조치 계획" placeholder="필수"></textarea>
          </div>
        </td>
        <td class="date-cell">
          <div class="decision-fields" data-decision-section="true_positive">
            <input data-index="$${index}" data-field="action_due_date" type="date" aria-label="조치 예정일" placeholder="YYYY-MM-DD" min="$${todayDateOnly()}" max="$${maxActionDueDate()}">
          </div>
        </td>`;
    }
    function hydrateRow(row) {
      if (!row || row.getAttribute('data-hydrated') === 'true') {
        return;
      }
      const index = Number(row.getAttribute('data-index'));
      const finding = REVIEW_DATA.findings[index];
      row.innerHTML = renderFindingCells(finding, index);
      row.setAttribute('data-hydrated', 'true');
      setFieldValues(row, index);
      applyDecisionVisibility(row);
      applyScopeVisibility(row);
      applyValidationState(row, index);
      hydratedRows.set(index, row);
    }
    function dehydrateRow(row) {
      if (!row || row.getAttribute('data-hydrated') !== 'true') {
        return;
      }
      const index = Number(row.getAttribute('data-index'));
      row.innerHTML = renderPlaceholderRow(index);
      row.setAttribute('data-hydrated', 'false');
      hydratedRows.delete(index);
    }
    function updateHydratedRow(index) {
      const row = hydratedRows.get(Number(index));
      if (row) {
        setFieldValues(row, index);
        applyDecisionVisibility(row);
        applyScopeVisibility(row);
        applyValidationState(row, index);
      }
    }
    function resetRowObserver() {
      hydratedRows.clear();
      if (rowObserver) {
        rowObserver.disconnect();
      }
      rowObserver = 'IntersectionObserver' in window
        ? new IntersectionObserver(entries => {
          entries.forEach(entry => {
            if (entry.isIntersecting) {
              hydrateRow(entry.target);
            } else {
              dehydrateRow(entry.target);
            }
          });
        }, { root: null, rootMargin: '1000px 0px', threshold: 0 })
        : null;
    }
    function observeRow(row) {
      if (rowObserver) {
        rowObserver.observe(row);
      } else {
        hydrateRow(row);
      }
    }
    function renderFindings() {
      resetRowObserver();
      const fragment = document.createDocumentFragment();
      sortRows(REVIEW_DATA.findings.map((_, index) => index)).forEach(index => {
        const row = document.createElement('tr');
        row.setAttribute('data-index', String(index));
        row.setAttribute('data-hydrated', 'false');
        row.innerHTML = renderPlaceholderRow(index);
        fragment.appendChild(row);
      });
      tbody.replaceChildren(fragment);
      tbody.querySelectorAll('tr[data-index]').forEach(observeRow);
      updateSortHeaders();
    }
    document.querySelectorAll('#findingsTable th[data-sort-key] button').forEach(button => {
      button.addEventListener('click', () => {
        const key = button.closest('th').getAttribute('data-sort-key');
        sortState = {
          key,
          direction: sortState.key === key && sortState.direction === 'asc' ? 'desc' : 'asc'
        };
        renderFindings();
      });
    });
    function handleDecisionClick(event) {
      const button = event.target.closest('[data-decision-button]');
      if (!button) {
        return;
      }
      const index = button.getAttribute('data-index');
      const decision = button.getAttribute('data-decision-button');
      const currentDecision = getFormState(index).decision;
      updateFormState(index, 'decision', currentDecision === decision ? '' : decision);
      clearValidationField(index, 'decision');
      clearValidationField(index, 'false_positive_reason');
      clearValidationField(index, 'expires_at');
      clearValidationField(index, 'action_plan');
      clearValidationField(index, 'action_due_date');
      const row = button.closest('tr');
      updateDecisionButtons(row, index);
      applyDecisionVisibility(row);
      applyScopeVisibility(row);
      applyValidationState(row, index);
    }
    function handleFormEvent(event) {
      if (!event.target.matches('[data-field]')) {
        return;
      }
      const input = event.target;
      const index = input.getAttribute('data-index');
      const field = input.getAttribute('data-field');
      updateFormState(index, field, input.value);
      if (!isBlank(input.value)) {
        clearValidationField(index, field);
      }
    }
    tbody.addEventListener('click', handleDecisionClick);
    tbody.addEventListener('input', handleFormEvent);
    tbody.addEventListener('change', handleFormEvent);
    document.getElementById('applyBulkTruePositivePlan').addEventListener('click', applyBulkTruePositivePlan);
    document.getElementById('applyBulkFalsePositiveReason').addEventListener('click', applyBulkFalsePositiveReason);
    renderFindings();
    document.getElementById('downloadResponse').addEventListener('click', () => {
      const values = collectFormValues();
      const responses = REVIEW_DATA.findings.map((finding, index) => {
        const response = Object.assign({
          finding_key: finding.finding_key,
          finding_hash: finding.finding_hash,
          file_identifier: finding.file_identifier,
          file_identifier_pattern: finding.hive_table_fqn ? '' : finding.file_identifier,
          hive_database: finding.hive_database,
          hive_table: finding.hive_table,
          hive_table_fqn: finding.hive_table_fqn,
          column_name: finding.column_name,
          pii_type: finding.pii_type,
          sample_row_count: finding.sampled_row_count,
          match_count: finding.match_count,
          non_empty_match_ratio: finding.non_empty_match_ratio
        }, values[index] || {});
        return response;
      });
      const responseValidationErrors = validateResponses(responses);
      if (responseValidationErrors.length > 0) {
        setValidationState(responseValidationErrors);
        focusFirstValidationError(firstValidationErrorInDisplayOrder(responseValidationErrors));
        return;
      }
      setValidationState([]);
      const sanitizedResponses = responses.map(sanitizeResponse).filter(response => response.decision);
      const envelope = {
        schema_version: 1,
        scan_path: REVIEW_DATA.scan_path,
        responder: document.getElementById('responder').value.trim(),
        responded_at: new Date().toISOString(),
        responses: sanitizedResponses
      };
      const blob = new Blob([JSON.stringify(envelope, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `response-$${formatResponseScanPath(REVIEW_DATA.scan_path)}-$${formatResponseTimestamp(new Date())}.json`;
      link.click();
      URL.revokeObjectURL(url);
    });
  </script>
</body>
</html>
"""
  }

  private def findingToJson(finding: ReviewFinding, sampleMode: String): String = {
    val samples = finding.evidence.take(5).map(evidenceToJson(_, sampleMode)).mkString("[", ",", "]")
    s"""{"scan_path":${jsonString(finding.scanPath)},"file_identifier":${jsonString(finding.fileIdentifier)},"hive_database":${jsonString(finding.hiveDatabase)},"hive_table":${jsonString(finding.hiveTable)},"hive_table_fqn":${jsonString(finding.hiveTableFqn)},"column_name":${jsonString(finding.columnName)},"pii_type":${jsonString(finding.piiType)},"match_count":${finding.matchCount},"sampled_row_count":${finding.sampledRowCount},"match_ratio":${finding.matchRatio},"non_empty_match_ratio":${finding.nonEmptyMatchRatio},"confidence":${finding.confidence},"finding_key":${jsonString(finding.findingKey)},"finding_hash":${jsonString(finding.findingHash)},"fingerprint_complete":${finding.fingerprintComplete},"has_multiple_file_evidence":${finding.hasMultipleFileEvidence},"evidence_samples":$samples}"""
  }

  private def evidenceToJson(evidence: ReviewEvidence, sampleMode: String): String = {
    val matched = renderSample(evidence.sampleMatchedFragment, evidence.sampleMatchedFragment, sampleMode)
    val raw = renderSample(evidence.sampleRawValue, evidence.sampleMatchedFragment, sampleMode)
    s"""{"file_identifier":${jsonString(evidence.fileIdentifier)},"sample_matched_fragment":${jsonString(matched)},"sample_raw_value":${jsonString(raw)},"match_count":${evidence.matchCount},"confidence":${evidence.confidence}}"""
  }

  private def renderSample(value: String, matchedFragment: String, sampleMode: String): String = sampleMode match {
    case "raw" => value
    case "none" => ""
    case _ =>
      val maskedFragment = maskScalar(matchedFragment)
      if (Option(matchedFragment).exists(_.nonEmpty)) {
        Option(value).getOrElse("").replace(matchedFragment, maskedFragment)
      } else {
        maskScalar(value)
      }
  }

  private def maskScalar(value: String): String = {
    val normalized = Option(value).getOrElse("")
    val atIndex = normalized.indexOf('@')
    if (atIndex > 1) {
      val localPart = normalized.substring(0, atIndex)
      val domain = normalized.substring(atIndex)
      s"${localPart.head}***${localPart.last}$domain"
    } else if (normalized.length <= 4) {
      "*" * normalized.length
    } else {
      normalized.take(2) + "***" + normalized.takeRight(2)
    }
  }
}
