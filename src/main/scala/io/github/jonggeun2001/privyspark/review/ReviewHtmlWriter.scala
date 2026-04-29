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
    table { border-collapse: collapse; width: 100%; }
    th, td { border: 1px solid #d5d8dc; padding: 8px; vertical-align: top; }
    th { background: #f4f6f7; text-align: left; }
    .sort-button { all: unset; cursor: pointer; display: inline-flex; align-items: center; gap: 4px; font-weight: 600; }
    .sort-button:focus-visible { outline: 2px solid #1f6feb; outline-offset: 2px; }
    .sort-indicator { min-width: 1em; }
    textarea, input, select { width: 100%; box-sizing: border-box; }
    .table-wrap { overflow-x: auto; }
    .sample { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; white-space: pre-wrap; }
    .field { display: block; margin-bottom: 8px; }
    .field > span { display: block; font-weight: 600; margin-bottom: 4px; }
    .hint { color: #566573; font-size: 12px; line-height: 1.45; margin: 6px 0 10px; white-space: pre-wrap; }
    .metric-cell { text-align: right; white-space: nowrap; }
    .pattern-cell { min-width: 180px; }
    .reason-cell, .plan-cell { min-width: 220px; }
    .scope-cell { min-width: 240px; }
    .placeholder-cell { min-height: 120px; color: #566573; background: #fbfcfc; }
    .placeholder-summary { display: block; min-height: 120px; }
    .bulk-actions { display: flex; gap: 12px; align-items: end; flex-wrap: wrap; margin: 16px 0; }
    .bulk-actions label { display: inline-flex; flex-direction: column; gap: 4px; font-weight: 600; }
    .decision-fields[hidden], [data-decision-section][hidden] { display: none; }
  </style>
</head>
<body>
  <h1>PrivySpark Review</h1>
  <p>Scan path: <code id="scanPath"></code></p>
  <p><label>응답자 <input id="responder"></label></p>
  <p class="bulk-actions">
    <label>삭제 예정일 <input id="bulkDeleteDueDate" type="date"></label>
    <button type="button" id="applyBulkDeletePlan">일괄 삭제 계획 등록</button>
  </p>
  <div class="table-wrap">
  <table id="findingsTable">
    <thead>
      <tr>
        <th scope="col" data-sort-key="path" aria-sort="none"><button type="button" class="sort-button">경로 <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="hive" aria-sort="none"><button type="button" class="sort-button">Hive <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="column" aria-sort="none"><button type="button" class="sort-button">컬럼 <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="pii" aria-sort="none"><button type="button" class="sort-button">PII <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="sampled_row_count" aria-sort="none"><button type="button" class="sort-button">sampled_row_count <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="match_count" aria-sort="none"><button type="button" class="sort-button">match_count <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="non_empty_match_ratio" aria-sort="none"><button type="button" class="sort-button">non_empty_match_ratio <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="sample" aria-sort="none"><button type="button" class="sort-button">샘플 <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="decision" aria-sort="none"><button type="button" class="sort-button">판정 <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="scope" aria-sort="none"><button type="button" class="sort-button">Allowlist Scope <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="false_positive_reason" aria-sort="none"><button type="button" class="sort-button">오탐 사유 <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="file_identifier_pattern" aria-sort="none"><button type="button" class="sort-button">file_identifier_pattern <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="column_name_pattern" aria-sort="none"><button type="button" class="sort-button">column_name_pattern <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="pii_type_pattern" aria-sort="none"><button type="button" class="sort-button">pii_type_pattern <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="expires_at" aria-sort="none"><button type="button" class="sort-button">pattern expires_at <span class="sort-indicator" aria-hidden="true"></span></button></th>
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
    const BulkDeleteActionPlan = '삭제 처리';
    const scopeGuidance = finding => {
      const exactHint = finding.fingerprint_complete
        ? 'exact: 이 finding만 제외합니다. 다음 스캔에서 dataset_path, file_identifier, column_name, pii_type, file_size, mtime, checksum fingerprint가 모두 다시 일치할 때만 suppress됩니다. 일반적으로 기본 선택입니다.'
        : 'exact: 이 finding은 checksum 등 fingerprint metadata가 부족해 collector에서 거부됩니다. pattern을 쓰거나 정탐으로 처리하세요.';
      const patternFileHint = finding.has_multiple_file_evidence
        ? '여러 파일 증거가 있어 file_identifier_pattern 필수입니다.'
        : 'file_identifier_pattern을 비우면 현재 대표 file_identifier를 사용합니다.';
      return [
        exactHint,
        'pattern: 반복 오탐을 넓게 제외합니다. 사유와 expires_at(YYYY-MM-DD)이 필수이고 pattern 필드 중 하나 이상 필요합니다.',
        patternFileHint,
        '*는 glob 와일드카드입니다. pii_type=* 금지.'
      ].join('\\n');
    };
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
    const FormFieldDefaults = {
      decision: '',
      allowlist_scope: 'exact',
      false_positive_reason: '',
      file_identifier_pattern: '',
      column_name_pattern: '',
      pii_type_pattern: '',
      expires_at: '',
      action_plan: '',
      action_due_date: ''
    };
    const FormFieldNames = Object.keys(FormFieldDefaults);
    const formState = new Map();
    const hydratedRows = new Map();
    const collator = new Intl.Collator('ko-KR', { numeric: true, sensitivity: 'base' });
    REVIEW_DATA.findings.forEach((_, index) => {
      formState.set(index, Object.assign({}, FormFieldDefaults));
    });
    let sortState = { key: null, direction: 'asc' };
    let rowObserver = null;
    function getFormState(index) {
      const numericIndex = Number(index);
      if (!formState.has(numericIndex)) {
        formState.set(numericIndex, Object.assign({}, FormFieldDefaults));
      }
      return formState.get(numericIndex);
    }
    function updateFormState(index, field, value) {
      if (!FormFieldNames.includes(field)) {
        return;
      }
      getFormState(index)[field] = value;
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
        sample.file_identifier,
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
          return finding.pii_type;
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
        case 'scope':
          return formSortText(index, ['allowlist_scope']);
        case 'false_positive_reason':
        case 'file_identifier_pattern':
        case 'column_name_pattern':
        case 'pii_type_pattern':
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
    }
    function applyDecisionVisibility(row) {
      const decisionInput = row.querySelector('[data-field="decision"]');
      if (!decisionInput) {
        return;
      }
      const decision = decisionInput.value;
      row.querySelectorAll('[data-decision-section]').forEach(section => {
        section.hidden = section.getAttribute('data-decision-section') !== decision;
      });
    }
    function applyBulkDeletePlan() {
      const dueDate = document.getElementById('bulkDeleteDueDate').value;
      const bulkSortKeys = new Set(['action_plan', 'action_due_date']);
      const shouldRefreshSort = bulkSortKeys.has(sortState.key);
      let changed = false;
      formState.forEach((values, index) => {
        if (values.decision === 'true_positive') {
          values.action_plan = BulkDeleteActionPlan;
          if (dueDate) {
            values.action_due_date = dueDate;
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
    function sanitizeResponse(response) {
      if (response.decision === 'false_positive') {
        return Object.assign({}, response, {
          action_plan: null,
          action_due_date: null
        });
      }
      if (response.decision === 'true_positive') {
        return Object.assign({}, response, {
          false_positive_reason: null,
          allowlist_scope: null,
          file_identifier_pattern: null,
          column_name_pattern: null,
          pii_type_pattern: null,
          expires_at: null
        });
      }
      return response;
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
        finding.pii_type
      ].filter(Boolean).join(' / ');
      return `<td colspan="17" class="placeholder-cell"><span hidden data-finding-key="$${escapeHtml(finding.finding_key)}">$${escapeHtml(finding.finding_key)}</span><span class="placeholder-summary">$${escapeHtml(summary)}</span></td>`;
    }
    function renderFindingCells(finding, index) {
      return `
        <td>$${escapeHtml(finding.file_identifier)}<span hidden data-finding-key="$${escapeHtml(finding.finding_key)}">$${escapeHtml(finding.finding_key)}</span></td>
        <td>$${escapeHtml(finding.hive_table_fqn)}</td>
        <td>$${escapeHtml(finding.column_name)}</td>
        <td>$${escapeHtml(finding.pii_type)}</td>
        <td class="metric-cell">$${escapeHtml(finding.sampled_row_count)}</td>
        <td class="metric-cell">$${escapeHtml(finding.match_count)}</td>
        <td class="metric-cell">$${escapeHtml(finding.non_empty_match_ratio)}</td>
        <td class="sample">$${finding.evidence_samples.map(sample => escapeHtml(sample.file_identifier) + '\\n' + escapeHtml(sample.sample_matched_fragment) + '\\n' + escapeHtml(sample.sample_raw_value)).join('\\n---\\n')}</td>
        <td>
          <label class="field"><span>판정</span>
            <select data-index="$${index}" data-field="decision">
              <option value="">선택</option>
              <option value="false_positive">오탐</option>
              <option value="true_positive">정탐</option>
            </select>
          </label>
          <div class="hint">오탐은 다음 스캔 suppress 대상입니다. 정탐은 suppress하지 않고 조치 계획을 남깁니다.</div>
        </td>
        <td class="scope-cell">
          <div data-decision-section="false_positive">
            <label class="field"><span>오탐 scope</span>
              <select data-index="$${index}" data-field="allowlist_scope">
                <option value="exact">exact</option>
                <option value="pattern">pattern</option>
              </select>
            </label>
            <div class="hint">$${escapeHtml(scopeGuidance(finding))}</div>
          </div>
        </td>
        <td class="reason-cell">
          <div class="decision-fields" data-decision-section="false_positive">
            <textarea data-index="$${index}" data-field="false_positive_reason" aria-label="오탐 사유" placeholder="오탐 판단 근거. exact와 pattern 모두 필수"></textarea>
          </div>
        </td>
        <td class="pattern-cell">
          <div class="decision-fields" data-decision-section="false_positive">
            <input data-index="$${index}" data-field="file_identifier_pattern" aria-label="file_identifier_pattern" placeholder="예: project_db/customer/*">
          </div>
        </td>
        <td class="pattern-cell">
          <div class="decision-fields" data-decision-section="false_positive">
            <input data-index="$${index}" data-field="column_name_pattern" aria-label="column_name_pattern" placeholder="예: temp_*">
          </div>
        </td>
        <td class="pattern-cell">
          <div class="decision-fields" data-decision-section="false_positive">
            <input data-index="$${index}" data-field="pii_type_pattern" aria-label="pii_type_pattern" placeholder="예: driver_license_number (* 금지)">
          </div>
        </td>
        <td class="pattern-cell">
          <div class="decision-fields" data-decision-section="false_positive">
            <input data-index="$${index}" data-field="expires_at" aria-label="pattern expires_at" placeholder="YYYY-MM-DD">
          </div>
        </td>
        <td class="plan-cell">
          <div class="decision-fields" data-decision-section="true_positive">
            <textarea data-index="$${index}" data-field="action_plan" aria-label="정탐 조치 계획" placeholder="정탐이면 필수. 예: 컬럼 마스킹, 삭제 처리"></textarea>
          </div>
        </td>
        <td class="pattern-cell">
          <div class="decision-fields" data-decision-section="true_positive">
            <input data-index="$${index}" data-field="action_due_date" aria-label="조치 예정일" placeholder="YYYY-MM-DD">
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
    function handleFormEvent(event) {
      if (!event.target.matches('[data-field]')) {
        return;
      }
      const input = event.target;
      const index = input.getAttribute('data-index');
      const field = input.getAttribute('data-field');
      updateFormState(index, field, input.value);
      if (field === 'decision') {
        applyDecisionVisibility(input.closest('tr'));
      }
    }
    tbody.addEventListener('input', handleFormEvent);
    tbody.addEventListener('change', handleFormEvent);
    document.getElementById('applyBulkDeletePlan').addEventListener('click', applyBulkDeletePlan);
    renderFindings();
    document.getElementById('downloadResponse').addEventListener('click', () => {
      const values = collectFormValues();
      const responses = REVIEW_DATA.findings.map((finding, index) => Object.assign({
        finding_key: finding.finding_key,
        finding_hash: finding.finding_hash,
        file_identifier: finding.file_identifier,
        column_name: finding.column_name,
        pii_type: finding.pii_type,
        file_identifier_pattern: null,
        column_name_pattern: null,
        pii_type_pattern: null
      }, values[index] || {})).map(sanitizeResponse).filter(response => response.decision);
      const envelope = {
        schema_version: 1,
        scan_path: REVIEW_DATA.scan_path,
        scan_results_fingerprint: REVIEW_DATA.scan_results_fingerprint,
        responder: document.getElementById('responder').value.trim(),
        responded_at: new Date().toISOString(),
        responses
      };
      const blob = new Blob([JSON.stringify(envelope, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `response-$${formatResponseTimestamp(new Date())}.json`;
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
