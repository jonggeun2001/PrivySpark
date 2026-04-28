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
    reviewHtmlPath: Option[String]
  ): Unit = {
    val normalizedSampleMode = normalizeSampleMode(sampleMode).getOrElse(DefaultSampleMode)
    val findings = ReviewFindingBuilder.fromScanResultsIterator(
      results.iterator,
      ReviewFindingBuilder.DefaultMaxEvidenceSamples
    )
    writeFindings(conf, outputRoot, scanPath, findings, normalizedSampleMode, reviewHtmlPath)
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
    reviewHtmlPath: Option[String]
  ): Unit = {
    val normalizedSampleMode = normalizeSampleMode(sampleMode).getOrElse(DefaultSampleMode)
    val findings = ReviewFindingBuilder.fromScanResultsIterator(
      ScanResultsReader.iterateScanResults(resultDf, ordered = true),
      ReviewFindingBuilder.DefaultMaxEvidenceSamples
    )
    writeFindings(conf, outputRoot, scanPath, findings, normalizedSampleMode, reviewHtmlPath)
  }

  private def writeFindings(
    conf: Configuration,
    outputRoot: String,
    scanPath: String,
    findings: Seq[ReviewFinding],
    sampleMode: String,
    reviewHtmlPath: Option[String]
  ): Unit = {
    val scanResultsFingerprint = ReviewFindingBuilder.scanResultsFingerprint(findings)
    val html = renderHtml(scanPath, scanResultsFingerprint, findings, sampleMode)
    val htmlPath = resolveHtmlPath(outputRoot, reviewHtmlPath)
    val fs = htmlPath.getFileSystem(conf)
    Option(htmlPath.getParent).foreach(fs.mkdirs)
    val writer = new BufferedWriter(new OutputStreamWriter(fs.create(htmlPath, true), StandardCharsets.UTF_8))
    try {
      writer.write(html)
    } finally {
      writer.close()
    }
  }

  private def resolveHtmlPath(outputRoot: String, reviewHtmlPath: Option[String]): Path =
    reviewHtmlPath
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(new Path(_))
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
    .sample { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; white-space: pre-wrap; }
    .field { display: block; margin-bottom: 8px; }
    .field > span { display: block; font-weight: 600; margin-bottom: 4px; }
    .hint { color: #566573; font-size: 12px; line-height: 1.45; margin: 6px 0 10px; white-space: pre-wrap; }
    .scope-cell { min-width: 240px; }
  </style>
</head>
<body>
  <h1>PrivySpark Review</h1>
  <p>Scan path: <code id="scanPath"></code></p>
  <p><label>응답자 <input id="responder"></label></p>
  <table id="findingsTable">
    <thead>
      <tr>
        <th scope="col" data-sort-key="path" aria-sort="none"><button type="button" class="sort-button">경로 / Hive <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="column" aria-sort="none"><button type="button" class="sort-button">컬럼 / PII <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="metrics" aria-sort="none"><button type="button" class="sort-button">지표 <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="sample" aria-sort="none"><button type="button" class="sort-button">샘플 <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="decision" aria-sort="none"><button type="button" class="sort-button">판정 <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="scope" aria-sort="none"><button type="button" class="sort-button">Allowlist Scope <span class="sort-indicator" aria-hidden="true"></span></button></th>
        <th scope="col" data-sort-key="reason" aria-sort="none"><button type="button" class="sort-button">사유 / 계획 <span class="sort-indicator" aria-hidden="true"></span></button></th>
      </tr>
    </thead>
    <tbody></tbody>
  </table>
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
    let sortState = { key: null, direction: 'asc' };
    function collectFormValues() {
      const values = {};
      document.querySelectorAll('[data-index]').forEach(input => {
        const index = input.getAttribute('data-index');
        const field = input.getAttribute('data-field');
        values[index] = values[index] || {};
        values[index][field] = input.value;
      });
      return values;
    }
    function sampleSortText(finding) {
      return finding.evidence_samples.map(sample => [
        sample.file_identifier,
        sample.sample_matched_fragment,
        sample.sample_raw_value
      ].join(' ')).join(' ');
    }
    function formSortText(values, index, fields) {
      const rowValues = values[index] || {};
      return fields.map(field => rowValues[field] || '').join(' ');
    }
    function getSortValue(row, values) {
      const finding = row.finding;
      switch (sortState.key) {
        case 'path':
          return [finding.file_identifier, finding.hive_table_fqn, finding.finding_key];
        case 'column':
          return [finding.column_name, finding.pii_type];
        case 'metrics':
          return [Number(finding.match_count) || 0, Number(finding.confidence) || 0];
        case 'sample':
          return sampleSortText(finding);
        case 'decision':
          return formSortText(values, row.index, ['decision']);
        case 'scope':
          return formSortText(values, row.index, ['allowlist_scope']);
        case 'reason':
          return formSortText(values, row.index, [
            'false_positive_reason',
            'file_identifier_pattern',
            'column_name_pattern',
            'pii_type_pattern',
            'expires_at',
            'action_plan',
            'action_due_date'
          ]);
        default:
          return row.index;
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
      return String(left ?? '').localeCompare(String(right ?? ''), 'ko-KR', {
        numeric: true,
        sensitivity: 'base'
      });
    }
    function sortRows(rows, values) {
      if (!sortState.key) {
        return rows;
      }
      const direction = sortState.direction === 'desc' ? -1 : 1;
      return rows.slice().sort((left, right) => {
        const result = compareSortValues(getSortValue(left, values), getSortValue(right, values));
        return result === 0 ? left.index - right.index : result * direction;
      });
    }
    function setFieldValues(row, index, savedValues) {
      Object.entries(savedValues[index] || {}).forEach(([field, value]) => {
        const input = row.querySelector('[data-index="' + index + '"][data-field="' + field + '"]');
        if (input) {
          input.value = value;
        }
      });
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
    function renderFindings(savedValues = {}) {
      tbody.innerHTML = '';
      const rows = REVIEW_DATA.findings.map((finding, index) => ({ finding, index }));
      sortRows(rows, savedValues).forEach(rowData => {
        const finding = rowData.finding;
        const index = rowData.index;
        const row = document.createElement('tr');
        row.innerHTML = `
        <td>$${escapeHtml(finding.file_identifier)}<br><small>$${escapeHtml(finding.hive_database)}.$${escapeHtml(finding.hive_table)}</small><br><small>$${escapeHtml(finding.finding_key)}</small></td>
        <td>$${escapeHtml(finding.column_name)}<br>$${escapeHtml(finding.pii_type)}</td>
        <td>count=$${escapeHtml(finding.match_count)}<br>confidence=$${escapeHtml(finding.confidence)}</td>
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
          <label class="field"><span>오탐 scope</span>
            <select data-index="$${index}" data-field="allowlist_scope">
              <option value="exact">exact</option>
              <option value="pattern">pattern</option>
            </select>
          </label>
          <div class="hint">$${escapeHtml(scopeGuidance(finding))}</div>
        </td>
        <td>
          <label class="field"><span>오탐 사유</span>
            <textarea data-index="$${index}" data-field="false_positive_reason" placeholder="오탐 판단 근거. exact와 pattern 모두 필수"></textarea>
          </label>
          <label class="field"><span>file_identifier_pattern</span>
            <input data-index="$${index}" data-field="file_identifier_pattern" placeholder="예: project_db/customer/*">
          </label>
          <label class="field"><span>column_name_pattern</span>
            <input data-index="$${index}" data-field="column_name_pattern" placeholder="예: temp_*">
          </label>
          <label class="field"><span>pii_type_pattern</span>
            <input data-index="$${index}" data-field="pii_type_pattern" placeholder="예: driver_license_number (* 금지)">
          </label>
          <label class="field"><span>pattern expires_at</span>
            <input data-index="$${index}" data-field="expires_at" placeholder="YYYY-MM-DD">
          </label>
          <label class="field"><span>정탐 조치 계획</span>
            <textarea data-index="$${index}" data-field="action_plan" placeholder="정탐이면 필수. 예: 컬럼 마스킹, 접근권한 회수"></textarea>
          </label>
          <label class="field"><span>조치 예정일</span>
            <input data-index="$${index}" data-field="action_due_date" placeholder="YYYY-MM-DD">
          </label>
        </td>`;
        setFieldValues(row, index, savedValues);
        tbody.appendChild(row);
      });
      updateSortHeaders();
    }
    document.querySelectorAll('#findingsTable th[data-sort-key] button').forEach(button => {
      button.addEventListener('click', () => {
        const key = button.closest('th').getAttribute('data-sort-key');
        const savedValues = collectFormValues();
        sortState = {
          key,
          direction: sortState.key === key && sortState.direction === 'asc' ? 'desc' : 'asc'
        };
        renderFindings(savedValues);
      });
    });
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
      }, values[index] || {})).filter(response => response.decision);
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
