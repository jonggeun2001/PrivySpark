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
    textarea, input, select { width: 100%; box-sizing: border-box; }
    .sample { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; white-space: pre-wrap; }
  </style>
</head>
<body>
  <h1>PrivySpark Review</h1>
  <p>Scan path: <code id="scanPath"></code></p>
  <p><label>응답자 <input id="responder"></label></p>
  <table id="findingsTable">
    <thead>
      <tr>
        <th>경로 / Hive</th>
        <th>컬럼 / PII</th>
        <th>지표</th>
        <th>샘플</th>
        <th>판정</th>
        <th>사유 / 계획</th>
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
    REVIEW_DATA.findings.forEach((finding, index) => {
      const row = document.createElement('tr');
      row.innerHTML = `
        <td>$${escapeHtml(finding.file_identifier)}<br><small>$${escapeHtml(finding.hive_database)}.$${escapeHtml(finding.hive_table)}</small><br><small>$${escapeHtml(finding.finding_key)}</small></td>
        <td>$${escapeHtml(finding.column_name)}<br>$${escapeHtml(finding.pii_type)}</td>
        <td>count=$${escapeHtml(finding.match_count)}<br>confidence=$${escapeHtml(finding.confidence)}</td>
        <td class="sample">$${finding.evidence_samples.map(sample => escapeHtml(sample.file_identifier) + '\\n' + escapeHtml(sample.sample_matched_fragment) + '\\n' + escapeHtml(sample.sample_raw_value)).join('\\n---\\n')}</td>
        <td>
          <select data-index="$${index}" data-field="decision">
            <option value="">선택</option>
            <option value="false_positive">오탐</option>
            <option value="true_positive">정탐</option>
          </select>
          <select data-index="$${index}" data-field="allowlist_scope">
            <option value="exact">exact</option>
            <option value="pattern">pattern</option>
          </select>
        </td>
        <td>
          <textarea data-index="$${index}" data-field="false_positive_reason" placeholder="오탐 사유"></textarea>
          <input data-index="$${index}" data-field="file_identifier_pattern" placeholder="file_identifier pattern (* 지원)">
          <input data-index="$${index}" data-field="column_name_pattern" placeholder="column_name pattern (* 지원)">
          <input data-index="$${index}" data-field="pii_type_pattern" placeholder="pii_type pattern (* 제외)">
          <input data-index="$${index}" data-field="expires_at" placeholder="pattern 만료일 YYYY-MM-DD">
          <textarea data-index="$${index}" data-field="action_plan" placeholder="정탐 조치 계획"></textarea>
          <input data-index="$${index}" data-field="action_due_date" placeholder="YYYY-MM-DD">
        </td>`;
      tbody.appendChild(row);
    });
    document.getElementById('downloadResponse').addEventListener('click', () => {
      const values = {};
      document.querySelectorAll('[data-index]').forEach(input => {
        const index = input.getAttribute('data-index');
        const field = input.getAttribute('data-field');
        values[index] = values[index] || {};
        values[index][field] = input.value;
      });
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
    s"""{"scan_path":${jsonString(finding.scanPath)},"file_identifier":${jsonString(finding.fileIdentifier)},"hive_database":${jsonString(finding.hiveDatabase)},"hive_table":${jsonString(finding.hiveTable)},"hive_table_fqn":${jsonString(finding.hiveTableFqn)},"column_name":${jsonString(finding.columnName)},"pii_type":${jsonString(finding.piiType)},"match_count":${finding.matchCount},"sampled_row_count":${finding.sampledRowCount},"match_ratio":${finding.matchRatio},"non_empty_match_ratio":${finding.nonEmptyMatchRatio},"confidence":${finding.confidence},"finding_key":${jsonString(finding.findingKey)},"finding_hash":${jsonString(finding.findingHash)},"evidence_samples":$samples}"""
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
