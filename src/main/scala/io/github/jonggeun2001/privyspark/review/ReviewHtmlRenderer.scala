package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.report.JsonCodec.jsonString

import java.nio.charset.StandardCharsets
import scala.io.Source

private[privyspark] object ReviewHtmlRenderer {
  private val TemplateResource = "/review/review.html.template"
  private val ScriptResource = "/review/review.js"
  private val ReviewDataPlaceholder = "${REVIEW_DATA_JSON}"
  private val ScriptPlaceholder = "${REVIEW_APP_SCRIPT}"

  private val Template = loadResource(TemplateResource)
  private val Script = loadResource(ScriptResource).stripSuffix("\n")

  def render(
    scanPath: String,
    scanResultsFingerprint: String,
    findings: Seq[ReviewFinding],
    sampleMode: String,
    actionPlanStates: Map[String, ReviewActionPlanStatus] = Map.empty,
    partInfo: Option[ReviewHtmlPartInfo] = None
  ): String = {
    val findingJson = findings
      .map(finding => ReviewSampleMasker.findingToJson(finding, sampleMode, actionPlanStates.get(finding.findingKey)))
      .mkString("[", ",", "]")
    val partJson = partInfo.map(info => s""","review_part":${partInfoToJson(info)}""").getOrElse("")
    val reviewData =
      s"""{"schema_version":1,"scan_path":${jsonString(scanPath)},"scan_results_fingerprint":${jsonString(scanResultsFingerprint)},"findings":$findingJson$partJson}"""
    val safeReviewData = reviewData.replace("</", "<\\/")
    replaceRequired(
      replaceRequired(Template, ScriptPlaceholder, Script),
      ReviewDataPlaceholder,
      safeReviewData
    )
  }

  def renderIndex(
    scanPath: String,
    scanResultsFingerprint: String,
    parts: Seq[ReviewHtmlPartInfo]
  ): String = {
    val links = parts.map { part =>
      s"""      <li><a href="${escapeHtml(part.fileName)}">${escapeHtml(part.fileName)}</a> - ${part.findingStart}~${part.findingEnd} / ${part.totalFindings} (${part.findingCount}건)</li>"""
    }.mkString("\n")
    s"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <title>Review</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px; color: #17202a; line-height: 1.55; }
    code { background: #eef2f7; padding: 1px 4px; border-radius: 3px; }
    .notice { background: #f8fafc; border: 1px solid #d5d8dc; padding: 12px 16px; margin: 16px 0; }
    li { margin: 6px 0; }
  </style>
</head>
<body>
  <h1>Review</h1>
  <p>Scan path: <code>${escapeHtml(scanPath)}</code></p>
  <p>Scan results fingerprint: <code>${escapeHtml(scanResultsFingerprint)}</code></p>
  <div class="notice">
    <strong>분할된 리뷰 파일</strong>
    <p>검출 건수가 많아 브라우저 메모리 사용량을 줄이기 위해 review HTML을 2MB 이하 파일로 분할했습니다. 아래 part 파일을 각각 열어 응답 파일을 생성하고, 생성된 JSON 파일을 모두 review state inbox에 제출하세요.</p>
  </div>
  <ol>
$links
  </ol>
</body>
</html>"""
  }

  private def partInfoToJson(info: ReviewHtmlPartInfo): String =
    s"""{"part_number":${info.partNumber},"part_count":${info.partCount},"finding_start":${info.findingStart},"finding_end":${info.findingEnd},"finding_count":${info.findingCount},"total_findings":${info.totalFindings},"file_name":${jsonString(info.fileName)}}"""

  private def escapeHtml(value: String): String =
    Option(value).getOrElse("").flatMap {
      case '&' => "&amp;"
      case '<' => "&lt;"
      case '>' => "&gt;"
      case '"' => "&quot;"
      case '\'' => "&#39;"
      case ch => ch.toString
    }

  private def replaceRequired(template: String, placeholder: String, value: String): String = {
    require(template.contains(placeholder), s"review HTML template is missing placeholder $placeholder")
    template.replace(placeholder, value)
  }

  private def loadResource(resourcePath: String): String = {
    val stream = Option(getClass.getResourceAsStream(resourcePath))
      .getOrElse(throw new IllegalStateException(s"Missing review HTML resource: $resourcePath"))
    val source = Source.fromInputStream(stream, StandardCharsets.UTF_8.name())
    try {
      source.mkString
    } finally {
      source.close()
    }
  }
}

private[privyspark] final case class ReviewHtmlPartInfo(
  partNumber: Int,
  partCount: Int,
  findingStart: Int,
  findingEnd: Int,
  findingCount: Int,
  totalFindings: Int,
  fileName: String
)
