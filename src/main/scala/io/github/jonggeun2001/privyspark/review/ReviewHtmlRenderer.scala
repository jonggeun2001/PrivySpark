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
    sampleMode: String
  ): String = {
    val findingJson = findings.map(ReviewSampleMasker.findingToJson(_, sampleMode)).mkString("[", ",", "]")
    val reviewData =
      s"""{"schema_version":1,"scan_path":${jsonString(scanPath)},"scan_results_fingerprint":${jsonString(scanResultsFingerprint)},"findings":$findingJson}"""
    val safeReviewData = reviewData.replace("</", "<\\/")
    replaceRequired(
      replaceRequired(Template, ScriptPlaceholder, Script),
      ReviewDataPlaceholder,
      safeReviewData
    )
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
