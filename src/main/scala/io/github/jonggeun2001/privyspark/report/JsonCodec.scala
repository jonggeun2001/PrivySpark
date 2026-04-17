package io.github.jonggeun2001.privyspark.report

import io.github.jonggeun2001.privyspark.model.{ProgressRun, ScanError, ScanResult}

import scala.util.Try

private[privyspark] object JsonCodec {
  def scanResultToJson(result: ScanResult): String =
    s"""{"dataset_path":${jsonString(result.dataset_path)},"scan_timestamp":${jsonString(result.scan_timestamp)},"file_identifier":${jsonString(result.file_identifier)},"column_name":${jsonString(result.column_name)},"pii_type":${jsonString(result.pii_type)},"match_count":${result.match_count},"sampled_row_count":${result.sampled_row_count},"match_ratio":${result.match_ratio},"non_empty_match_ratio":${result.non_empty_match_ratio},"confidence":${result.confidence},"sample_raw_value":${jsonString(result.sample_raw_value)},"sample_matched_fragment":${jsonString(result.sample_matched_fragment)}}"""

  def scanErrorToJson(error: ScanError): String =
    s"""{"dataset_path":${jsonString(error.dataset_path)},"scan_timestamp":${jsonString(error.scan_timestamp)},"file_identifier":${jsonString(error.file_identifier)},"error_message":${jsonString(error.error_message)}}"""

  def progressCompletionToJson(scope: String, identifier: String, resultCount: Int, errorCount: Int): String =
    s"""{"scope":${jsonString(scope)},"identifier":${jsonString(identifier)},"result_count":$resultCount,"error_count":$errorCount,"state":"completed"}"""

  def activeRunMetadataJson(
    progressRun: ProgressRun,
    state: String,
    lastHeartbeatEpochMillis: Long,
    errorMessage: Option[String]
  ): String =
    s"""{"run_id":${jsonString(progressRun.runId)},"dataset_path":${jsonString(progressRun.datasetPath)},"output_root":${jsonString(progressRun.outputRoot)},"scan_timestamp":${jsonString(progressRun.scanTimestamp)},"state":${jsonString(state)},"last_heartbeat_epoch_ms":$lastHeartbeatEpochMillis,"error_message":${jsonNullableString(errorMessage)}}"""

  def progressRunMetadataJson(
    progressRun: ProgressRun,
    state: String,
    errorMessage: Option[String]
  ): String =
    s"""{"run_id":${jsonString(progressRun.runId)},"dataset_path":${jsonString(progressRun.datasetPath)},"output_root":${jsonString(progressRun.outputRoot)},"scan_timestamp":${jsonString(progressRun.scanTimestamp)},"state":${jsonString(state)},"error_message":${jsonNullableString(errorMessage)}}"""

  def jsonString(value: String): String = "\"" + escapeJson(Option(value).getOrElse("")) + "\""

  def jsonNullableString(value: Option[String]): String = value.map(jsonString).getOrElse("null")

  def extractJsonStringField(json: String, field: String): Option[String] = {
    val pattern = (""""""" + java.util.regex.Pattern.quote(field) + """":"([^"]*)"""").r
    pattern.findFirstMatchIn(json).map(_.group(1))
  }

  def extractJsonLongField(json: String, field: String): Option[Long] = {
    val pattern = (""""""" + java.util.regex.Pattern.quote(field) + """":([0-9]+)""").r
    pattern.findFirstMatchIn(json).flatMap(m => Try(m.group(1).toLong).toOption)
  }

  def escapeJson(value: String): String = {
    val builder = new StringBuilder
    value.foreach {
      case '"' => builder.append("\\\"")
      case '\\' => builder.append("\\\\")
      case '\b' => builder.append("\\b")
      case '\f' => builder.append("\\f")
      case '\n' => builder.append("\\n")
      case '\r' => builder.append("\\r")
      case '\t' => builder.append("\\t")
      case ch if ch < ' ' => builder.append(f"\\u${ch.toInt}%04x")
      case ch => builder.append(ch)
    }
    builder.toString()
  }
}
