package io.github.jonggeun2001.privyspark.report

import io.github.jonggeun2001.privyspark.model.{ProgressRun, ScanError, ScanResult}

import scala.util.Try

private[privyspark] object JsonCodec {
  def scanResultToJson(result: ScanResult): String =
    s"""{"dataset_path":${jsonString(result.dataset_path)},"scan_timestamp":${jsonString(result.scan_timestamp)},"file_identifier":${jsonString(result.file_identifier)},"column_name":${jsonString(result.column_name)},"pii_type":${jsonString(result.pii_type)},"match_count":${result.match_count},"sampled_row_count":${result.sampled_row_count},"match_ratio":${result.match_ratio},"non_empty_match_ratio":${result.non_empty_match_ratio},"confidence":${result.confidence},"sample_raw_value":${jsonString(result.sample_raw_value)},"sample_matched_fragment":${jsonString(result.sample_matched_fragment)},"file_size":${result.file_size},"file_mtime_epoch_ms":${result.file_mtime_epoch_ms},"review_status":${jsonString(result.review_status)},"review_reason":${jsonString(result.review_reason)},"review_invalidated":${result.review_invalidated},"review_scope_file_identifiers":${jsonString(result.review_scope_file_identifiers)},"review_scope_file_fingerprints":${jsonString(result.review_scope_file_fingerprints)}}"""

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

  def extractJsonStringField(json: String, field: String): Option[String] =
    findJsonFieldValueStart(json, field).flatMap(parseJsonString(json, _).map(_._1))

  def extractJsonLongField(json: String, field: String): Option[Long] =
    findJsonFieldValueStart(json, field).flatMap { startIndex =>
      val endIndex = json.indexWhere(ch => ch == ',' || ch == '}' || ch.isWhitespace, startIndex) match {
        case -1 => json.length
        case index => index
      }
      Try(json.substring(startIndex, endIndex).trim.toLong).toOption
    }

  def extractJsonBooleanField(json: String, field: String): Option[Boolean] =
    findJsonFieldValueStart(json, field).flatMap { startIndex =>
      if (json.startsWith("true", startIndex)) {
        Some(true)
      } else if (json.startsWith("false", startIndex)) {
        Some(false)
      } else {
        None
      }
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

  private def findJsonFieldValueStart(json: String, field: String): Option[Int] = {
    val fieldToken = "\"" + escapeJson(field) + "\""
    var searchFrom = 0

    while (searchFrom >= 0 && searchFrom < json.length) {
      val fieldIndex = json.indexOf(fieldToken, searchFrom)
      if (fieldIndex < 0) {
        return None
      }

      val colonIndex = skipWhitespace(json, fieldIndex + fieldToken.length)
      if (colonIndex < json.length && json.charAt(colonIndex) == ':') {
        return Some(skipWhitespace(json, colonIndex + 1))
      }

      searchFrom = fieldIndex + fieldToken.length
    }

    None
  }

  private def skipWhitespace(json: String, fromIndex: Int): Int = {
    var index = fromIndex
    while (index < json.length && json.charAt(index).isWhitespace) {
      index += 1
    }
    index
  }

  private def parseJsonString(json: String, startIndex: Int): Option[(String, Int)] = {
    if (startIndex >= json.length || json.charAt(startIndex) != '"') {
      return None
    }

    val builder = new StringBuilder
    var index = startIndex + 1
    while (index < json.length) {
      json.charAt(index) match {
        case '"' =>
          return Some(builder.toString() -> (index + 1))
        case '\\' =>
          index += 1
          if (index >= json.length) {
            return None
          }
          json.charAt(index) match {
            case '"' => builder.append('"')
            case '\\' => builder.append('\\')
            case '/' => builder.append('/')
            case 'b' => builder.append('\b')
            case 'f' => builder.append('\f')
            case 'n' => builder.append('\n')
            case 'r' => builder.append('\r')
            case 't' => builder.append('\t')
            case 'u' =>
              if (index + 4 >= json.length) {
                return None
              }
              Try(Integer.parseInt(json.substring(index + 1, index + 5), 16)).toOption match {
                case Some(codePoint) =>
                  builder.append(codePoint.toChar)
                  index += 4
                case None =>
                  return None
              }
            case _ =>
              return None
          }
        case ch =>
          builder.append(ch)
      }
      index += 1
    }

    None
  }
}
