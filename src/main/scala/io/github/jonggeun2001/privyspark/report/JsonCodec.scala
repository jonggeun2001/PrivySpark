package io.github.jonggeun2001.privyspark.report

import io.github.jonggeun2001.privyspark.model.{ProgressRun, ScanError, ScanResult}

import scala.util.Try

private[privyspark] object JsonCodec {
  def scanResultToJson(result: ScanResult): String =
    s"""{"dataset_path":${jsonString(result.dataset_path)},"scan_timestamp":${jsonString(result.scan_timestamp)},"file_identifier":${jsonString(result.file_identifier)},"column_name":${jsonString(result.column_name)},"pii_type":${jsonString(result.pii_type)},"match_count":${result.match_count},"sampled_row_count":${result.sampled_row_count},"non_empty_value_count":${result.non_empty_value_count},"match_ratio":${result.match_ratio},"non_empty_match_ratio":${result.non_empty_match_ratio},"confidence":${result.confidence},"sample_raw_value":${jsonString(result.sample_raw_value)},"sample_matched_fragment":${jsonString(result.sample_matched_fragment)},"file_size":${result.file_size},"file_mtime_epoch_ms":${result.file_mtime_epoch_ms},"hive_table_fqn":${jsonString(result.hive_table_fqn)},"aggregated":${result.aggregated},"aggregated_file_count":${result.aggregated_file_count},"aggregated_partition_count":${result.aggregated_partition_count},"review_status":${jsonString(result.review_status)},"review_reason":${jsonString(result.review_reason)},"review_invalidated":${result.review_invalidated},"review_scope_file_identifiers":${jsonString(result.review_scope_file_identifiers)},"review_scope_file_fingerprints":${jsonString(result.review_scope_file_fingerprints)}}"""

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

  def extractJsonObjectArrayField(json: String, field: String): Option[Seq[String]] =
    findJsonFieldValueStart(json, field).flatMap { startIndex =>
      if (startIndex >= json.length || json.charAt(startIndex) != '[') {
        None
      } else {
        val objects = scala.collection.mutable.ArrayBuffer.empty[String]
        var index = startIndex + 1
        var done = false
        while (!done && index < json.length) {
          index = skipWhitespace(json, index)
          if (index < json.length && json.charAt(index) == ']') {
            done = true
            index += 1
          } else if (index < json.length && json.charAt(index) == '{') {
            val nextIndex = skipJsonValue(json, index)
            if (nextIndex < 0) {
              return None
            }
            objects += json.substring(index, nextIndex)
            index = skipWhitespace(json, nextIndex)
            if (index < json.length && json.charAt(index) == ',') {
              index += 1
            } else if (index < json.length && json.charAt(index) == ']') {
              done = true
              index += 1
            }
          } else {
            return None
          }
        }
        if (done) Some(objects.toSeq) else None
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
    var index = skipWhitespace(json, 0)
    if (index >= json.length || json.charAt(index) != '{') {
      return None
    }

    index += 1
    while (index < json.length) {
      index = skipWhitespace(json, index)
      if (index >= json.length || json.charAt(index) == '}') {
        return None
      }

      parseJsonString(json, index) match {
        case Some((fieldName, afterFieldName)) =>
          val colonIndex = skipWhitespace(json, afterFieldName)
          if (colonIndex >= json.length || json.charAt(colonIndex) != ':') {
            return None
          }

          val valueStart = skipWhitespace(json, colonIndex + 1)
          if (fieldName == field) {
            return Some(valueStart)
          }

          val nextIndex = skipJsonValue(json, valueStart)
          if (nextIndex < 0) {
            return None
          }
          index = skipWhitespace(json, nextIndex)
          if (index < json.length && json.charAt(index) == ',') {
            index += 1
          }
        case None =>
          return None
      }
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

  private def skipJsonValue(json: String, startIndex: Int): Int = {
    if (startIndex >= json.length) {
      -1
    } else {
      json.charAt(startIndex) match {
        case '"' =>
          parseJsonString(json, startIndex).map(_._2).getOrElse(-1)
        case '{' =>
          skipJsonStructure(json, startIndex, '{', '}')
        case '[' =>
          skipJsonStructure(json, startIndex, '[', ']')
        case _ =>
          var index = startIndex
          while (index < json.length && json.charAt(index) != ',' && json.charAt(index) != '}' && json.charAt(index) != ']') {
            index += 1
          }
          index
      }
    }
  }

  private def skipJsonStructure(json: String, startIndex: Int, openChar: Char, closeChar: Char): Int = {
    var depth = 0
    var index = startIndex

    while (index < json.length) {
      json.charAt(index) match {
        case '"' =>
          parseJsonString(json, index) match {
            case Some((_, nextIndex)) =>
              index = nextIndex
            case None =>
              return -1
          }
        case ch if ch == openChar =>
          depth += 1
          index += 1
        case ch if ch == closeChar =>
          depth -= 1
          index += 1
          if (depth == 0) {
            return index
          }
        case _ =>
          index += 1
      }
    }

    -1
  }
}
