package io.github.jonggeun2001.privyspark.review.collect

import io.github.jonggeun2001.privyspark.report.JsonCodec.{extractJsonLongField, extractJsonObjectArrayField, extractJsonStringField}
import io.github.jonggeun2001.privyspark.review.{RejectedResponse, ResponseEnvelope, ResponseItem}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import scala.util.Try

private[privyspark] object ResponseEnvelopeReader {
  def readResponseEnvelopes(conf: Configuration, inboxPath: String): Seq[Either[RejectedResponse, ResponseEnvelope]] = {
    val path = new Path(inboxPath)
    val fs = path.getFileSystem(conf)
    if (!fs.exists(path)) {
      Seq.empty
    } else {
      fs.listStatus(path)
        .filter(status => status.isFile && status.getPath.getName.endsWith(".json"))
        .toSeq
        .sortBy(_.getPath.toString)
        .map { status =>
          val sourcePath = status.getPath.toString
          parseEnvelope(sourcePath, readText(conf, sourcePath)).left.map(reason => RejectedResponse(sourcePath, reason))
        }
    }
  }

  private[privyspark] def parseEnvelope(sourcePath: String, json: String): Either[String, ResponseEnvelope] = {
    val normalizedJson = stripLeadingBom(json)
    val schemaVersion = extractJsonStringField(normalizedJson, "schema_version").orElse(extractNumericField(normalizedJson, "schema_version")).getOrElse("")
    if (schemaVersion != "1") {
      Left(s"unsupported schema_version: $schemaVersion")
    } else {
      val responseObjects = extractJsonObjectArrayField(normalizedJson, "responses").getOrElse(Seq.empty)
      Right(ResponseEnvelope(
        sourcePath = sourcePath,
        scanPath = extractJsonStringField(normalizedJson, "scan_path").getOrElse(""),
        responder = extractJsonStringField(normalizedJson, "responder").getOrElse(""),
        respondedAt = extractJsonStringField(normalizedJson, "responded_at").getOrElse(""),
        responses = responseObjects.map(parseItem)
      ))
    }
  }

  private def stripLeadingBom(value: String): String =
    if (value.nonEmpty && value.charAt(0) == '\uFEFF') value.substring(1) else value

  private def parseItem(json: String): ResponseItem = {
    val hiveTableFqn = extractJsonStringField(json, "hive_table_fqn").getOrElse("")
    val (fallbackDatabase, fallbackTable) = splitHiveTableFqn(hiveTableFqn)
    ResponseItem(
      findingKey = extractJsonStringField(json, "finding_key").getOrElse(""),
      findingHash = extractJsonStringField(json, "finding_hash").getOrElse(""),
      fileIdentifier = extractJsonStringField(json, "file_identifier").getOrElse(""),
      fileIdentifierPattern = extractJsonStringField(json, "file_identifier_pattern").getOrElse(""),
      hiveDatabase = extractJsonStringField(json, "hive_database").getOrElse(fallbackDatabase),
      hiveTable = extractJsonStringField(json, "hive_table").getOrElse(fallbackTable),
      hiveTableFqn = hiveTableFqn,
      columnName = extractJsonStringField(json, "column_name").getOrElse(""),
      piiType = extractJsonStringField(json, "pii_type").getOrElse(""),
      sampleRowCount = extractJsonLongField(json, "sample_row_count")
        .orElse(extractJsonLongField(json, "sampled_row_count"))
        .getOrElse(0L),
      matchCount = extractJsonLongField(json, "match_count").getOrElse(0L),
      nonEmptyMatchRatio = extractJsonDoubleField(json, "non_empty_match_ratio").getOrElse(0.0),
      decision = extractJsonStringField(json, "decision").getOrElse(""),
      falsePositiveReason = extractJsonStringField(json, "false_positive_reason").getOrElse(""),
      allowlistScope = extractJsonStringField(json, "allowlist_scope").getOrElse(""),
      expiresAt = extractJsonStringField(json, "expires_at").getOrElse(""),
      actionPlan = extractJsonStringField(json, "action_plan").getOrElse(""),
      actionDueDate = extractJsonStringField(json, "action_due_date").getOrElse("")
    )
  }

  private[privyspark] def splitHiveTableFqn(hiveTableFqn: String): (String, String) = {
    val normalized = Option(hiveTableFqn).map(_.trim).getOrElse("")
    val delimiterIndex = normalized.lastIndexOf('.')
    if (delimiterIndex > 0 && delimiterIndex < normalized.length - 1) {
      normalized.substring(0, delimiterIndex) -> normalized.substring(delimiterIndex + 1)
    } else {
      "" -> normalized
    }
  }

  private def extractNumericField(json: String, field: String): Option[String] = {
    val pattern = ("\"" + field + "\"\\s*:\\s*([0-9]+)").r
    pattern.findFirstMatchIn(json).map(_.group(1))
  }

  private def extractJsonDoubleField(json: String, field: String): Option[Double] = {
    val pattern = ("\"" + field + "\"\\s*:\\s*(-?[0-9]+(?:\\.[0-9]+)?)").r
    pattern.findFirstMatchIn(json).flatMap(matchResult => Try(matchResult.group(1).toDouble).toOption)
  }

  private def readText(conf: Configuration, path: String): String = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(conf)
    val reader = new BufferedReader(new InputStreamReader(fs.open(hadoopPath), StandardCharsets.UTF_8))
    val builder = new StringBuilder
    try {
      var line = reader.readLine()
      while (line != null) {
        builder.append(line).append('\n')
        line = reader.readLine()
      }
    } finally {
      reader.close()
    }
    builder.toString()
  }
}
