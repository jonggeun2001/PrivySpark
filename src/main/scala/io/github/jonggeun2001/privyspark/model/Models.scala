package io.github.jonggeun2001.privyspark.model

object PiiRuleMatchType {
  val Value = "value"
  val FullColumn = "full_column"

  val Supported: Set[String] = Set(Value, FullColumn)

  def normalize(rawValue: String): Option[String] = {
    Option(rawValue).map(_.trim.toLowerCase).filter(Supported.contains)
  }
}

final case class PiiRule(
  piiType: String,
  regex: String,
  columnHints: Seq[String] = Seq.empty,
  matchType: String = PiiRuleMatchType.Value
)

final case class Suppression(columnName: String, piiType: String)

private[privyspark] final case class MatchCount(
  columnName: String,
  piiType: String,
  count: Long,
  metricAlias: String = ""
)

private[privyspark] final case class SampleValue(sampleRawValue: String, sampleMatchedFragment: String)

final case class ScanResult(
  dataset_path: String,
  scan_timestamp: String,
  file_identifier: String,
  column_name: String,
  pii_type: String,
  match_count: Long,
  sampled_row_count: Long,
  non_empty_value_count: Long = 0L,
  match_ratio: Double,
  non_empty_match_ratio: Double,
  confidence: Double,
  sample_raw_value: String,
  sample_matched_fragment: String,
  file_size: Long = 0L,
  file_mtime_epoch_ms: Long = 0L,
  hive_table_fqn: String = "",
  aggregated: Boolean = false,
  aggregated_file_count: Int = 1,
  aggregated_partition_count: Int = 0,
  review_status: String = "pending",
  review_reason: String = "",
  review_invalidated: Boolean = false,
  review_scope_file_identifiers: String = "",
  review_scope_file_fingerprints: String = ""
)

final case class ScanError(
  dataset_path: String,
  scan_timestamp: String,
  file_identifier: String,
  error_message: String
)
