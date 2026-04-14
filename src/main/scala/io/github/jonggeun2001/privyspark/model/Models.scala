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

final case class ScanResult(
  dataset_path: String,
  scan_timestamp: String,
  file_identifier: String,
  column_name: String,
  pii_type: String,
  match_count: Long,
  sampled_row_count: Long,
  match_ratio: Double,
  non_null_match_ratio: Double,
  confidence: Double
)

final case class ScanError(
  dataset_path: String,
  scan_timestamp: String,
  file_identifier: String,
  error_message: String
)
