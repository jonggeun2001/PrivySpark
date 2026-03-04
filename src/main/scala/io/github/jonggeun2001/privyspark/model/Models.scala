package io.github.jonggeun2001.privyspark.model

final case class PiiRule(piiType: String, regex: String)

final case class ScanResult(
  dataset_path: String,
  scan_timestamp: String,
  file_identifier: String,
  column_name: String,
  pii_type: String,
  match_count: Long,
  match_ratio: Double,
  confidence: Double
)

final case class ScanError(
  dataset_path: String,
  scan_timestamp: String,
  file_identifier: String,
  error_message: String
)
