package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.report.JsonCodec.jsonString

private[privyspark] object AllowlistJson {
  def exactEntryToJson(entry: AllowlistEntry): String =
    s"""{"dataset_path":${jsonString(entry.datasetPath)},"file_identifier":${jsonString(entry.fileIdentifier)},"column_name":${jsonString(entry.columnName)},"pii_type":${jsonString(entry.piiType)},"reason":${jsonString(entry.reason)},"reviewer":${jsonString(entry.reviewer)},"reviewed_at":${jsonString(entry.reviewedAt)},"source_run_id":${jsonString(entry.sourceRunId)},"file_size":${entry.fileSize},"file_mtime_epoch_ms":${entry.fileMtimeEpochMs},"file_checksum_algo":${jsonString(entry.fileChecksumAlgo)},"file_checksum":${jsonString(entry.fileChecksum)}}"""

  def patternEntryToJson(entry: PatternAllowlistEntry): String =
    s"""{"entry_type":"pattern","dataset_path":${jsonString(entry.datasetPath)},"file_identifier_pattern":${jsonString(entry.fileIdentifierPattern)},"column_name_pattern":${jsonString(entry.columnNamePattern)},"pii_type_pattern":${jsonString(entry.piiTypePattern)},"reason":${jsonString(entry.reason)},"reviewer":${jsonString(entry.reviewer)},"reviewed_at":${jsonString(entry.reviewedAt)},"expires_at":${jsonString(entry.expiresAt)},"source_finding_key":${jsonString(entry.sourceFindingKey)}}"""
}
