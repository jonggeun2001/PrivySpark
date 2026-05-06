package io.github.jonggeun2001.privyspark.review

private[privyspark] object ReviewWorkbookLayout {
  val ReviewSheetName = "review"
  val MetadataSheetName = "_metadata"
  val HeaderRowIndex = 4
  val FirstDataRowIndex = HeaderRowIndex + 1
  val ResponderRowIndex = 2
  val ResponderColumnIndex = 1
  val PermanentFalsePositiveExpiresAt = "9999-12-31"

  final case class Column(header: String, fieldName: String, hidden: Boolean, width: Int)

  val DecisionColumnIndex = 8
  val FalsePositiveReasonColumnIndex = 10
  val ActionPlanColumnIndex = 11
  val ActionDueDateColumnIndex = 12

  val Columns: Seq[Column] = Seq(
    Column("경로", "path", hidden = false, width = 32),
    Column("Hive 테이블", "hive", hidden = false, width = 24),
    Column("컬럼명", "column", hidden = false, width = 22),
    Column("개인정보 유형", "pii", hidden = false, width = 18),
    Column("샘플 행 수", "sample_row_count", hidden = false, width = 14),
    Column("검출 건수", "match_count", hidden = false, width = 14),
    Column("검출비율(%)", "non_empty_match_ratio_percent", hidden = false, width = 14),
    Column("검출샘플(검출값/데이터)", "sample", hidden = false, width = 42),
    Column("판정", "decision", hidden = false, width = 14),
    Column("기존 조치 상태", "existing_action_status", hidden = false, width = 24),
    Column("오탐 사유", "false_positive_reason", hidden = false, width = 36),
    Column("정탐 조치 계획", "action_plan", hidden = false, width = 36),
    Column("조치 예정일", "action_due_date", hidden = false, width = 16),
    Column("scan_path", "scan_path", hidden = true, width = 18),
    Column("finding_key", "finding_key", hidden = true, width = 18),
    Column("finding_hash", "finding_hash", hidden = true, width = 18),
    Column("file_identifier", "file_identifier", hidden = true, width = 18),
    Column("hive_database", "hive_database", hidden = true, width = 18),
    Column("hive_table", "hive_table", hidden = true, width = 18),
    Column("hive_table_fqn", "hive_table_fqn", hidden = true, width = 18),
    Column("column_name", "column_name", hidden = true, width = 18),
    Column("pii_type", "pii_type", hidden = true, width = 18),
    Column("sample_row_count_raw", "sample_row_count_raw", hidden = true, width = 18),
    Column("match_count_raw", "match_count_raw", hidden = true, width = 18),
    Column("non_empty_match_ratio_raw", "non_empty_match_ratio_raw", hidden = true, width = 18)
  )

  val HiddenColumnIndexByField: Map[String, Int] =
    Columns.zipWithIndex.collect { case (column, index) if column.hidden => column.fieldName -> index }.toMap
}
