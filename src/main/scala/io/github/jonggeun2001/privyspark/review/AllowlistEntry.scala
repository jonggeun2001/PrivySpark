package io.github.jonggeun2001.privyspark.review

final case class AllowlistKey(
  datasetPath: String,
  fileIdentifier: String,
  columnName: String,
  piiType: String
)

final case class AllowlistEntry(
  datasetPath: String,
  fileIdentifier: String,
  columnName: String,
  piiType: String,
  reason: String,
  reviewer: String,
  reviewedAt: String,
  sourceRunId: String,
  fileSize: Long,
  fileMtimeEpochMs: Long,
  fileChecksumAlgo: String,
  fileChecksum: String
) {
  def key: AllowlistKey = AllowlistKey(datasetPath, fileIdentifier, columnName, piiType)
}

final case class ResolvedFileFingerprint(
  fileIdentifier: String,
  physicalPath: String,
  fileSize: Long,
  fileMtimeEpochMs: Long,
  fileChecksumAlgo: String,
  fileChecksum: String
)

final case class RecordedFileFingerprint(
  fileIdentifier: String,
  fileSize: Long,
  fileMtimeEpochMs: Long,
  fileChecksumAlgo: String,
  fileChecksum: String
)

object RecordedFileFingerprint {
  def fromResolved(fingerprint: ResolvedFileFingerprint): RecordedFileFingerprint =
    RecordedFileFingerprint(
      fileIdentifier = fingerprint.fileIdentifier,
      fileSize = fingerprint.fileSize,
      fileMtimeEpochMs = fingerprint.fileMtimeEpochMs,
      fileChecksumAlgo = fingerprint.fileChecksumAlgo,
      fileChecksum = fingerprint.fileChecksum
    )
}

final case class AllowlistEvaluation(
  shouldSuppress: Boolean,
  reviewStatus: String = ReviewStatus.Pending,
  reviewReason: String = "",
  reviewInvalidated: Boolean = false
)

object ReviewStatus {
  val Pending = "pending"
  val FalsePositive = "false_positive"
  val TruePositive = "true_positive"

  val Supported: Set[String] = Set(Pending, FalsePositive, TruePositive)

  def normalize(rawValue: String): Option[String] = {
    Option(rawValue).map(_.trim.toLowerCase).filter(Supported.contains)
  }
}
