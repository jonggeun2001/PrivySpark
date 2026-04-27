package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.model.ScanResult

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

private[privyspark] final case class ReviewEvidence(
  fileIdentifier: String,
  fileSize: Long,
  fileMtimeEpochMs: Long,
  fileChecksumAlgo: String,
  fileChecksum: String,
  sampleMatchedFragment: String,
  sampleRawValue: String,
  matchCount: Long,
  confidence: Double
)

private[privyspark] final case class ReviewFinding(
  scanPath: String,
  hiveDatabase: String,
  hiveTable: String,
  hiveTableFqn: String,
  columnName: String,
  piiType: String,
  matchCount: Long,
  sampledRowCount: Long,
  matchRatio: Double,
  nonEmptyMatchRatio: Double,
  confidence: Double,
  findingKey: String,
  findingHash: String,
  evidence: Seq[ReviewEvidence]
)

private[privyspark] object ReviewFindingBuilder {
  def fromScanResults(results: Seq[ScanResult]): Seq[ReviewFinding] = {
    results
      .groupBy(result => (result.dataset_path, result.hive_table_fqn, result.column_name, result.pii_type))
      .map {
        case ((scanPath, hiveTableFqn, columnName, piiType), groupedResults) =>
          val (hiveDatabase, hiveTable) = splitHiveTableFqn(hiveTableFqn)
          val findingKey = sha256(Seq(
            normalizeScanPath(scanPath),
            hiveDatabase,
            hiveTable,
            columnName,
            piiType
          ).mkString("|"))
          val evidence = groupedResults.flatMap(resultToEvidence).sortBy(e => (e.fileIdentifier, e.fileChecksum))
          val findingHash = sha256((Seq(findingKey) ++ evidence.map(e =>
            Seq(e.fileIdentifier, e.fileSize, e.fileMtimeEpochMs, e.fileChecksumAlgo, e.fileChecksum).mkString("|")
          )).mkString("|"))

          ReviewFinding(
            scanPath = scanPath,
            hiveDatabase = hiveDatabase,
            hiveTable = hiveTable,
            hiveTableFqn = hiveTableFqn,
            columnName = columnName,
            piiType = piiType,
            matchCount = groupedResults.map(_.match_count).sum,
            sampledRowCount = groupedResults.map(_.sampled_row_count).sum,
            matchRatio = groupedResults.map(_.match_ratio).max,
            nonEmptyMatchRatio = groupedResults.map(_.non_empty_match_ratio).max,
            confidence = groupedResults.map(_.confidence).max,
            findingKey = findingKey,
            findingHash = findingHash,
            evidence = evidence
          )
      }
      .toSeq
      .sortBy(finding => (finding.scanPath, finding.hiveTableFqn, finding.columnName, finding.piiType))
  }

  def scanResultsFingerprint(findings: Seq[ReviewFinding]): String =
    sha256(findings.map(finding => s"${finding.findingKey}|${finding.findingHash}").sorted.mkString("|"))

  def normalizeScanPath(scanPath: String): String =
    Option(scanPath).map(_.trim.stripSuffix("/")).getOrElse("")

  private def resultToEvidence(result: ScanResult): Seq[ReviewEvidence] = {
    val decodedFingerprints = ReviewScopeFingerprintCodec.decode(result.review_scope_file_fingerprints).getOrElse(Seq.empty)
    if (decodedFingerprints.nonEmpty) {
      decodedFingerprints.map { fingerprint =>
        ReviewEvidence(
          fileIdentifier = fingerprint.fileIdentifier,
          fileSize = fingerprint.fileSize,
          fileMtimeEpochMs = fingerprint.fileMtimeEpochMs,
          fileChecksumAlgo = fingerprint.fileChecksumAlgo,
          fileChecksum = fingerprint.fileChecksum,
          sampleMatchedFragment = result.sample_matched_fragment,
          sampleRawValue = result.sample_raw_value,
          matchCount = result.match_count,
          confidence = result.confidence
        )
      }
    } else {
      Seq(ReviewEvidence(
        fileIdentifier = result.file_identifier,
        fileSize = result.file_size,
        fileMtimeEpochMs = result.file_mtime_epoch_ms,
        fileChecksumAlgo = "",
        fileChecksum = "",
        sampleMatchedFragment = result.sample_matched_fragment,
        sampleRawValue = result.sample_raw_value,
        matchCount = result.match_count,
        confidence = result.confidence
      ))
    }
  }

  private def splitHiveTableFqn(hiveTableFqn: String): (String, String) = {
    val normalized = Option(hiveTableFqn).map(_.trim).getOrElse("")
    val delimiterIndex = normalized.lastIndexOf('.')
    if (delimiterIndex > 0 && delimiterIndex < normalized.length - 1) {
      normalized.substring(0, delimiterIndex) -> normalized.substring(delimiterIndex + 1)
    } else {
      "" -> normalized
    }
  }

  private def sha256(value: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    digest.digest(value.getBytes(StandardCharsets.UTF_8)).map(byte => "%02x".format(byte & 0xff)).mkString
  }
}
