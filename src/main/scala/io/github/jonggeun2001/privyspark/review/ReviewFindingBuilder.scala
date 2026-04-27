package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.model.ScanResult

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import scala.collection.mutable

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
  val DefaultMaxEvidenceSamples = 5

  def fromScanResults(results: Seq[ScanResult]): Seq[ReviewFinding] =
    fromScanResultsIterator(results.iterator, Int.MaxValue)

  def fromScanResultsIterator(results: Iterator[ScanResult], maxEvidencePerFinding: Int): Seq[ReviewFinding] = {
    val sampleLimit = math.max(0, maxEvidencePerFinding)
    val accumulators = mutable.Map.empty[ReviewFindingGroupKey, ReviewFindingAccumulator]

    results.foreach { result =>
      val groupKey = ReviewFindingGroupKey(result.dataset_path, result.hive_table_fqn, result.column_name, result.pii_type)
      val accumulator = accumulators.getOrElseUpdate(groupKey, new ReviewFindingAccumulator(groupKey, sampleLimit))
      accumulator.add(result)
    }

    accumulators.values.map(_.toFinding).toSeq
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

  private final case class ReviewFindingGroupKey(
    scanPath: String,
    hiveTableFqn: String,
    columnName: String,
    piiType: String
  )

  private final class ReviewFindingAccumulator(groupKey: ReviewFindingGroupKey, sampleLimit: Int) {
    private val (hiveDatabase, hiveTable) = splitHiveTableFqn(groupKey.hiveTableFqn)
    private val evidenceSamples = mutable.ArrayBuffer.empty[ReviewEvidence]
    private val evidenceHashParts = mutable.ArrayBuffer.empty[String]
    private var matchCount = 0L
    private var sampledRowCount = 0L
    private var matchRatio = 0.0
    private var nonEmptyMatchRatio = 0.0
    private var confidence = 0.0

    private val findingKey = sha256(Seq(
      normalizeScanPath(groupKey.scanPath),
      hiveDatabase,
      hiveTable,
      groupKey.columnName,
      groupKey.piiType
    ).mkString("|"))

    def add(result: ScanResult): Unit = {
      matchCount += result.match_count
      sampledRowCount += result.sampled_row_count
      matchRatio = math.max(matchRatio, result.match_ratio)
      nonEmptyMatchRatio = math.max(nonEmptyMatchRatio, result.non_empty_match_ratio)
      confidence = math.max(confidence, result.confidence)

      resultToEvidence(result).foreach { evidence =>
        evidenceHashParts += Seq(
          evidence.fileIdentifier,
          evidence.fileSize,
          evidence.fileMtimeEpochMs,
          evidence.fileChecksumAlgo,
          evidence.fileChecksum
        ).mkString("|")
        if (evidenceSamples.size < sampleLimit) {
          evidenceSamples += evidence
        }
      }
    }

    def toFinding: ReviewFinding = {
      val findingHash = sha256((Seq(findingKey) ++ evidenceHashParts.sorted).mkString("|"))
      ReviewFinding(
        scanPath = groupKey.scanPath,
        hiveDatabase = hiveDatabase,
        hiveTable = hiveTable,
        hiveTableFqn = groupKey.hiveTableFqn,
        columnName = groupKey.columnName,
        piiType = groupKey.piiType,
        matchCount = matchCount,
        sampledRowCount = sampledRowCount,
        matchRatio = matchRatio,
        nonEmptyMatchRatio = nonEmptyMatchRatio,
        confidence = confidence,
        findingKey = findingKey,
        findingHash = findingHash,
        evidence = evidenceSamples.toSeq.sortBy(e => (e.fileIdentifier, e.fileChecksum))
      )
    }
  }
}
