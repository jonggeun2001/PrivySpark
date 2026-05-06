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
  fileIdentifier: String,
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
  fingerprintComplete: Boolean,
  hasMultipleFileEvidence: Boolean,
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
      val groupKey = ReviewFindingGroupKey(result.dataset_path, result.file_identifier, result.column_name, result.pii_type)
      val accumulator = accumulators.getOrElseUpdate(groupKey, new ReviewFindingAccumulator(groupKey, sampleLimit))
      accumulator.add(result)
    }

    accumulators.values.map(_.toFinding).toSeq
      .sortBy(finding => (finding.scanPath, finding.fileIdentifier, finding.columnName, finding.piiType))
  }

  def scanResultsFingerprint(findings: Seq[ReviewFinding]): String =
    sha256(findings.map(finding => s"${finding.findingKey}|${finding.findingHash}").sorted.mkString("|"))

  def findingKeyForResult(result: ScanResult): String = {
    findingKeyForFields(result.dataset_path, result.file_identifier, result.column_name, result.pii_type)
  }

  def evidenceFromScanResult(result: ScanResult): Seq[ReviewEvidence] =
    resultToEvidence(result).sortBy(e => (e.fileIdentifier, e.fileChecksum))

  def normalizeScanPath(scanPath: String): String =
    ReviewPathNormalizer.normalizeScanPath(scanPath)

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
    fileIdentifier: String,
    columnName: String,
    piiType: String
  )

  private final class ReviewFindingAccumulator(groupKey: ReviewFindingGroupKey, sampleLimit: Int) {
    private val evidenceSamples = mutable.ArrayBuffer.empty[ReviewEvidence]
    private val evidenceDigest = MessageDigest.getInstance("SHA-256")
    private var hiveTableFqn = ""
    private var hiveDatabase = ""
    private var hiveTable = ""
    private var matchCount = 0L
    private var sampledRowCount = 0L
    private var matchRatio = 0.0
    private var nonEmptyMatchRatio = 0.0
    private var confidence = 0.0
    private var fingerprintComplete = true
    private var firstFileIdentifier: Option[String] = None
    private var hasMultipleFileEvidence = false

    private val findingKey = findingKeyForFields(
      groupKey.scanPath,
      groupKey.fileIdentifier,
      groupKey.columnName,
      groupKey.piiType
    )

    def add(result: ScanResult): Unit = {
      val resultHiveTableFqn = Option(result.hive_table_fqn).map(_.trim).getOrElse("")
      if (hiveTableFqn.trim.isEmpty && resultHiveTableFqn.nonEmpty) {
        hiveTableFqn = resultHiveTableFqn
        val split = splitHiveTableFqn(hiveTableFqn)
        hiveDatabase = split._1
        hiveTable = split._2
      }
      matchCount += result.match_count
      sampledRowCount += result.sampled_row_count
      matchRatio = math.max(matchRatio, result.match_ratio)
      nonEmptyMatchRatio = math.max(nonEmptyMatchRatio, result.non_empty_match_ratio)
      confidence = math.max(confidence, result.confidence)

      evidenceFromScanResult(result).foreach { evidence =>
        updateDigest(evidenceDigest, evidenceHashPart(result, evidence))
        if (evidence.fileChecksum.trim.isEmpty) {
          fingerprintComplete = false
        }
        firstFileIdentifier match {
          case Some(first) if first != evidence.fileIdentifier =>
            hasMultipleFileEvidence = true
          case None =>
            firstFileIdentifier = Some(evidence.fileIdentifier)
          case _ =>
        }
        if (evidenceSamples.size < sampleLimit) {
          evidenceSamples += evidence
        }
      }
    }

    def toFinding: ReviewFinding = {
      val findingHash = sha256(s"$findingKey|${bytesToHex(evidenceDigest.digest())}")
      ReviewFinding(
        scanPath = groupKey.scanPath,
        fileIdentifier = groupKey.fileIdentifier,
        hiveDatabase = hiveDatabase,
        hiveTable = hiveTable,
        hiveTableFqn = hiveTableFqn,
        columnName = groupKey.columnName,
        piiType = groupKey.piiType,
        matchCount = matchCount,
        sampledRowCount = sampledRowCount,
        matchRatio = matchRatio,
        nonEmptyMatchRatio = nonEmptyMatchRatio,
        confidence = confidence,
        findingKey = findingKey,
        findingHash = findingHash,
        fingerprintComplete = fingerprintComplete,
        hasMultipleFileEvidence = hasMultipleFileEvidence,
        evidence = evidenceSamples.toSeq.sortBy(e => (e.fileIdentifier, e.fileChecksum))
      )
    }
  }

  private def evidenceHashPart(result: ScanResult, evidence: ReviewEvidence): Seq[String] = Seq(
    result.scan_timestamp,
    evidence.fileIdentifier,
    evidence.fileSize.toString,
    evidence.fileMtimeEpochMs.toString,
    evidence.fileChecksumAlgo,
    evidence.fileChecksum,
    result.match_count.toString,
    result.sampled_row_count.toString,
    result.match_ratio.toString,
    result.non_empty_match_ratio.toString,
    result.confidence.toString
  )

  private def findingKeyForFields(
    scanPath: String,
    fileIdentifier: String,
    columnName: String,
    piiType: String
  ): String =
    sha256(Seq(
      normalizeScanPath(scanPath),
      fileIdentifier,
      columnName,
      piiType
    ).mkString("|"))

  private def updateDigest(digest: MessageDigest, values: Seq[String]): Unit = {
    values.foreach { value =>
      val bytes = Option(value).getOrElse("").getBytes(StandardCharsets.UTF_8)
      digest.update(bytes.length.toString.getBytes(StandardCharsets.UTF_8))
      digest.update(0.toByte)
      digest.update(bytes)
      digest.update(0.toByte)
    }
  }

  private def bytesToHex(bytes: Array[Byte]): String =
    bytes.map(byte => "%02x".format(byte & 0xff)).mkString
}
