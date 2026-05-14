package io.github.jonggeun2001.privyspark.report

import io.github.jonggeun2001.privyspark.model.ScanResult
import io.github.jonggeun2001.privyspark.review.{RecordedFileFingerprint, ReviewScopeFingerprintCodec, ReviewScopeIdentifierCodec}
import io.github.jonggeun2001.privyspark.util.{DetectionMetricMath, PathIdentifiers}
import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.sql.functions.col

import scala.collection.mutable

private[privyspark] object ScanResultReportAggregator {
  def aggregateForReport(resultDf: DataFrame): DataFrame = {
    import resultDf.sparkSession.implicits._

    val schema = Encoders.product[ScanResult].schema
    resultDf
      .select(schema.fieldNames.map(name => col(name)): _*)
      .as[ScanResult]
      .groupByKey(result => ScanResultGroupKey(result.dataset_path, tableKeyForResult(result), result.column_name, result.pii_type))
      .mapGroups { case (groupKey, rows) => aggregateRows(groupKey, rows) }
      .toDF()
  }

  private final case class ScanResultGroupKey(
    datasetPath: String,
    tableKey: String,
    columnName: String,
    piiType: String
  )

  private final class Accumulator(groupKey: ScanResultGroupKey) {
    private var scanTimestamp = ""
    private var displayFileIdentifier: Option[String] = None
    private var originalFileIdentifier: Option[String] = None
    private var hiveTableFqn = ""
    private var matchCount = 0L
    private var sampledRowCount = 0L
    private var nonEmptyValueCount = 0L
    private var sampleRawValue = ""
    private var sampleMatchedFragment = ""
    private var fileSize = 0L
    private var fileMtimeEpochMs = 0L
    private var reviewStatus = "pending"
    private var reviewReason = ""
    private var reviewInvalidated = false
    private var hadScopeIdentifiers = false
    private val fileIdentifiers = mutable.LinkedHashSet.empty[String]
    private val partitionIdentifiers = mutable.LinkedHashSet.empty[String]
    private val fingerprintsByIdentifier = mutable.LinkedHashMap.empty[String, RecordedFileFingerprint]

    def add(result: ScanResult): Unit = {
      val resultHiveTableFqn = normalized(result.hive_table_fqn)
      val hiveMapped = resultHiveTableFqn.nonEmpty
      val resultFileIdentifiers = aggregationFileIdentifiers(result)
      if (scanTimestamp.isEmpty || normalized(result.scan_timestamp) > scanTimestamp) {
        scanTimestamp = normalized(result.scan_timestamp)
      }
      if (originalFileIdentifier.isEmpty) {
        originalFileIdentifier = Some(normalized(result.file_identifier))
      }
      if (hiveTableFqn.isEmpty && resultHiveTableFqn.nonEmpty) {
        hiveTableFqn = resultHiveTableFqn
      }
      if (displayFileIdentifier.isEmpty) {
        displayFileIdentifier = Some(displayIdentifierFor(result, hiveMapped, resultFileIdentifiers))
      }
      if (normalized(result.review_status).nonEmpty && reviewStatus == "pending") {
        reviewStatus = normalized(result.review_status)
      }
      if (reviewReason.isEmpty && normalized(result.review_reason).nonEmpty) {
        reviewReason = normalized(result.review_reason)
      }
      reviewInvalidated = reviewInvalidated || result.review_invalidated
      hadScopeIdentifiers = hadScopeIdentifiers || normalized(result.review_scope_file_identifiers).nonEmpty

      resultFileIdentifiers.foreach { fileIdentifier =>
        fileIdentifiers += fileIdentifier
        if (hiveMapped) {
          partitionIdentifierFor(fileIdentifier).foreach(partitionIdentifiers += _)
        }
      }
      decodedFingerprints(result).foreach { fingerprint =>
        if (!fingerprintsByIdentifier.contains(fingerprint.fileIdentifier)) {
          fingerprintsByIdentifier += fingerprint.fileIdentifier -> fingerprint
        }
      }

      matchCount += result.match_count
      sampledRowCount += result.sampled_row_count
      nonEmptyValueCount += effectiveNonEmptyValueCount(result)
      if (sampleRawValue.isEmpty && normalized(result.sample_raw_value).nonEmpty) {
        sampleRawValue = result.sample_raw_value
      }
      if (sampleMatchedFragment.isEmpty && normalized(result.sample_matched_fragment).nonEmpty) {
        sampleMatchedFragment = result.sample_matched_fragment
      }
      fileSize += result.file_size
      fileMtimeEpochMs = math.max(fileMtimeEpochMs, result.file_mtime_epoch_ms)
    }

    def toResult: ScanResult = {
      val aggregatedFileCount = math.max(1, fileIdentifiers.size)
      val aggregatedPartitionCount = if (hiveTableFqn.nonEmpty) partitionIdentifiers.size else 0
      val aggregated = hiveTableFqn.nonEmpty && (aggregatedFileCount > 1 || aggregatedPartitionCount > 1)
      val fileIdentifier = displayFileIdentifier.orElse(originalFileIdentifier).getOrElse(groupKey.tableKey)
      val scopeIdentifiers =
        if (hiveTableFqn.nonEmpty || aggregated || hadScopeIdentifiers) {
          ReviewScopeIdentifierCodec.encode(fileIdentifiers.toSeq.sorted)
        } else {
          ""
        }

      ScanResult(
        dataset_path = groupKey.datasetPath,
        scan_timestamp = scanTimestamp,
        file_identifier = fileIdentifier,
        column_name = groupKey.columnName,
        pii_type = groupKey.piiType,
        match_count = matchCount,
        sampled_row_count = sampledRowCount,
        non_empty_value_count = nonEmptyValueCount,
        match_ratio = DetectionMetricMath.ratio(matchCount, sampledRowCount),
        non_empty_match_ratio = DetectionMetricMath.ratio(matchCount, nonEmptyValueCount),
        confidence = DetectionMetricMath.wilsonLowerBound(matchCount, nonEmptyValueCount),
        sample_raw_value = sampleRawValue,
        sample_matched_fragment = sampleMatchedFragment,
        file_size = fileSize,
        file_mtime_epoch_ms = fileMtimeEpochMs,
        hive_table_fqn = hiveTableFqn,
        aggregated = aggregated,
        aggregated_file_count = aggregatedFileCount,
        aggregated_partition_count = aggregatedPartitionCount,
        review_status = reviewStatus,
        review_reason = reviewReason,
        review_invalidated = reviewInvalidated,
        review_scope_file_identifiers = scopeIdentifiers,
        review_scope_file_fingerprints = ReviewScopeFingerprintCodec.encode(fingerprintsByIdentifier.values.toSeq)
      )
    }
  }

  private def aggregateRows(groupKey: ScanResultGroupKey, rows: Iterator[ScanResult]): ScanResult = {
    val accumulator = new Accumulator(groupKey)
    rows.foreach(accumulator.add)
    accumulator.toResult
  }

  private def tableKeyForResult(result: ScanResult): String = {
    val hiveTableFqn = normalized(result.hive_table_fqn)
    if (hiveTableFqn.nonEmpty) hiveTableFqn else result.file_identifier
  }

  private def aggregationFileIdentifiers(result: ScanResult): Seq[String] = {
    val scopeIdentifiers = decodedScopeIdentifiers(result)
    val fingerprintIdentifiers = decodedFingerprints(result).map(_.fileIdentifier)
    val identifiers = ((scopeIdentifiers ++ fingerprintIdentifiers) :+ result.file_identifier)
      .map(normalized)
      .filter(_.nonEmpty)
      .distinct
    if (identifiers.nonEmpty) identifiers else Seq(result.file_identifier)
  }

  private def decodedScopeIdentifiers(result: ScanResult): Seq[String] =
    ReviewScopeIdentifierCodec.decode(result.review_scope_file_identifiers).fold(
      errorMessage => throw new IllegalArgumentException(errorMessage),
      identity
    )

  private def decodedFingerprints(result: ScanResult): Seq[RecordedFileFingerprint] =
    ReviewScopeFingerprintCodec.decode(result.review_scope_file_fingerprints).fold(
      errorMessage => throw new IllegalArgumentException(errorMessage),
      identity
    )

  private def displayIdentifierFor(
    result: ScanResult,
    hiveMapped: Boolean,
    aggregateFileIdentifiers: Seq[String]
  ): String = {
    if (!hiveMapped) {
      result.file_identifier
    } else {
      val candidates = (aggregateFileIdentifiers :+ result.file_identifier)
        .map(normalized)
        .filter(_.nonEmpty)
      candidates
        .find(identifier => PathIdentifiers.splitHiveLayoutIdentifier(identifier).partitionDepth > 0)
        .map(PathIdentifiers.stripTrailingHivePartitionSegments)
        .getOrElse(PathIdentifiers.stripTrailingHivePartitionSegments(result.file_identifier))
    }
  }

  private def partitionIdentifierFor(fileIdentifier: String): Option[String] = {
    val normalizedIdentifier = normalized(fileIdentifier).replace('\\', '/').replaceAll("/+$", "")
    val slashIndex = normalizedIdentifier.lastIndexOf('/')
    if (slashIndex <= 0) {
      None
    } else {
      val parent = normalizedIdentifier.substring(0, slashIndex)
      val split = PathIdentifiers.splitHiveLayoutIdentifier(parent)
      if (split.partitionDepth > 0) Some(parent) else None
    }
  }

  private def effectiveNonEmptyValueCount(result: ScanResult): Long =
    if (result.non_empty_value_count > 0L) result.non_empty_value_count
    else if (result.non_empty_match_ratio > 0.0 && result.match_count > 0L) {
      math.round(result.match_count.toDouble / result.non_empty_match_ratio)
    } else {
      result.sampled_row_count
    }

  private def normalized(value: String): String =
    Option(value).getOrElse("").trim
}
