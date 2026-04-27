package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.config.SuppressionSet
import io.github.jonggeun2001.privyspark.hive.HiveTableLookupIndex
import io.github.jonggeun2001.privyspark.model.{PiiRule, ProgressRun, ScanError, ScanGroup, ScanResult}
import io.github.jonggeun2001.privyspark.review.AllowlistMatcher
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

import scala.util.Random

private[privyspark] object GroupScanner {
  def scanGroups(
    spark: SparkSession,
    datasetPath: String,
    groups: Seq[ScanGroup],
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    groupParallelism: Int = -1,
    fileParallelism: Int = -1,
    fileSampleRatio: Option[Double] = None,
    fileSampleMinFiles: Int = 10,
    suppressions: SuppressionSet = SuppressionSet.empty,
    allowlistMatcher: AllowlistMatcher = AllowlistMatcher.empty,
    allowlistInputRoot: Option[String] = None,
    progressRun: Option[ProgressRun] = None,
    retainPayloads: Boolean = true,
    csvHeadCache: CsvHeadCache = new CsvHeadCache(),
    hiveLookup: Option[Broadcast[HiveTableLookupIndex]] = None
  ): Seq[(ScanGroup, Seq[ScanResult], Seq[ScanError])] =
    GroupScanCoordinator.scanGroups(
      spark,
      datasetPath,
      groups,
      rules,
      sampleRatio,
      timestamp,
      groupParallelism,
      fileParallelism,
      fileSampleRatio,
      fileSampleMinFiles,
      suppressions,
      allowlistMatcher,
      allowlistInputRoot,
      progressRun,
      retainPayloads,
      csvHeadCache,
      hiveLookup
    )

  def scanGroup(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    fileParallelism: Int = -1,
    fileSampleRatio: Option[Double] = None,
    fileSampleMinFiles: Int = 10,
    suppressions: SuppressionSet = SuppressionSet.empty,
    allowlistMatcher: AllowlistMatcher = AllowlistMatcher.empty,
    allowlistInputRoot: Option[String] = None,
    progressRun: Option[ProgressRun] = None,
    csvHeadCache: CsvHeadCache = new CsvHeadCache(),
    selectedSourceKeys: Option[Seq[String]] = None,
    hiveLookup: Option[Broadcast[HiveTableLookupIndex]] = None
  ): (Seq[ScanResult], Seq[ScanError]) =
    GroupScanCoordinator.scanGroup(
      spark,
      datasetPath,
      group,
      rules,
      sampleRatio,
      timestamp,
      fileParallelism,
      fileSampleRatio,
      fileSampleMinFiles,
      suppressions,
      allowlistMatcher,
      allowlistInputRoot,
      progressRun,
      csvHeadCache,
      selectedSourceKeys,
      hiveLookup
    )

  def scanGroupByFile(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    fileParallelism: Int = -1,
    suppressions: SuppressionSet = SuppressionSet.empty,
    allowlistMatcher: AllowlistMatcher = AllowlistMatcher.empty,
    allowlistInputRoot: Option[String] = None,
    progressRun: Option[ProgressRun] = None,
    csvHeadCache: CsvHeadCache = new CsvHeadCache(),
    fileSampleRatio: Option[Double] = None,
    fileSampleMinFiles: Int = 10,
    selectedSourceKeys: Option[Seq[String]] = None,
    hiveLookup: Option[Broadcast[HiveTableLookupIndex]] = None
  ): (Seq[ScanResult], Seq[ScanError]) =
    GroupScanCoordinator.scanGroupByFile(
      spark,
      datasetPath,
      group,
      rules,
      sampleRatio,
      timestamp,
      fileParallelism,
      suppressions,
      allowlistMatcher,
      allowlistInputRoot,
      progressRun,
      csvHeadCache,
      fileSampleRatio,
      fileSampleMinFiles,
      selectedSourceKeys,
      hiveLookup
    )

  def scanGroupBatch(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    fileSampleRatio: Option[Double] = None,
    fileSampleMinFiles: Int = 10,
    suppressions: SuppressionSet = SuppressionSet.empty,
    allowlistMatcher: AllowlistMatcher = AllowlistMatcher.empty,
    allowlistInputRoot: Option[String] = None,
    selectedSourceKeys: Option[Seq[String]] = None,
    hiveLookup: Option[Broadcast[HiveTableLookupIndex]] = None
  ): Seq[ScanResult] =
    GroupScanCoordinator.scanGroupBatch(
      spark,
      datasetPath,
      group,
      rules,
      sampleRatio,
      timestamp,
      fileSampleRatio,
      fileSampleMinFiles,
      suppressions,
      allowlistMatcher,
      allowlistInputRoot,
      selectedSourceKeys,
      hiveLookup = hiveLookup
    )

  def selectSampledFileKeys(fileKeys: Seq[String], fileSampleRatio: Double): Seq[String] = {
    require(fileKeys.nonEmpty, "fileKeys must not be empty")
    require(fileSampleRatio > 0.0 && fileSampleRatio <= 1.0, "fileSampleRatio must be > 0.0 and <= 1.0")

    val sampleSize = math.max(1, math.min(fileKeys.size, math.ceil(fileKeys.size * fileSampleRatio).toInt))
    val selectedKeySet = Random.shuffle(fileKeys.indices.toVector).take(sampleSize).map(fileKeys).toSet
    fileKeys.filter(selectedKeySet.contains)
  }
}
