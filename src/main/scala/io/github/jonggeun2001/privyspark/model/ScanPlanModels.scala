package io.github.jonggeun2001.privyspark.model

private[privyspark] final case class ScanReadOptions(sheetName: Option[String] = None)

private[privyspark] final case class ScanFileEntry(
  sourceKey: String,
  physicalPath: String,
  directoryPath: String,
  format: String,
  logicalIdentifier: String,
  fileSize: Long = 0L,
  fileMtimeEpochMs: Long = 0L,
  readOptions: ScanReadOptions = ScanReadOptions(),
  allowDirectoryIdentifier: Boolean = true
)

private[privyspark] final case class ScanGroup(
  directoryPath: String,
  format: String,
  schemaSignature: String,
  filePaths: Seq[String],
  useDirectoryIdentifier: Boolean = false,
  directoryIdentifierEligible: Boolean = false,
  schemaSampled: Boolean = false,
  csvHasHeader: Boolean = true,
  physicalPathsByKey: Map[String, String] = Map.empty,
  logicalIdentifiersByKey: Map[String, String] = Map.empty,
  fileSizesByKey: Map[String, Long] = Map.empty,
  fileMtimesByKey: Map[String, Long] = Map.empty,
  readOptionsByKey: Map[String, ScanReadOptions] = Map.empty,
  allowDirectoryIdentifier: Boolean = true
)

private[privyspark] final case class DirectoryScanPlan(
  groups: Seq[ScanGroup],
  errors: Seq[ScanError],
  totalFiles: Int,
  directoryCount: Int,
  ignoredFiles: Int = 0,
  stagingPaths: Seq[String] = Seq.empty
)

private[privyspark] final case class FileScanMetrics(
  fileIdentifier: String,
  sampledRowCount: Long,
  nonEmptyValueCounts: Map[String, Long],
  matchCounts: Seq[MatchCount],
  sampleValues: Map[String, SampleValue],
  fileSize: Long,
  fileMtimeEpochMs: Long,
  scanTimestamp: String
)

private[privyspark] final case class CachedSchemaSignature(signature: String, csvHasHeader: Boolean)

private[privyspark] final case class ProbeSample(bytes: Array[Byte], truncated: Boolean)

private[privyspark] final case class PreScanFileOutcome(
  filePath: String,
  groupingDirectoryPath: String,
  preScanErrorScope: String,
  expandedEntries: Seq[ScanFileEntry],
  expandedErrors: Seq[ScanError],
  ignoredEntries: Int = 0,
  stagingPaths: Seq[String],
  pathInferredFormat: Option[String] = None,
  probeRequired: Boolean = false,
  skipped: Boolean = false,
  failure: Option[Throwable] = None
)

private[privyspark] final case class ProgressRun(
  runId: String,
  rootPath: String,
  runPath: String,
  activeRunPath: String,
  datasetPath: String,
  outputRoot: String,
  scanTimestamp: String,
  resultsPath: String,
  errorsPath: String,
  metaPath: String,
  completionsPath: String
)

private[privyspark] final case class ActiveRunMarker(runId: String, state: String, lastHeartbeatEpochMillis: Long)

private[privyspark] final case class ProgressRunMetadata(runId: String, state: String)

private[privyspark] final case class ReportFormatPaths(format: String, rootPath: String, resultPath: String, errorPath: String)
