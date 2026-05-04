package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.model.{ScanError, ScanGroup, ScanResult}
import io.github.jonggeun2001.privyspark.util.DriverLogger

private[privyspark] object GroupScanFallbackPolicy {
  def fallback(
    group: ScanGroup,
    error: Throwable,
    schemaSampledFn: () => (Seq[ScanResult], Seq[ScanError]),
    fileScanFn: () => (Seq[ScanResult], Seq[ScanError])
  ): (Seq[ScanResult], Seq[ScanError]) = {
    val errorMessage = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
    DriverLogger.warn(
      "group_scan_fallback",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "files" -> group.filePaths.size,
      "reason" -> errorMessage
    )
    DriverLogger.debug(
      "group_scan_fallback_requested",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "files" -> group.filePaths.size,
      "reason" -> errorMessage
    )
    if (group.schemaSampled) {
      DriverLogger.warn(
        "group_scan_fallback_execute",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "schema" -> group.schemaSignature,
        "files" -> group.filePaths.size,
        "mode" -> "schema_resplit"
      )
      val exactSplitResult = schemaSampledFn()
      DriverLogger.debug(
        "group_scan_complete",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "schema" -> group.schemaSignature,
        "result_rows" -> exactSplitResult._1.size,
        "error_rows" -> exactSplitResult._2.size,
        "mode" -> "fallback_schema_resplit"
      )
      exactSplitResult
    } else {
      val fallbackResult = fileScanFn()
      DriverLogger.debug(
        "group_scan_complete",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "schema" -> group.schemaSignature,
        "result_rows" -> fallbackResult._1.size,
        "error_rows" -> fallbackResult._2.size,
        "mode" -> "fallback_file_scan"
      )
      fallbackResult
    }
  }
}
