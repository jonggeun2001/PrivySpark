package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.model.ScanGroup
import io.github.jonggeun2001.privyspark.scan.SourceExpansion.supportsBatchScan

private[privyspark] sealed trait GroupScanRoute

private[privyspark] object GroupScanRoute {
  case object SampledExact extends GroupScanRoute
  case object FileScan extends GroupScanRoute
  case object BatchScan extends GroupScanRoute
}

private[privyspark] object GroupScanRouter {
  def routeOf(group: ScanGroup): GroupScanRoute = {
    if (!supportsBatchScan(group) || group.useDirectoryIdentifier) {
      GroupScanRoute.FileScan
    } else if (group.schemaSampled && group.filePaths.size > 1 && requiresExactPerFileSchema(group)) {
      GroupScanRoute.SampledExact
    } else {
      GroupScanRoute.BatchScan
    }
  }

  private def requiresExactPerFileSchema(group: ScanGroup): Boolean =
    group.format == "json" || group.format == "csv"
}
