package io.github.jonggeun2001.privyspark.scan.discovery

private[privyspark] final case class DiscoveredFile(
  path: String,
  size: Long,
  mtimeEpochMs: Long
)
