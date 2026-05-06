package io.github.jonggeun2001.privyspark.review

import java.util.Locale

private[privyspark] object ReviewPathNormalizer {
  def normalizeScanPath(scanPath: String): String = {
    val trimmed = Option(scanPath).map(_.trim).getOrElse("")
    if (trimmed.isEmpty) {
      ""
    } else if (trimmed.toLowerCase(Locale.ROOT).startsWith("hdfs://")) {
      normalizeHdfsPath(trimmed)
    } else {
      trimmed.stripSuffix("/")
    }
  }

  private def normalizeHdfsPath(path: String): String = {
    val rest = path.substring("hdfs://".length)
    val (authority, rawPath) =
      if (rest.startsWith("/")) {
        "" -> rest
      } else {
        val pathStart = rest.indexOf('/')
        if (pathStart < 0) {
          rest -> "/"
        } else {
          rest.substring(0, pathStart) -> rest.substring(pathStart)
        }
      }
    val normalizedPath = stripTrailingSlashes(collapseSlashes(rawPath))
    if (authority.isEmpty) {
      s"hdfs://$normalizedPath"
    } else {
      s"hdfs://$authority$normalizedPath"
    }
  }

  private def collapseSlashes(path: String): String = {
    val withLeadingSlash = if (path.isEmpty || !path.startsWith("/")) s"/$path" else path
    withLeadingSlash.replaceAll("/{2,}", "/")
  }

  private def stripTrailingSlashes(path: String): String = {
    val withoutTrailing = path.replaceAll("/+$", "")
    if (withoutTrailing.isEmpty) "/" else withoutTrailing
  }
}
