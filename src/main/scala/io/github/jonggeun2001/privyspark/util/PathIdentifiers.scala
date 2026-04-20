package io.github.jonggeun2001.privyspark.util

import io.github.jonggeun2001.privyspark.model.{ScanGroup, ScanReadOptions}
import org.apache.hadoop.fs.Path

import scala.util.control.NonFatal

private[privyspark] object PathIdentifiers {
  private def stripTrailingSlash(path: String): String = {
    val normalized = Option(path).getOrElse("").replace('\\', '/')
    if (normalized == "/") normalized else normalized.replaceAll("/+$", "")
  }

  private def fallbackIdentifier(path: String): String = {
    val normalized = stripTrailingSlash(path)
    Option(new Path(normalized).getName).filter(_.nonEmpty).getOrElse(normalized)
  }

  def resolveRelativeIdentifier(datasetPath: String, targetPath: String): String = {
    resolveRelativeIdentifier(datasetPath, targetPath, useCurrentDirectoryMarker = false)
  }

  def resolveDirectoryIdentifier(datasetPath: String, directoryPath: String): String = {
    resolveRelativeIdentifier(datasetPath, directoryPath, useCurrentDirectoryMarker = true)
  }

  private def resolveRelativeIdentifier(
    datasetPath: String,
    targetPath: String,
    useCurrentDirectoryMarker: Boolean
  ): String = {
    val datasetUri = new Path(datasetPath).toUri.normalize()
    val targetUri = new Path(targetPath).toUri.normalize()

    val schemesCompatible =
      datasetUri.getScheme == null || targetUri.getScheme == null || datasetUri.getScheme == targetUri.getScheme
    val authoritiesCompatible =
      datasetUri.getAuthority == null || targetUri.getAuthority == null || datasetUri.getAuthority == targetUri.getAuthority

    val datasetComparablePath = stripTrailingSlash(Option(datasetUri.getPath).filter(_.nonEmpty).getOrElse(datasetPath))
    val targetComparablePath = stripTrailingSlash(Option(targetUri.getPath).filter(_.nonEmpty).getOrElse(targetPath))

    if (!schemesCompatible || !authoritiesCompatible) {
      fallbackIdentifier(targetPath)
    } else if (datasetComparablePath == targetComparablePath) {
      if (useCurrentDirectoryMarker) "." else fallbackIdentifier(targetPath)
    } else {
      val prefix = if (datasetComparablePath == "/") "/" else s"$datasetComparablePath/"
      if (targetComparablePath.startsWith(prefix)) {
        targetComparablePath.substring(prefix.length)
      } else {
        fallbackIdentifier(targetPath)
      }
    }
  }

  def comparableGroupingPath(path: String): String = {
    try {
      val uri = new Path(path).toUri.normalize()
      stripTrailingSlash(Option(uri.getPath).filter(_.nonEmpty).getOrElse(path))
    } catch {
      case NonFatal(_) =>
        stripTrailingSlash(path)
    }
  }

  def canonicalizePath(path: String): String = {
    val uri = new Path(path).toUri.normalize()
    val normalizedPath = stripTrailingSlash(Option(uri.getPath).filter(_.nonEmpty).getOrElse(path))
    normalizedPath
  }

  def comparablePathVariants(path: String): Set[String] = {
    val canonical = canonicalizePath(path)
    Set(
      canonical,
      canonical.replace("%2523", "%23"),
      canonical.replace("%23", "#"),
      canonical.replace("%2523", "#"),
      canonical.replace("#", "%23")
    )
  }

  def resolvePhysicalPath(group: ScanGroup, sourceKey: String): String = {
    group.physicalPathsByKey.getOrElse(sourceKey, sourceKey)
  }

  def resolveReadOptions(group: ScanGroup, sourceKey: String): ScanReadOptions = {
    group.readOptionsByKey.getOrElse(sourceKey, ScanReadOptions())
  }

  def resolveLogicalIdentifier(group: ScanGroup, datasetPath: String, sourceKey: String): String = {
    group.logicalIdentifiersByKey.getOrElse(
      sourceKey,
      resolveRelativeIdentifier(datasetPath, resolvePhysicalPath(group, sourceKey))
    )
  }

  def resolveLogicalIdentifierForPhysicalPath(
    group: ScanGroup,
    datasetPath: String,
    physicalPath: String
  ): String = {
    resolveSourceKeyForPhysicalPath(group, physicalPath) match {
      case Some(sourceKey) =>
        resolveLogicalIdentifier(group, datasetPath, sourceKey)
      case None =>
        resolveRelativeIdentifier(datasetPath, physicalPath)
    }
  }

  def resolveSourceKeyForPhysicalPath(
    group: ScanGroup,
    physicalPath: String
  ): Option[String] = {
    val canonicalPhysicalPath = canonicalizePath(physicalPath)
    val exactMatches = group.filePaths.filter { sourceKey =>
      canonicalizePath(resolvePhysicalPath(group, sourceKey)) == canonicalPhysicalPath
    }
    val matchingSourceKeys =
      if (exactMatches.nonEmpty) {
        exactMatches
      } else {
        val targetVariants = comparablePathVariants(physicalPath)
        group.filePaths.filter { sourceKey =>
          comparablePathVariants(resolvePhysicalPath(group, sourceKey)).exists(targetVariants.contains)
        }
      }

    matchingSourceKeys.distinct match {
      case Seq(sourceKey) =>
        Some(sourceKey)
      case Seq() =>
        None
      case multiple =>
        throw new IllegalStateException(
          s"Ambiguous logical identifier mapping for physical path: $physicalPath (${multiple.mkString(",")})"
        )
    }
  }
}
