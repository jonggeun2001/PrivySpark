package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.util.PathIdentifiers.canonicalizePath
import org.apache.hadoop.fs.Path

private[privyspark] object ArchiveStaging {
  val ZipFormat = "zip"
  val JarFormat = "jar"
  val TarFormat = "tar"
  val SevenZFormat = "7z"
  val RarFormat = "rar"
  val ArchiveFormats = Set(ZipFormat, JarFormat, TarFormat, SevenZFormat, RarFormat)
  val MaxArchiveExpansionDepth = 1

  def normalizeArchiveEntryName(entryName: String): String = {
    Option(entryName).getOrElse("").replace('\\', '/')
  }

  def safeResolveArchiveEntryPath(root: Path, entryName: String): Option[Path] = {
    val sanitizedEntryName = normalizeArchiveEntryName(entryName)
    val pathSegments = sanitizedEntryName.split('/').filter(_.nonEmpty)
    if (pathSegments.isEmpty || pathSegments.exists(segment => segment == "." || segment == "..")) {
      return None
    }
    val resolvedPath = new Path(root, sanitizedEntryName)
    val rootComparable = canonicalizePath(root.toString)
    val resolvedComparable = canonicalizePath(resolvedPath.toString)
    if (resolvedComparable == rootComparable || resolvedComparable.startsWith(s"$rootComparable/")) Some(resolvedPath) else None
  }

  def ensureArchiveEntryParent(
    fs: org.apache.hadoop.fs.FileSystem,
    targetPath: Path
  ): Either[String, Unit] = {
    Option(targetPath.getParent) match {
      case None => Right(())
      case Some(parent) if fs.exists(parent) && fs.getFileStatus(parent).isDirectory => Right(())
      case Some(parent) if fs.exists(parent) => Left(s"Archive entry parent is not a directory: ${parent.toString}")
      case Some(parent) if fs.mkdirs(parent) => Right(())
      case Some(parent) => Left(s"Archive entry parent creation failed: ${parent.toString}")
    }
  }
}
