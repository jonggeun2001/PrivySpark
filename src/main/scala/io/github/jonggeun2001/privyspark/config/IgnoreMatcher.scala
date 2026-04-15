package io.github.jonggeun2001.privyspark.config

import org.apache.hadoop.fs.Path

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.regex.Pattern
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

final class IgnoreMatcher private (patterns: Seq[IgnoreMatcher.CompiledPattern]) {
  import IgnoreMatcher._

  def matched(targetPath: String, rootPath: String, isDirectory: Boolean = false): Option[String] = {
    if (patterns.isEmpty) {
      None
    } else {
      val relativePath = normalizeForMatching(resolveRelativePath(rootPath, targetPath))
      val pathCandidates = buildPathCandidates(relativePath, isDirectory)
      val basenameCandidates = pathCandidates.map(pathBasename).filter(_.nonEmpty).distinct

      patterns.collectFirst {
        case pattern if pattern.matches(pathCandidates, basenameCandidates, isDirectory) => pattern.original
      }
    }
  }

  def isEmpty: Boolean = patterns.isEmpty
}

object IgnoreMatcher {
  private sealed trait MatchMode
  private case object BasenameMode extends MatchMode
  private case object PathMode extends MatchMode

  private final case class CompiledPattern(
    original: String,
    mode: MatchMode,
    regex: Pattern,
    requiresDirectory: Boolean = false
  ) {
    def matches(pathCandidates: Seq[String], basenameCandidates: Seq[String], isDirectory: Boolean): Boolean = {
      if (requiresDirectory && !isDirectory) {
        return false
      }
      val candidates = mode match {
        case BasenameMode => basenameCandidates
        case PathMode => pathCandidates
      }
      candidates.exists(candidate => regex.matcher(candidate).matches())
    }
  }

  val empty: IgnoreMatcher = new IgnoreMatcher(Seq.empty)

  def fromSources(inline: Seq[String], file: Option[String]): IgnoreMatcher = {
    val mergedPatterns = inline.flatMap(normalizePattern) ++ file.toSeq.flatMap(loadIgnoreFile)
    new IgnoreMatcher(mergedPatterns.map(compilePattern))
  }

  private def loadIgnoreFile(path: String): Seq[String] = {
    val nioPath = resolveIgnoreFilePath(path)
    Files.readAllLines(nioPath, StandardCharsets.UTF_8).asScala.toSeq.flatMap(normalizePattern)
  }

  private def resolveIgnoreFilePath(path: String): java.nio.file.Path = {
    val normalized = Option(path).map(_.trim).getOrElse("")
    Try(Paths.get(new java.net.URI(normalized))).getOrElse(Paths.get(normalized))
  }

  private def normalizePattern(rawPattern: String): Option[String] = {
    val trimmed = Option(rawPattern).map(_.trim).getOrElse("")
    if (trimmed.isEmpty || trimmed.startsWith("#")) None else Some(trimmed)
  }

  private def compilePattern(pattern: String): CompiledPattern = {
    val directoryPattern = pattern.endsWith("/")
    val corePattern = if (directoryPattern) pattern.dropRight(1) else pattern
    val hasSlash = corePattern.contains("/")

    if (directoryPattern && !hasSlash) {
      CompiledPattern(
        original = pattern,
        mode = PathMode,
        regex = Pattern.compile(s"(^|.*/)${globToRegex(corePattern)}/?$$"),
        requiresDirectory = true
      )
    } else if (directoryPattern) {
      CompiledPattern(
        original = pattern,
        mode = PathMode,
        regex = Pattern.compile(s"^${globToRegex(corePattern)}/?$$"),
        requiresDirectory = true
      )
    } else if (hasSlash) {
      CompiledPattern(
        original = pattern,
        mode = PathMode,
        regex = Pattern.compile(s"^${globToRegex(corePattern)}$$")
      )
    } else {
      CompiledPattern(
        original = pattern,
        mode = BasenameMode,
        regex = Pattern.compile(s"^${globToRegex(corePattern)}$$")
      )
    }
  }

  private def globToRegex(glob: String): String = {
    val builder = new StringBuilder
    var index = 0

    while (index < glob.length) {
      val current = glob.charAt(index)
      current match {
        case '*' if index + 1 < glob.length && glob.charAt(index + 1) == '*' =>
          builder.append(".*")
          index += 1
        case '*' =>
          builder.append("[^/]*")
        case '?' =>
          builder.append("[^/]")
        case value if "\\.^$+{}[]()|".contains(value) =>
          builder.append('\\').append(value)
        case value =>
          builder.append(value)
      }
      index += 1
    }

    builder.toString()
  }

  private def buildPathCandidates(relativePath: String, isDirectory: Boolean): Seq[String] = {
    val candidates = ArrayBuffer.empty[String]

    def addCandidate(value: String): Unit = {
      val normalized = normalizeForMatching(value)
      if (normalized.nonEmpty && !candidates.contains(normalized)) {
        candidates += normalized
      }
      if (isDirectory && normalized.nonEmpty) {
        val directoryVariant = normalized.stripSuffix("/") + "/"
        if (!candidates.contains(directoryVariant)) {
          candidates += directoryVariant
        }
      }
    }

    addCandidate(relativePath)
    val archiveSeparatorIndex = relativePath.lastIndexOf('!')
    if (archiveSeparatorIndex >= 0 && archiveSeparatorIndex + 1 < relativePath.length) {
      addCandidate(relativePath.substring(archiveSeparatorIndex + 1))
    }

    candidates.toSeq
  }

  private def pathBasename(path: String): String = {
    val normalized = normalizeForMatching(path)
    val archiveRelativePath = {
      val archiveSeparatorIndex = normalized.lastIndexOf('!')
      if (archiveSeparatorIndex >= 0 && archiveSeparatorIndex + 1 < normalized.length) {
        normalized.substring(archiveSeparatorIndex + 1)
      } else {
        normalized
      }
    }
    val trimmed = archiveRelativePath.stripSuffix("/")
    val lastSeparatorIndex = trimmed.lastIndexOf('/')
    if (lastSeparatorIndex >= 0) trimmed.substring(lastSeparatorIndex + 1) else trimmed
  }

  private def normalizeForMatching(path: String): String = {
    val normalized = Option(path).getOrElse("").replace('\\', '/')
    if (normalized == "/") normalized else normalized.replaceAll("/+$", "").stripPrefix("./")
  }

  private def stripTrailingSlash(path: String): String = {
    val normalized = Option(path).getOrElse("").replace('\\', '/')
    if (normalized == "/") normalized else normalized.replaceAll("/+$", "")
  }

  private def fallbackIdentifier(path: String): String = {
    val normalized = stripTrailingSlash(path)
    Option(new Path(normalized).getName).filter(_.nonEmpty).getOrElse(normalized)
  }

  private def resolveRelativePath(rootPath: String, targetPath: String): String = {
    val normalizedRootPath = Option(rootPath).getOrElse("").replace('\\', '/')
    val normalizedTargetPath = Option(targetPath).getOrElse("").replace('\\', '/')
    val rootUri = new Path(normalizedRootPath).toUri.normalize()
    val targetUri = new Path(normalizedTargetPath).toUri.normalize()

    val schemesCompatible =
      rootUri.getScheme == null || targetUri.getScheme == null || rootUri.getScheme == targetUri.getScheme
    val authoritiesCompatible =
      rootUri.getAuthority == null || targetUri.getAuthority == null || rootUri.getAuthority == targetUri.getAuthority

    val comparableRootPath = stripTrailingSlash(Option(rootUri.getPath).filter(_.nonEmpty).getOrElse(normalizedRootPath))
    val comparableTargetPath = stripTrailingSlash(Option(targetUri.getPath).filter(_.nonEmpty).getOrElse(normalizedTargetPath))

    if (!schemesCompatible || !authoritiesCompatible) {
      normalizedTargetPath
    } else if (comparableRootPath == comparableTargetPath) {
      fallbackIdentifier(normalizedTargetPath)
    } else {
      val prefix = if (comparableRootPath == "/") "/" else s"$comparableRootPath/"
      if (comparableTargetPath.startsWith(prefix)) {
        comparableTargetPath.substring(prefix.length)
      } else {
        normalizedTargetPath
      }
    }
  }
}
