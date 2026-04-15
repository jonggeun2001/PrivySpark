package io.github.jonggeun2001.privyspark.config

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkEnv
import org.apache.spark.SparkFiles

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.regex.Pattern
import scala.collection.mutable.{ArrayBuffer, LinkedHashSet}

final class IgnoreMatcher private (patterns: Seq[IgnoreMatcher.CompiledPattern]) {
  import IgnoreMatcher._

  def matched(targetPath: String, rootPath: String, isDirectory: Boolean = false): Option[String] = {
    if (patterns.isEmpty) {
      None
    } else {
      val relativePath = normalizeForMatching(resolveRelativePath(rootPath, targetPath))
      val pathCandidates = buildPathCandidates(relativePath, isDirectory)
      val basenameCandidates = pathCandidates.map(pathBasename).filter(_.nonEmpty).distinct
      val directoryCandidates = buildDirectoryCandidates(pathCandidates, isDirectory)

      patterns.collectFirst {
        case pattern if pattern.matches(pathCandidates, basenameCandidates, directoryCandidates) => pattern.original
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
    regex: Pattern
  ) {
    def matches(pathCandidates: Seq[String], basenameCandidates: Seq[String], directoryCandidates: Seq[String]): Boolean = {
      val candidates = mode match {
        case BasenameMode => basenameCandidates
        case PathMode => pathCandidates
        case DirectoryMode => directoryCandidates
      }
      candidates.exists(candidate => regex.matcher(candidate).matches())
    }
  }

  val empty: IgnoreMatcher = new IgnoreMatcher(Seq.empty)

  private case object DirectoryMode extends MatchMode

  def fromSources(inline: Seq[String], file: Option[String]): IgnoreMatcher = {
    fromSources(new Configuration(), inline, file)
  }

  def fromSources(conf: Configuration, inline: Seq[String], file: Option[String]): IgnoreMatcher = {
    val mergedPatterns = inline.flatMap(normalizePattern) ++ file.toSeq.flatMap(loadIgnoreFile(conf, _))
    new IgnoreMatcher(mergedPatterns.map(compilePattern))
  }

  private def loadIgnoreFile(conf: Configuration, path: String): Seq[String] = {
    val normalizedPath = Option(path).map(_.trim).getOrElse("")
    resolveLocalIgnoreFile(normalizedPath) match {
      case Some(localPath) =>
        val reader = Files.newBufferedReader(localPath, StandardCharsets.UTF_8)
        readPatterns(reader)
      case None =>
        val hadoopPath = new Path(normalizedPath)
        val fs = hadoopPath.getFileSystem(conf)
        val reader = new BufferedReader(new InputStreamReader(fs.open(hadoopPath), StandardCharsets.UTF_8))
        readPatterns(reader)
    }
  }

  private def resolveLocalIgnoreFile(path: String): Option[java.nio.file.Path] = {
    val hadoopPath = new Path(path)
    val uri = hadoopPath.toUri

    if (uri.getScheme != null || uri.getAuthority != null) {
      None
    } else {
      val sparkFilesCandidate = Option(SparkEnv.get).map(_ => Paths.get(SparkFiles.get(path)))
      val workingDirectoryCandidate = Paths.get(path)

      Seq(sparkFilesCandidate, Some(workingDirectoryCandidate)).flatten.collectFirst {
        case candidate if Files.exists(candidate) => candidate.toAbsolutePath.normalize()
      }
    }
  }

  private def readPatterns(reader: BufferedReader): Seq[String] = {
    val lines = ArrayBuffer.empty[String]

    try {
      var line = reader.readLine()
      while (line != null) {
        lines += line
        line = reader.readLine()
      }
    } finally {
      reader.close()
    }

    lines.toSeq.flatMap(normalizePattern)
  }

  private def normalizePattern(rawPattern: String): Option[String] = {
    val trimmed = Option(rawPattern).map(_.trim).getOrElse("")
    if (trimmed.isEmpty || trimmed.startsWith("#")) None else Some(trimmed)
  }

  private def compilePattern(pattern: String): CompiledPattern = {
    val rootAnchored = pattern.startsWith("/")
    val anchoredPattern = if (rootAnchored) pattern.drop(1) else pattern
    val directoryPattern = anchoredPattern.endsWith("/")
    val corePattern = if (directoryPattern) anchoredPattern.dropRight(1) else anchoredPattern
    val hasSlash = corePattern.contains("/")

    if (rootAnchored && directoryPattern) {
      CompiledPattern(
        original = pattern,
        mode = DirectoryMode,
        regex = Pattern.compile(s"^${globToRegex(corePattern)}/$$")
      )
    } else if (rootAnchored) {
      CompiledPattern(
        original = pattern,
        mode = PathMode,
        regex = Pattern.compile(s"^${globToRegex(corePattern)}$$")
      )
    } else if (directoryPattern && !hasSlash) {
      CompiledPattern(
        original = pattern,
        mode = DirectoryMode,
        regex = Pattern.compile(s"(^|.*/)${globToRegex(corePattern)}/$$")
      )
    } else if (directoryPattern) {
      CompiledPattern(
        original = pattern,
        mode = DirectoryMode,
        regex = Pattern.compile(s"^${globToRegex(corePattern)}/$$")
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

  private[config] def buildPathCandidates(relativePath: String, isDirectory: Boolean): Seq[String] = {
    val candidates = LinkedHashSet.empty[String]

    def addCandidate(value: String): Unit = {
      val normalized = normalizeForMatching(value)
      if (normalized.nonEmpty) {
        candidates += normalized
      }
      if (isDirectory && normalized.nonEmpty) {
        val directoryVariant = normalized.stripSuffix("/") + "/"
        candidates += directoryVariant
      }
    }

    addCandidate(relativePath)
    val archiveSeparatorIndex = relativePath.lastIndexOf('!')
    if (archiveSeparatorIndex >= 0 && archiveSeparatorIndex + 1 < relativePath.length) {
      addCandidate(relativePath.substring(archiveSeparatorIndex + 1))
    }

    candidates.toSeq
  }

  private[config] def buildDirectoryCandidates(pathCandidates: Seq[String], isDirectory: Boolean): Seq[String] = {
    val directoryCandidates = LinkedHashSet.empty[String]

    def addDirectoryCandidate(value: String): Unit = {
      val normalized = normalizeForMatching(value).stripSuffix("/")
      if (normalized.nonEmpty) {
        directoryCandidates += normalized + "/"
      }
    }

    pathCandidates.foreach { candidate =>
      val normalized = normalizeForMatching(candidate).stripSuffix("/")
      if (normalized.nonEmpty) {
        val segments = normalized.split("/").filter(_.nonEmpty)
        val lastSegmentIsDirectory = isDirectory || candidate.endsWith("/")
        val maxSegmentCount = if (lastSegmentIsDirectory) segments.length else math.max(segments.length - 1, 0)

        (1 to maxSegmentCount).foreach { segmentCount =>
          addDirectoryCandidate(segments.take(segmentCount).mkString("/"))
        }
      }
    }

    directoryCandidates.toSeq
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
