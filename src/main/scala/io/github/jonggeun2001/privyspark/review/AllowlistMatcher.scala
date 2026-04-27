package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.report.JsonCodec.{extractJsonLongField, extractJsonStringField}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkEnv
import org.apache.spark.SparkFiles

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.LocalDate
import java.util.Locale
import scala.collection.mutable.ArrayBuffer

final class AllowlistMatcher private (
  private val entriesByKey: Map[AllowlistKey, AllowlistEntry],
  private val directoryCandidates: Set[(String, String, String, String)],
  private val patternEntries: Seq[PatternAllowlistEntry]
) {
  def isEmpty: Boolean = entriesByKey.isEmpty && patternEntries.isEmpty
  def size: Int = entriesByKey.size + patternEntries.size

  def hasExactCandidate(datasetPath: String, fileIdentifier: String, columnName: String, piiType: String): Boolean =
    entriesByKey.contains(AllowlistKey(datasetPath, fileIdentifier, columnName, piiType))

  def hasDirectoryCandidate(datasetPath: String, directoryIdentifier: String, columnName: String, piiType: String): Boolean =
    directoryCandidates.contains((datasetPath, directoryIdentifier, columnName, piiType))

  def hasPatternCandidate(datasetPath: String, fileIdentifier: String, columnName: String, piiType: String): Boolean =
    activePatternEntries.exists(patternMatches(_, datasetPath, fileIdentifier, columnName, piiType))

  def evaluate(
    datasetPath: String,
    columnName: String,
    piiType: String,
    fingerprints: Seq[ResolvedFileFingerprint]
  ): AllowlistEvaluation = {
    if (fingerprints.isEmpty) {
      return AllowlistEvaluation(shouldSuppress = false)
    }

    val activePatterns = activePatternEntries
    val allFingerprintsCoveredByPattern =
      activePatterns.nonEmpty &&
        fingerprints.forall(fingerprint => activePatterns.exists(patternMatches(_, datasetPath, fingerprint.fileIdentifier, columnName, piiType)))

    if (allFingerprintsCoveredByPattern) {
      return AllowlistEvaluation(shouldSuppress = true)
    }

    val exactMatches = fingerprints.flatMap { fingerprint =>
      entriesByKey.get(AllowlistKey(datasetPath, fingerprint.fileIdentifier, columnName, piiType)).map(_ -> fingerprint)
    }

    if (exactMatches.isEmpty) {
      AllowlistEvaluation(shouldSuppress = false)
    } else {
      val allFingerprintsCovered = exactMatches.size == fingerprints.size
      val mismatchedEntries = exactMatches.collect {
        case (entry, fingerprint) if !metadataMatches(entry, fingerprint) => entry
      }

      if (allFingerprintsCovered && mismatchedEntries.isEmpty) {
        AllowlistEvaluation(shouldSuppress = true)
      } else if (mismatchedEntries.nonEmpty) {
        AllowlistEvaluation(
          shouldSuppress = false,
          reviewStatus = ReviewStatus.Pending,
          reviewReason = mismatchedEntries.head.reason,
          reviewInvalidated = true
        )
      } else {
        AllowlistEvaluation(shouldSuppress = false)
      }
    }
  }

  private def metadataMatches(entry: AllowlistEntry, fingerprint: ResolvedFileFingerprint): Boolean =
    entry.fileSize == fingerprint.fileSize &&
      entry.fileMtimeEpochMs == fingerprint.fileMtimeEpochMs &&
      entry.fileChecksumAlgo.equalsIgnoreCase(fingerprint.fileChecksumAlgo) &&
      entry.fileChecksum.equalsIgnoreCase(fingerprint.fileChecksum)

  private def activePatternEntries: Seq[PatternAllowlistEntry] =
    patternEntries.filterNot(entry => isExpired(entry.expiresAt))

  private def isExpired(expiresAt: String): Boolean =
    try {
      LocalDate.parse(expiresAt).isBefore(LocalDate.now())
    } catch {
      case _: RuntimeException => true
    }

  private def patternMatches(
    entry: PatternAllowlistEntry,
    datasetPath: String,
    fileIdentifier: String,
    columnName: String,
    piiType: String
  ): Boolean =
    entry.datasetPath == datasetPath &&
      wildcardMatches(entry.fileIdentifierPattern, fileIdentifier) &&
      wildcardMatches(entry.columnNamePattern, columnName) &&
      wildcardMatches(entry.piiTypePattern, piiType)

  private def wildcardMatches(pattern: String, value: String): Boolean = {
    val normalizedPattern = Option(pattern).getOrElse("")
    val normalizedValue = Option(value).getOrElse("")
    val regex = normalizedPattern.flatMap {
      case '*' => ".*"
      case ch if "\\.[]{}()+-^$?|".contains(ch) => "\\" + ch
      case ch => ch.toString
    }
    normalizedValue.matches(regex)
  }
}

object AllowlistMatcher {
  val empty: AllowlistMatcher = fromEntries(Seq.empty)

  def fromEntries(entries: Seq[AllowlistEntry]): AllowlistMatcher = fromEntries(entries, Seq.empty)

  def fromEntries(entries: Seq[AllowlistEntry], patterns: Seq[PatternAllowlistEntry]): AllowlistMatcher = {
    val normalizedEntries = entries.groupBy(_.key).map {
      case (key, groupedEntries) => key -> groupedEntries.last
    }
    val normalizedPatterns = patterns.groupBy(_.key).map {
      case (_, groupedEntries) => groupedEntries.last
    }.toSeq
    val derivedDirectoryCandidates = normalizedEntries.values.flatMap { entry =>
      directoryCandidate(entry).map { directoryIdentifier =>
        (entry.datasetPath, directoryIdentifier, entry.columnName, entry.piiType)
      }
    }.toSet
    new AllowlistMatcher(normalizedEntries, derivedDirectoryCandidates, normalizedPatterns)
  }

  def combine(matchers: Seq[AllowlistMatcher]): AllowlistMatcher =
    fromEntries(
      matchers.flatMap(_.entriesByKey.values),
      matchers.flatMap(_.patternEntries)
    )

  def load(conf: Configuration, path: String): AllowlistMatcher = {
    val normalizedPath = Option(path).map(_.trim).getOrElse("")
    if (normalizedPath.isEmpty) {
      empty
    } else {
      val resolvedPath = resolveReadableAllowlistPath(conf, normalizedPath).getOrElse(normalizedPath)
      fromEntries(loadEntries(conf, resolvedPath), loadPatternEntries(conf, resolvedPath))
    }
  }

  def loadExisting(conf: Configuration, path: String): AllowlistMatcher = {
    val normalizedPath = Option(path).map(_.trim).getOrElse("")
    if (normalizedPath.isEmpty || resolveReadableAllowlistPath(conf, normalizedPath).isEmpty) {
      empty
    } else {
      load(conf, normalizedPath)
    }
  }

  def loadExistingMany(conf: Configuration, paths: Seq[String]): AllowlistMatcher = {
    val resolvedPaths = paths.flatMap(path => resolveReadableAllowlistPath(conf, path.trim))
    fromEntries(
      resolvedPaths.flatMap(loadEntries(conf, _)),
      resolvedPaths.flatMap(loadPatternEntries(conf, _))
    )
  }

  def loadEntries(conf: Configuration, path: String): Seq[AllowlistEntry] = {
    val normalizedPath = Option(path).map(_.trim).getOrElse("")
    if (normalizedPath.isEmpty) {
      Seq.empty
    } else {
      readLines(conf, resolveReadableAllowlistPath(conf, normalizedPath).getOrElse(normalizedPath)).flatMap(parseEntry)
    }
  }

  def loadPatternEntries(conf: Configuration, path: String): Seq[PatternAllowlistEntry] = {
    val normalizedPath = Option(path).map(_.trim).getOrElse("")
    if (normalizedPath.isEmpty) {
      Seq.empty
    } else {
      readLines(conf, resolveReadableAllowlistPath(conf, normalizedPath).getOrElse(normalizedPath)).flatMap(parsePatternEntry)
    }
  }

  private def resolveReadableAllowlistPath(conf: Configuration, path: String): Option[String] = {
    Seq(path, s"$path.bak").collectFirst {
      case candidate if allowlistPathExists(conf, candidate) => candidate
    }
  }

  private def allowlistPathExists(conf: Configuration, path: String): Boolean = {
    resolveLocalAllowlistFile(path).exists(Files.exists(_)) || {
      val hadoopPath = new Path(path)
      hadoopPath.getFileSystem(conf).exists(hadoopPath)
    }
  }

  private def parseEntry(line: String): Option[AllowlistEntry] = {
    val entryType = extractJsonStringField(line, "entry_type").map(_.trim.toLowerCase(Locale.ROOT))
    if (entryType.contains("pattern")) {
      return None
    }
    for {
      datasetPath <- extractJsonStringField(line, "dataset_path")
      fileIdentifier <- extractJsonStringField(line, "file_identifier")
      columnName <- extractJsonStringField(line, "column_name")
      piiType <- extractJsonStringField(line, "pii_type")
      reviewer <- extractJsonStringField(line, "reviewer")
      reviewedAt <- extractJsonStringField(line, "reviewed_at")
      fileSize <- extractJsonLongField(line, "file_size")
      fileMtimeEpochMs <- extractJsonLongField(line, "file_mtime_epoch_ms")
      fileChecksum <- extractJsonStringField(line, "file_checksum")
    } yield {
      AllowlistEntry(
        datasetPath = datasetPath,
        fileIdentifier = fileIdentifier,
        columnName = columnName,
        piiType = piiType,
        reason = extractJsonStringField(line, "reason").getOrElse(""),
        reviewer = reviewer,
        reviewedAt = reviewedAt,
        sourceRunId = extractJsonStringField(line, "source_run_id").getOrElse(""),
        fileSize = fileSize,
        fileMtimeEpochMs = fileMtimeEpochMs,
        fileChecksumAlgo = extractJsonStringField(line, "file_checksum_algo").getOrElse(FileIdentifierResolver.DefaultChecksumAlgo),
        fileChecksum = fileChecksum
      )
    }
  }

  private def parsePatternEntry(line: String): Option[PatternAllowlistEntry] = {
    val entryType = extractJsonStringField(line, "entry_type").map(_.trim.toLowerCase(Locale.ROOT))
    if (!entryType.contains("pattern")) {
      None
    } else {
      for {
        datasetPath <- extractJsonStringField(line, "dataset_path")
        fileIdentifierPattern <- extractJsonStringField(line, "file_identifier_pattern")
          .orElse(extractJsonStringField(line, "file_identifier"))
        columnNamePattern <- extractJsonStringField(line, "column_name_pattern")
          .orElse(extractJsonStringField(line, "column_name"))
        piiTypePattern <- extractJsonStringField(line, "pii_type_pattern")
          .orElse(extractJsonStringField(line, "pii_type"))
        reason <- extractJsonStringField(line, "reason")
        reviewer <- extractJsonStringField(line, "reviewer")
        reviewedAt <- extractJsonStringField(line, "reviewed_at")
        expiresAt <- extractJsonStringField(line, "expires_at")
      } yield PatternAllowlistEntry(
        datasetPath = datasetPath,
        fileIdentifierPattern = fileIdentifierPattern,
        columnNamePattern = columnNamePattern,
        piiTypePattern = piiTypePattern,
        reason = reason,
        reviewer = reviewer,
        reviewedAt = reviewedAt,
        expiresAt = expiresAt,
        sourceFindingKey = extractJsonStringField(line, "source_finding_key").getOrElse("")
      )
    }
  }

  private def directoryCandidate(entry: AllowlistEntry): Option[String] = {
    val identifier = Option(entry.fileIdentifier).getOrElse("")
    if (identifier.contains("!") || identifier.contains("#")) {
      None
    } else {
      val path = new Path(identifier)
      Option(path.getParent).map(_.toString).orElse(Some("."))
    }
  }

  private def readLines(conf: Configuration, path: String): Seq[String] = {
    val reader = resolveLocalAllowlistFile(path) match {
      case Some(localPath) =>
        Files.newBufferedReader(localPath, StandardCharsets.UTF_8)
      case None =>
        val hadoopPath = new Path(path)
        val fs = hadoopPath.getFileSystem(conf)
        new BufferedReader(new InputStreamReader(fs.open(hadoopPath), StandardCharsets.UTF_8))
    }

    val lines = ArrayBuffer.empty[String]
    try {
      var line = reader.readLine()
      while (line != null) {
        val trimmed = line.trim
        if (trimmed.nonEmpty) {
          lines += trimmed
        }
        line = reader.readLine()
      }
    } finally {
      reader.close()
    }

    lines.toSeq
  }

  private def resolveLocalAllowlistFile(path: String): Option[java.nio.file.Path] = {
    val hadoopPath = new Path(path)
    val uri = hadoopPath.toUri
    val workingDirectoryCandidate = Paths.get(path)

    if (uri.getScheme != null || uri.getAuthority != null || workingDirectoryCandidate.isAbsolute) {
      None
    } else {
      val sparkFilesCandidate = Option(SparkEnv.get).map(_ => Paths.get(SparkFiles.get(path)))

      Seq(sparkFilesCandidate, Some(workingDirectoryCandidate)).flatten.collectFirst {
        case candidate if Files.exists(candidate) => candidate.toAbsolutePath.normalize()
      }
    }
  }
}
