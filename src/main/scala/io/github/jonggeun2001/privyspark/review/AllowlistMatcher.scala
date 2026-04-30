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
import scala.util.Try

final class AllowlistMatcher private (
  private val recurringEntries: Seq[RecurringAllowlistEntry]
) {
  def isEmpty: Boolean = recurringEntries.isEmpty
  def size: Int = recurringEntries.size

  def hasExactCandidate(datasetPath: String, fileIdentifier: String, columnName: String, piiType: String): Boolean = false

  def hasDirectoryCandidate(datasetPath: String, directoryIdentifier: String, columnName: String, piiType: String): Boolean = false

  def hasPatternCandidate(datasetPath: String, fileIdentifier: String, columnName: String, piiType: String): Boolean =
    hasRecurringCandidate(datasetPath, "", fileIdentifier, columnName, piiType)

  def hasRecurringCandidate(
    datasetPath: String,
    hiveTableFqn: String,
    fileIdentifier: String,
    columnName: String,
    piiType: String
  ): Boolean =
    activeRecurringEntries.exists(recurringMatches(_, datasetPath, hiveTableFqn, fileIdentifier, columnName, piiType))

  def evaluate(
    datasetPath: String,
    columnName: String,
    piiType: String,
    fingerprints: Seq[ResolvedFileFingerprint]
  ): AllowlistEvaluation = {
    val fileIdentifier = fingerprints.headOption.map(_.fileIdentifier).getOrElse("")
    evaluate(datasetPath, "", fileIdentifier, columnName, piiType, fingerprints)
  }

  def evaluate(
    datasetPath: String,
    hiveTableFqn: String,
    fileIdentifier: String,
    columnName: String,
    piiType: String,
    fingerprints: Seq[ResolvedFileFingerprint]
  ): AllowlistEvaluation = {
    val identifiers = (Seq(fileIdentifier) ++ fingerprints.map(_.fileIdentifier))
      .map(value => Option(value).getOrElse(""))
      .filter(_.nonEmpty)
      .distinct
    val matched = activeRecurringEntries.exists { entry =>
      if (entry.hiveTableFqn.trim.nonEmpty) {
        recurringMatches(entry, datasetPath, hiveTableFqn, fileIdentifier, columnName, piiType)
      } else {
        identifiers.exists(identifier => recurringMatches(entry, datasetPath, hiveTableFqn, identifier, columnName, piiType))
      }
    }
    AllowlistEvaluation(shouldSuppress = matched)
  }

  private def activeRecurringEntries: Seq[RecurringAllowlistEntry] =
    recurringEntries.filterNot(entry => isExpired(entry.expiresAt))

  private def isExpired(expiresAt: String): Boolean =
    try {
      LocalDate.parse(expiresAt).isBefore(LocalDate.now())
    } catch {
      case _: RuntimeException => true
    }

  private def recurringMatches(
    entry: RecurringAllowlistEntry,
    datasetPath: String,
    hiveTableFqn: String,
    fileIdentifier: String,
    columnName: String,
    piiType: String
  ): Boolean = {
    val sameScanPath =
      ReviewPathNormalizer.normalizeScanPath(entry.scanPath) == ReviewPathNormalizer.normalizeScanPath(datasetPath)
    val sameColumnAndType =
      fieldMatches(entry.columnName, columnName) &&
        fieldMatches(entry.piiType, piiType)
    val matchesScope =
      if (entry.hiveTableFqn.trim.nonEmpty) {
        entry.hiveTableFqn == Option(hiveTableFqn).getOrElse("").trim
      } else {
        wildcardMatches(entry.fileIdentifierPattern, fileIdentifier)
      }
    sameScanPath && sameColumnAndType && matchesScope
  }

  private def fieldMatches(pattern: String, value: String): Boolean = {
    val normalizedPattern = Option(pattern).map(_.trim).getOrElse("")
    if (normalizedPattern.contains("*")) wildcardMatches(normalizedPattern, value)
    else normalizedPattern == Option(value).getOrElse("")
  }

  private def wildcardMatches(pattern: String, value: String): Boolean = {
    val normalizedPattern = Option(pattern).map(_.trim).filter(_.nonEmpty).getOrElse("")
    val normalizedValue = Option(value).getOrElse("")
    if (normalizedPattern.isEmpty) {
      false
    } else {
      val regex = normalizedPattern.flatMap {
        case '*' => ".*"
        case ch if "\\.[]{}()+-^$?|".contains(ch) => "\\" + ch
        case ch => ch.toString
      }
      normalizedValue.matches(regex)
    }
  }
}

object AllowlistMatcher {
  val empty: AllowlistMatcher = fromRecurringEntries(Seq.empty)

  def fromEntries(entries: Seq[AllowlistEntry]): AllowlistMatcher = empty

  def fromEntries(entries: Seq[AllowlistEntry], patterns: Seq[PatternAllowlistEntry]): AllowlistMatcher =
    fromRecurringEntries(patterns.map(patternToRecurringEntry))

  def fromRecurringEntries(entries: Seq[RecurringAllowlistEntry]): AllowlistMatcher = {
    val normalized = entries.groupBy(_.key).map {
      case (_, groupedEntries) => groupedEntries.last
    }.toSeq
    new AllowlistMatcher(normalized)
  }

  def combine(matchers: Seq[AllowlistMatcher]): AllowlistMatcher =
    fromRecurringEntries(matchers.flatMap(_.recurringEntries))

  def load(conf: Configuration, path: String): AllowlistMatcher = {
    val normalizedPath = Option(path).map(_.trim).getOrElse("")
    if (normalizedPath.isEmpty) {
      empty
    } else {
      val resolvedPath = resolveReadableAllowlistPath(conf, normalizedPath).getOrElse(normalizedPath)
      fromRecurringEntries(loadRecurringEntries(conf, resolvedPath))
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
    fromRecurringEntries(resolvedPaths.flatMap(loadRecurringEntries(conf, _)))
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

  def loadRecurringEntries(conf: Configuration, path: String): Seq[RecurringAllowlistEntry] = {
    val normalizedPath = Option(path).map(_.trim).getOrElse("")
    if (normalizedPath.isEmpty) {
      Seq.empty
    } else {
      readLines(conf, resolveReadableAllowlistPath(conf, normalizedPath).getOrElse(normalizedPath)).flatMap(parseRecurringEntry)
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
    if (entryType.exists(value => value == "pattern" || value == "recurring")) {
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

  private def parseRecurringEntry(line: String): Option[RecurringAllowlistEntry] = {
    val entryType = extractJsonStringField(line, "entry_type").map(_.trim.toLowerCase(Locale.ROOT))
    entryType match {
      case Some("recurring") =>
        for {
          scanPath <- extractJsonStringField(line, "scan_path").orElse(extractJsonStringField(line, "dataset_path"))
          columnName <- extractJsonStringField(line, "column_name")
          piiType <- extractJsonStringField(line, "pii_type")
          reason <- extractJsonStringField(line, "reason")
          reviewer <- extractJsonStringField(line, "reviewer")
          reviewedAt <- extractJsonStringField(line, "reviewed_at")
          expiresAt <- extractJsonStringField(line, "expires_at")
        } yield RecurringAllowlistEntry(
          scanPath = scanPath,
          hiveTableFqn = extractJsonStringField(line, "hive_table_fqn").getOrElse(""),
          fileIdentifierPattern = extractJsonStringField(line, "file_identifier_pattern")
            .orElse(extractJsonStringField(line, "file_identifier"))
            .getOrElse(""),
          columnName = columnName,
          piiType = piiType,
          reason = reason,
          reviewer = reviewer,
          reviewedAt = reviewedAt,
          expiresAt = expiresAt,
          sourceFindingKey = extractJsonStringField(line, "source_finding_key").getOrElse(""),
          sampleRowCount = extractJsonLongField(line, "sample_row_count").getOrElse(0L),
          matchCount = extractJsonLongField(line, "match_count").getOrElse(0L),
          nonEmptyMatchRatio = extractJsonDoubleField(line, "non_empty_match_ratio").getOrElse(0.0)
        )
      case Some("pattern") =>
        parsePatternEntry(line).map(patternToRecurringEntry)
      case _ =>
        None
    }
  }

  private def patternToRecurringEntry(entry: PatternAllowlistEntry): RecurringAllowlistEntry =
    RecurringAllowlistEntry(
      scanPath = entry.datasetPath,
      hiveTableFqn = "",
      fileIdentifierPattern = entry.fileIdentifierPattern,
      columnName = entry.columnNamePattern,
      piiType = entry.piiTypePattern,
      reason = entry.reason,
      reviewer = entry.reviewer,
      reviewedAt = entry.reviewedAt,
      expiresAt = entry.expiresAt,
      sourceFindingKey = entry.sourceFindingKey,
      sampleRowCount = 0L,
      matchCount = 0L,
      nonEmptyMatchRatio = 0.0
    )

  private def extractJsonDoubleField(json: String, field: String): Option[Double] = {
    val pattern = ("\"" + field + "\"\\s*:\\s*(-?[0-9]+(?:\\.[0-9]+)?)").r
    pattern.findFirstMatchIn(json).flatMap(matchResult => Try(matchResult.group(1).toDouble).toOption)
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
