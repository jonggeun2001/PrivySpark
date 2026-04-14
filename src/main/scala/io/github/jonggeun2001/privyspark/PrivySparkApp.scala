package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.DetectionAggregator.MatchCount
import io.github.jonggeun2001.privyspark.config.RulesetLoader
import io.github.jonggeun2001.privyspark.model.{PiiRule, PiiRuleMatchType, ScanError, ScanResult}
import org.apache.poi.ss.usermodel.WorkbookFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.execution.datasources.csv.CSVUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.{col, input_file_name}

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.charset.{CharacterCodingException, CodingErrorAction}
import java.time.Instant
import java.util.UUID
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.ZipInputStream
import java.nio.file.NoSuchFileException
import com.univocity.parsers.csv.CsvParser
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random
import scala.util.Try
import scala.util.control.ControlThrowable
import scala.util.control.NonFatal

object PrivySparkApp {
  private[privyspark] final case class ScanReadOptions(sheetName: Option[String] = None)
  private[privyspark] final case class ScanFileEntry(
    sourceKey: String,
    physicalPath: String,
    directoryPath: String,
    format: String,
    logicalIdentifier: String,
    readOptions: ScanReadOptions = ScanReadOptions(),
    allowDirectoryIdentifier: Boolean = true
  )
  private[privyspark] final case class ScanGroup(
    directoryPath: String,
    format: String,
    schemaSignature: String,
    filePaths: Seq[String],
    useDirectoryIdentifier: Boolean = false,
    directoryIdentifierEligible: Boolean = false,
    schemaSampled: Boolean = false,
    csvHasHeader: Boolean = true,
    physicalPathsByKey: Map[String, String] = Map.empty,
    logicalIdentifiersByKey: Map[String, String] = Map.empty,
    readOptionsByKey: Map[String, ScanReadOptions] = Map.empty,
    allowDirectoryIdentifier: Boolean = true
  )
  private[privyspark] final case class DirectoryScanPlan(
    groups: Seq[ScanGroup],
    errors: Seq[ScanError],
    totalFiles: Int,
    directoryCount: Int,
    stagingPaths: Seq[String] = Seq.empty
  )
  private final case class FileScanMetrics(
    fileIdentifier: String,
    sampledRowCount: Long,
    matchCounts: Seq[MatchCount]
  )
  private final case class ProbeSample(bytes: Array[Byte], truncated: Boolean)
  private final case class PreScanFileOutcome(
    filePath: String,
    groupingDirectoryPath: String,
    preScanErrorScope: String,
    expandedEntries: Seq[ScanFileEntry],
    expandedErrors: Seq[ScanError],
    stagingPaths: Seq[String],
    pathInferredFormat: Option[String] = None,
    probeRequired: Boolean = false,
    skipped: Boolean = false,
    failure: Option[Throwable] = None
  )
  private[privyspark] final case class ProgressRun(
    runId: String,
    rootPath: String,
    runPath: String,
    activeRunPath: String,
    datasetPath: String,
    outputRoot: String,
    scanTimestamp: String,
    resultsPath: String,
    errorsPath: String,
    metaPath: String,
    completionsPath: String
  )
  private final case class ActiveRunMarker(runId: String, state: String, lastHeartbeatEpochMillis: Long)
  private final case class ProgressRunMetadata(runId: String, state: String)

  private val FileIdentifierColumn = "__privyspark_file_identifier"
  private val ProgressDirectoryName = "_progress"
  private val TextFormat = "text"
  private val XlsxFormat = "xlsx"
  private val AvroFormat = "avro"
  private val ZipFormat = "zip"
  private val JarFormat = "jar"
  private val ArchiveFormats = Set(ZipFormat, JarFormat)
  private val NonDirectoryIdentifierFormats = Set(TextFormat, XlsxFormat)
  private val MagicProbeByteLimit = 4
  private val TextProbeByteLimit = 512
  private val ActiveRunHeartbeatIntervalMillis = 30000L
  private val ActiveRunStaleThresholdMillis = 3L * 60L * 1000L
  private val PreparingRunStaleThresholdMillis = 30000L
  private val ParquetMagicBytes = Array[Byte]('P'.toByte, 'A'.toByte, 'R'.toByte, '1'.toByte)
  private val OrcMagicBytes = Array[Byte]('O'.toByte, 'R'.toByte, 'C'.toByte)
  private val MaxArchiveExpansionDepth = 1
  private[privyspark] val MaxFileReadAttempts = 2
  private[privyspark] val FileReadRetryDelayMillis = 200L
  private[privyspark] val PreScanProgressLogInterval = 10000
  private val ActiveRunMarkerLock = new AnyRef
  private val CommonCsvHeaderTokens = Set(
    "id",
    "name",
    "first",
    "last",
    "full",
    "maker",
    "model",
    "email",
    "mail",
    "phone",
    "tel",
    "mobile",
    "city",
    "state",
    "country",
    "이름",
    "이메일",
    "도시",
    "국가",
    "주소",
    "전화번호",
    "address",
    "addr",
    "zip",
    "postal",
    "code",
    "user",
    "account",
    "customer",
    "created",
    "updated",
    "timestamp",
    "date",
    "time",
    "age",
    "gender",
    "status",
    "type",
    "amount",
    "price",
    "count",
    "number",
    "value",
    "description",
    "product",
    "item"
  )
  private val PreScanParallelismConfKey = "spark.privyspark.preScanParallelism"
  private val DefaultPreScanParallelism = 4
  // Allow higher-than-core I/O fan-out without letting a single scan create an unbounded number of driver threads.
  private val MaxSafePreScanParallelism = 64
  private val GroupParallelismConfKey = "spark.privyspark.groupParallelism"
  private val DefaultGroupParallelism = 4
  private val FileParallelismConfKey = "spark.privyspark.fileParallelism"
  private val DefaultFileParallelism = 3
  private val RetriableFileReadErrorSnippets = Seq(
    "path does not exist",
    "file does not exist",
    "no such file",
    "underlying files have been updated",
    "failed_read_file",
    "encountered error while reading file"
  )

  private def logInfo(event: String, fields: (String, Any)*): Unit = {
    DriverLogger.info(event, fields: _*)
  }

  private def logWarn(event: String, fields: (String, Any)*): Unit = {
    DriverLogger.warn(event, fields: _*)
  }

  private[privyspark] def resetDebugCache(): Unit = {
    DriverLogger.resetCache()
  }

  private def logDebug(event: String, fields: (String, Any)*): Unit = {
    DriverLogger.debug(event, fields: _*)
  }

  private def elapsedMillis(startNanos: Long): Long = {
    (System.nanoTime() - startNanos) / 1000000L
  }

  private[privyspark] def resolvePreScanProgressInterval(fileCount: Int): Int = {
    if (fileCount <= 0) 1 else math.min(fileCount, PreScanProgressLogInterval)
  }

  private def stripTrailingSlash(path: String): String = {
    val normalized = Option(path).getOrElse("").replace('\\', '/')
    if (normalized == "/") normalized else normalized.replaceAll("/+$", "")
  }

  private def fallbackIdentifier(path: String): String = {
    val normalized = stripTrailingSlash(path)
    Option(new Path(normalized).getName).filter(_.nonEmpty).getOrElse(normalized)
  }

  private[privyspark] def resolveRelativeIdentifier(datasetPath: String, targetPath: String): String = {
    resolveRelativeIdentifier(datasetPath, targetPath, useCurrentDirectoryMarker = false)
  }

  private def resolveDirectoryIdentifier(datasetPath: String, directoryPath: String): String = {
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

  private def canonicalizePath(path: String): String = {
    val uri = new Path(path).toUri.normalize()
    val normalizedPath = stripTrailingSlash(Option(uri.getPath).filter(_.nonEmpty).getOrElse(path))
    normalizedPath
  }

  private def comparablePathVariants(path: String): Set[String] = {
    val canonical = canonicalizePath(path)
    Set(
      canonical,
      canonical.replace("%2523", "%23"),
      canonical.replace("%23", "#"),
      canonical.replace("%2523", "#"),
      canonical.replace("#", "%23")
    )
  }

  private def resolvePhysicalPath(group: ScanGroup, sourceKey: String): String = {
    group.physicalPathsByKey.getOrElse(sourceKey, sourceKey)
  }

  private def resolveReadOptions(group: ScanGroup, sourceKey: String): ScanReadOptions = {
    group.readOptionsByKey.getOrElse(sourceKey, ScanReadOptions())
  }

  private def resolveLogicalIdentifier(group: ScanGroup, datasetPath: String, sourceKey: String): String = {
    group.logicalIdentifiersByKey.getOrElse(
      sourceKey,
      resolveRelativeIdentifier(datasetPath, resolvePhysicalPath(group, sourceKey))
    )
  }

  private def resolveLogicalIdentifierForPhysicalPath(
    group: ScanGroup,
    datasetPath: String,
    physicalPath: String
  ): String = {
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
        resolveLogicalIdentifier(group, datasetPath, sourceKey)
      case Seq() =>
        resolveRelativeIdentifier(datasetPath, physicalPath)
      case multiple =>
        throw new IllegalStateException(
          s"Ambiguous logical identifier mapping for physical path: $physicalPath (${multiple.mkString(",")})"
        )
    }
  }

  private def supportsBatchScan(group: ScanGroup): Boolean = {
    group.format != XlsxFormat
  }

  private def cleanupStagingPaths(conf: org.apache.hadoop.conf.Configuration, stagingPaths: Seq[String]): Unit = {
    stagingPaths.foreach(path => deleteStagingPath(conf, path))
  }

  private def deleteStagingPath(conf: org.apache.hadoop.conf.Configuration, path: String): Unit = {
    try {
      val stagingPath = new Path(path)
      val fs = stagingPath.getFileSystem(conf)
      if (fs.exists(stagingPath) && !fs.delete(stagingPath, true)) {
        logWarn("staging_cleanup_failed", "path" -> path, "reason" -> "delete returned false")
      }
    } catch {
      case NonFatal(e) =>
        logWarn(
          "staging_cleanup_failed",
          "path" -> path,
          "reason" -> Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        )
    }
  }

  private def normalizeArchiveEntryName(entryName: String): String = {
    Option(entryName).getOrElse("").replace('\\', '/')
  }

  private def safeResolveArchiveEntryPath(root: Path, entryName: String): Option[Path] = {
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

  private def ensureArchiveEntryParent(
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

  private def readProbeBytes(conf: org.apache.hadoop.conf.Configuration, filePath: String, limit: Int): ProbeSample = {
    val sourcePath = new Path(filePath)
    val fs = sourcePath.getFileSystem(conf)
    val inputStream = fs.open(sourcePath)
    try {
      val buffer = new Array[Byte](limit)
      var totalBytesRead = 0
      var continueReading = true

      while (continueReading && totalBytesRead < limit) {
        val bytesRead = inputStream.read(buffer, totalBytesRead, limit - totalBytesRead)
        if (bytesRead < 0) {
          continueReading = false
        } else if (bytesRead == 0) {
          val singleByte = inputStream.read()
          if (singleByte < 0) {
            continueReading = false
          } else {
            buffer(totalBytesRead) = singleByte.toByte
            totalBytesRead += 1
          }
        } else {
          totalBytesRead += bytesRead
        }
      }

      val truncated =
        totalBytesRead >= limit && inputStream.read() >= 0
      val bytes =
        if (totalBytesRead <= 0) Array.emptyByteArray else java.util.Arrays.copyOf(buffer, totalBytesRead)
      ProbeSample(bytes, truncated)
    } finally {
      inputStream.close()
    }
  }

  private def inferMagicByteFormat(bytes: Array[Byte]): Option[String] = {
    if (bytes.length >= ParquetMagicBytes.length && ParquetMagicBytes.indices.forall(index => bytes(index) == ParquetMagicBytes(index))) {
      Some("parquet")
    } else if (bytes.length >= OrcMagicBytes.length && OrcMagicBytes.indices.forall(index => bytes(index) == OrcMagicBytes(index))) {
      Some("orc")
    } else {
      None
    }
  }

  private def inferTextFormat(
    bytes: Array[Byte],
    allowIncompleteTrailingSequence: Boolean = false
  ): Option[String] = {
    if (looksLikeText(bytes, allowIncompleteTrailingSequence)) Some(TextFormat) else None
  }

  private def looksLikeText(bytes: Array[Byte], allowIncompleteTrailingSequence: Boolean): Boolean = {
    if (bytes.isEmpty) {
      true
    } else if (bytes.contains(0.toByte)) {
      false
    } else if (!isValidUtf8(bytes, allowIncompleteTrailingSequence)) {
      false
    } else {
      val suspiciousControlBytes = bytes.count { rawByte =>
        val byte = rawByte & 0xff
        byte < 0x20 && byte != 0x09 && byte != 0x0A && byte != 0x0D
      }
      suspiciousControlBytes * 10 <= bytes.length
    }
  }

  private def isValidUtf8(bytes: Array[Byte], allowIncompleteTrailingSequence: Boolean): Boolean = {
    val trailingTrimBytes =
      if (allowIncompleteTrailingSequence) incompleteTrailingUtf8Bytes(bytes) else 0
    val candidateBytes =
      if (trailingTrimBytes <= 0) bytes
      else java.util.Arrays.copyOf(bytes, bytes.length - trailingTrimBytes)
    val decoder = StandardCharsets.UTF_8
      .newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)

    try {
      decoder.decode(ByteBuffer.wrap(candidateBytes))
      true
    } catch {
      case _: CharacterCodingException => false
    }
  }

  private def incompleteTrailingUtf8Bytes(bytes: Array[Byte]): Int = {
    if (bytes.isEmpty) {
      0
    } else {
      var index = bytes.length - 1
      var continuationBytes = 0

      while (index >= 0 && isUtf8ContinuationByte(bytes(index))) {
        continuationBytes += 1
        index -= 1
      }

      if (index < 0) {
        0
      } else {
        expectedUtf8SequenceLength(bytes(index) & 0xff) match {
          case Some(expectedLength) =>
            val observedLength = continuationBytes + 1
            if (observedLength < expectedLength && isValidIncompleteUtf8Prefix(bytes, index, observedLength, expectedLength)) {
              observedLength
            } else {
              0
            }
          case None =>
            0
        }
      }
    }
  }

  private def isUtf8ContinuationByte(rawByte: Byte): Boolean = {
    ((rawByte & 0xff) & 0xC0) == 0x80
  }

  private def expectedUtf8SequenceLength(leadByte: Int): Option[Int] = {
    if (leadByte <= 0x7F) {
      Some(1)
    } else if (leadByte >= 0xC2 && leadByte <= 0xDF) {
      Some(2)
    } else if (leadByte >= 0xE0 && leadByte <= 0xEF) {
      Some(3)
    } else if (leadByte >= 0xF0 && leadByte <= 0xF4) {
      Some(4)
    } else {
      None
    }
  }

  private def isValidIncompleteUtf8Prefix(
    bytes: Array[Byte],
    leadIndex: Int,
    observedLength: Int,
    expectedLength: Int
  ): Boolean = {
    val leadByte = bytes(leadIndex) & 0xff

    if (observedLength <= 0 || observedLength >= expectedLength) {
      false
    } else if (observedLength == 1) {
      true
    } else if (!isValidUtf8FirstContinuation(leadByte, bytes(leadIndex + 1) & 0xff)) {
      false
    } else {
      var offset = 2
      var valid = true

      while (offset < observedLength && valid) {
        valid = isUtf8ContinuationByte(bytes(leadIndex + offset))
        offset += 1
      }

      valid
    }
  }

  private def isValidUtf8FirstContinuation(leadByte: Int, continuationByte: Int): Boolean = {
    if (leadByte >= 0xC2 && leadByte <= 0xDF) {
      continuationByte >= 0x80 && continuationByte <= 0xBF
    } else if (leadByte == 0xE0) {
      continuationByte >= 0xA0 && continuationByte <= 0xBF
    } else if ((leadByte >= 0xE1 && leadByte <= 0xEC) || (leadByte >= 0xEE && leadByte <= 0xEF)) {
      continuationByte >= 0x80 && continuationByte <= 0xBF
    } else if (leadByte == 0xED) {
      continuationByte >= 0x80 && continuationByte <= 0x9F
    } else if (leadByte == 0xF0) {
      continuationByte >= 0x90 && continuationByte <= 0xBF
    } else if (leadByte >= 0xF1 && leadByte <= 0xF3) {
      continuationByte >= 0x80 && continuationByte <= 0xBF
    } else if (leadByte == 0xF4) {
      continuationByte >= 0x80 && continuationByte <= 0x8F
    } else {
      false
    }
  }

  private def detectPhysicalFormat(
    conf: org.apache.hadoop.conf.Configuration,
    filePath: String
  ): Option[String] = {
    val extensionFormat = FormatDetector.infer(filePath)
    if (extensionFormat.isDefined) {
      extensionFormat
    } else {
      val probeSample = readProbeBytes(conf, filePath, TextProbeByteLimit)
      inferMagicByteFormat(probeSample.bytes)
        .orElse(inferTextFormat(probeSample.bytes, allowIncompleteTrailingSequence = probeSample.truncated))
    }
  }

  private def isZeroBytePhysicalFile(conf: org.apache.hadoop.conf.Configuration, filePath: String): Boolean = {
    val sourcePath = new Path(filePath)
    val fs = sourcePath.getFileSystem(conf)
    fs.getFileStatus(sourcePath).getLen == 0L
  }

  private def effectiveRulesForFormat(format: String, rules: Seq[PiiRule]): Seq[PiiRule] = {
    rules
  }

  private def workbookDataAddress(sheetName: String): String = {
    s"'${sheetName.replace("'", "''")}'!A1"
  }

  private def sheetHasContent(sheet: org.apache.poi.ss.usermodel.Sheet): Boolean = {
    val rowIterator = sheet.rowIterator()
    while (rowIterator.hasNext) {
      val row = rowIterator.next()
      val cellIterator = row.cellIterator()
      while (cellIterator.hasNext) {
        val cell = cellIterator.next()
        if (cell != null && cell.toString.trim.nonEmpty) {
          return true
        }
      }
    }
    false
  }

  private def listVisibleWorkbookSheets(
    conf: org.apache.hadoop.conf.Configuration,
    filePath: String
  ): Either[String, Seq[String]] = {
    val sourcePath = new Path(filePath)
    val fs = sourcePath.getFileSystem(conf)
    val inputStream = fs.open(sourcePath)
    try {
      val workbook = WorkbookFactory.create(inputStream)
      try {
        val sheetNames = (0 until workbook.getNumberOfSheets).flatMap { index =>
          val hidden = workbook.isSheetHidden(index) || workbook.isSheetVeryHidden(index)
          val sheet = workbook.getSheetAt(index)
          if (!hidden && sheetHasContent(sheet)) Some(sheet.getSheetName) else None
        }
        if (sheetNames.nonEmpty) {
          Right(sheetNames)
        } else {
          Left("No non-empty visible sheets found")
        }
      } finally {
        workbook.close()
      }
    } catch {
      case NonFatal(e) =>
        Left(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
    } finally {
      inputStream.close()
    }
  }

  private def buildScanResults(
    datasetPath: String,
    timestamp: String,
    fileIdentifier: String,
    sampledRowCount: Long,
    matchCounts: Seq[MatchCount]
  ): Seq[ScanResult] = {
    if (sampledRowCount <= 0L) {
      Seq.empty
    } else {
      matchCounts.map { matchCount =>
        val matchRatio = roundProbability(matchCount.count.toDouble / sampledRowCount.toDouble)
        ScanResult(
          dataset_path = datasetPath,
          scan_timestamp = timestamp,
          file_identifier = fileIdentifier,
          column_name = matchCount.columnName,
          pii_type = matchCount.piiType,
          match_count = matchCount.count,
          match_ratio = matchRatio,
          confidence = matchRatio
        )
      }
    }
  }

  private def roundProbability(value: Double): Double = {
    BigDecimal.decimal(value)
      .setScale(2, scala.math.BigDecimal.RoundingMode.HALF_UP)
      .toDouble
  }

  @tailrec
  private def collectThrowableChain(current: Throwable, acc: Vector[Throwable] = Vector.empty): Vector[Throwable] = {
    if (current == null || acc.contains(current)) {
      acc
    } else {
      collectThrowableChain(current.getCause, acc :+ current)
    }
  }

  private def formatThrowableSummary(error: Throwable): String = {
    collectThrowableChain(error)
      .flatMap { cause =>
        Option(cause.getMessage)
          .filter(_.trim.nonEmpty)
          .map(message => s"${cause.getClass.getSimpleName}: ${message.trim}")
      }
      .headOption
      .getOrElse(error.getClass.getSimpleName)
  }

  private def isRetriableFileReadFailure(error: Throwable): Boolean = {
    collectThrowableChain(error).exists {
      case _: java.io.FileNotFoundException => true
      case _: NoSuchFileException => true
      case cause =>
        val normalizedMessage = Option(cause.getMessage).map(_.toLowerCase).getOrElse("")
        RetriableFileReadErrorSnippets.exists(normalizedMessage.contains)
    }
  }

  private def refreshReadPaths(spark: SparkSession, filePaths: Seq[String]): Unit = {
    val refreshTargets = filePaths.distinct.flatMap { path =>
      Seq(Some(path), Option(new Path(path).getParent).map(_.toString)).flatten
    }.distinct

    refreshTargets.foreach { path =>
      try {
        spark.catalog.refreshByPath(path)
      } catch {
        case NonFatal(_) => ()
      }
    }
  }

  private def pauseBeforeRetry(delayMs: Long): Unit = {
    if (delayMs > 0L) {
      try {
        Thread.sleep(delayMs)
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          throw new IllegalStateException("File read retry interrupted")
      }
    }
  }

  private[privyspark] def withFileReadRetry[A](
    spark: SparkSession,
    filePaths: Seq[String],
    operationName: String,
    maxAttempts: Int = MaxFileReadAttempts,
    retryDelayMs: Long = FileReadRetryDelayMillis
  )(block: => A): A = {
    require(maxAttempts >= 1, "maxAttempts must be >= 1")

    def attempt(attemptNumber: Int): A = {
      try {
        block
      } catch {
        case NonFatal(error) if attemptNumber < maxAttempts && isRetriableFileReadFailure(error) =>
          val nextAttempt = attemptNumber + 1
          val reason = formatThrowableSummary(error)
          logWarn(
            "file_read_retry",
            "operation" -> operationName,
            "attempt" -> nextAttempt,
            "max_attempts" -> maxAttempts,
            "files" -> filePaths.size,
            "reason" -> reason
          )
          logDebug(
            "file_read_retry",
            "operation" -> operationName,
            "attempt" -> nextAttempt,
            "max_attempts" -> maxAttempts,
            "files" -> filePaths.size,
            "reason" -> reason
          )
          refreshReadPaths(spark, filePaths)
          pauseBeforeRetry(retryDelayMs)
          attempt(nextAttempt)
      }
    }

    attempt(1)
  }

  private def resolveParallelism(itemCount: Int, configured: Int): Int = {
    if (itemCount <= 1) 1 else math.max(1, math.min(itemCount, configured))
  }

  private[privyspark] def defaultPreScanParallelism: Int = {
    DefaultPreScanParallelism
  }

  private[privyspark] def maxSafePreScanParallelism: Int = {
    MaxSafePreScanParallelism
  }

  private[privyspark] def resolveConfiguredPreScanParallelism(fileCount: Int, configured: Int, source: String): Int = {
    if (configured <= 0) {
      throw new IllegalArgumentException(s"$source must be > 0")
    }

    resolveParallelism(fileCount, math.min(configured, maxSafePreScanParallelism))
  }

  private[privyspark] def resolvePreScanParallelism(spark: SparkSession, fileCount: Int): Int = {
    spark.sparkContext.getConf.getOption(PreScanParallelismConfKey) match {
      case Some(_) =>
        resolveConfiguredPreScanParallelism(
          fileCount,
          spark.sparkContext.getConf.getInt(PreScanParallelismConfKey, defaultPreScanParallelism),
          PreScanParallelismConfKey
        )
      case None =>
        resolveParallelism(fileCount, defaultPreScanParallelism)
    }
  }

  private[privyspark] def resolveGroupParallelism(spark: SparkSession, groupCount: Int): Int = {
    resolveParallelism(groupCount, spark.sparkContext.getConf.getInt(GroupParallelismConfKey, DefaultGroupParallelism))
  }

  private[privyspark] def resolveFileParallelism(spark: SparkSession, fileCount: Int): Int = {
    resolveParallelism(fileCount, spark.sparkContext.getConf.getInt(FileParallelismConfKey, DefaultFileParallelism))
  }

  private[privyspark] def resolveCliParallelism(config: CliConfig): (Int, Int, Int) = {
    (
      config.preScanParallelism.getOrElse(-1),
      config.groupParallelism.getOrElse(-1),
      config.fileParallelism.getOrElse(-1)
    )
  }

  private[privyspark] def renderConfiguredParallelism(configured: Option[Int]): String = {
    configured.map(_.toString).getOrElse("spark_conf_or_default")
  }

  private[privyspark] def executeInParallel[A](parallelism: Int, tasks: Seq[() => A]): Seq[A] = {
    if (tasks.isEmpty) {
      Seq.empty
    } else if (parallelism <= 1 || tasks.size <= 1) {
      tasks.map(task => task())
    } else {
      val pool = Executors.newFixedThreadPool(parallelism)
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
      try {
        Await.result(Future.sequence(tasks.map(task => Future(task()))), Duration.Inf)
      } finally {
        pool.shutdown()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    runMain(args)
  }

  private[privyspark] def runMain(
    args: Array[String],
    createSparkSession: () => SparkSession = () => SparkSession.builder().appName("PrivySpark").getOrCreate(),
    exitWith: Int => Unit = code => System.exit(code)
  ): Unit = {
    val normalizedArgs = if (args.headOption.contains("scan")) args.drop(1) else args

    val parseResult = Cli.parseWithErrors(normalizedArgs)
    val config = parseResult.config.getOrElse {
      DriverLogger.emitAlways(
        DriverLogLevel.Error,
        "cli_argument_invalid",
        "errors" -> parseResult.errors.mkString(" | "),
        "args" -> normalizedArgs.mkString(" ")
      )
      exitWith(2)
      return
    }

    if (!PathValidator.isAbsolute(config.inputPath)) {
      DriverLogger.emitAlways(
        DriverLogLevel.Error,
        "cli_argument_invalid",
        "argument" -> "--path",
        "reason" -> "must_be_absolute_path_or_uri",
        "value" -> config.inputPath
      )
      exitWith(2)
      return
    }

    if (!PathValidator.isAbsolute(config.outputPath)) {
      DriverLogger.emitAlways(
        DriverLogLevel.Error,
        "cli_argument_invalid",
        "argument" -> "--output",
        "reason" -> "must_be_absolute_path_or_uri",
        "value" -> config.outputPath
      )
      exitWith(2)
      return
    }

    var spark: Option[SparkSession] = None

    try {
      val session = createSparkSession()
      spark = Some(session)
      session.sparkContext.setLogLevel("WARN")
      runScan(session, config)
    } catch {
      case control: ControlThrowable =>
        throw control
      case NonFatal(e) =>
        DriverLogger.emitAlways(
          DriverLogLevel.Error,
          "scan_failed",
          "exception" -> e.getClass.getSimpleName,
          "reason" -> Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        )
        exitWith(1)
    } finally {
      spark.foreach(_.stop())
    }
  }

  private def runScan(spark: SparkSession, config: CliConfig): Unit = {
    val (preScanParallelism, groupParallelism, fileParallelism) = resolveCliParallelism(config)
    logInfo(
      "scan_start",
      "input_path" -> config.inputPath,
      "output_path" -> config.outputPath,
      "ruleset" -> config.ruleset,
      "sample_ratio" -> config.sampleRatio,
      "file_sample_ratio" -> config.fileSampleRatio.getOrElse("none"),
      "configured_pre_scan_parallelism" -> renderConfiguredParallelism(config.preScanParallelism),
      "configured_group_parallelism" -> renderConfiguredParallelism(config.groupParallelism),
      "configured_file_parallelism" -> renderConfiguredParallelism(config.fileParallelism),
      "driver_log_level" -> DriverLogger.currentLogLevel.label.toLowerCase
    )
    val rules = RulesetLoader.load(config.ruleset)
    logDebug("ruleset_loaded", "rules" -> rules.size, "ruleset" -> config.ruleset)
    val timestamp = Instant.now().toString
    val scanPlan = scanDirectoryStructure(spark, config.inputPath, config.inputPath, timestamp, preScanParallelism)
    var progressRun: Option[ProgressRun] = None
    var heartbeatExecutor: Option[ScheduledExecutorService] = None
    try {
      logInfo(
        "scan_plan_ready",
        "groups" -> scanPlan.groups.size,
        "plan_errors" -> scanPlan.errors.size,
        "total_files" -> scanPlan.totalFiles,
        "directories" -> scanPlan.directoryCount
      )
      val preparedProgressRun = prepareProgressRun(
        spark.sparkContext.hadoopConfiguration,
        config.outputPath,
        config.inputPath,
        timestamp
      )
      progressRun = Some(preparedProgressRun)
      heartbeatExecutor = Some(startProgressHeartbeat(spark.sparkContext.hadoopConfiguration, preparedProgressRun))
      if (scanPlan.errors.nonEmpty) {
        persistProgressRecords(
          spark.sparkContext.hadoopConfiguration,
          preparedProgressRun,
          "plan",
          config.inputPath,
          Seq.empty,
          scanPlan.errors
        )
      }

      scanGroups(
        spark,
        config.inputPath,
        scanPlan.groups,
        rules,
        config.sampleRatio,
        timestamp,
        groupParallelism,
        fileParallelism,
        config.fileSampleRatio,
        Some(preparedProgressRun),
        retainPayloads = false
      )

      logDebug("report_write_start", "output_root" -> config.outputPath, "progress_run" -> preparedProgressRun.runId)
      val (resultCount, errorCount) = mergeProgressReports(spark, config.outputPath, preparedProgressRun)
      logDebug("report_write_complete", "results" -> resultCount, "errors" -> errorCount, "output_root" -> config.outputPath)
      logInfo(
        "scan_complete",
        "scanned_files" -> scanPlan.totalFiles,
        "grouped_dirs" -> scanPlan.directoryCount,
        "groups" -> scanPlan.groups.size,
        "detections" -> resultCount,
        "errors" -> errorCount,
        "output_root" -> config.outputPath
      )

      println(
        s"[PrivySpark] scanned_files=${scanPlan.totalFiles}, grouped_dirs=${scanPlan.directoryCount}, groups=${scanPlan.groups.size}, detections=$resultCount, errors=$errorCount"
      )
    } catch {
      case NonFatal(e) =>
        heartbeatExecutor.foreach(stopProgressHeartbeat)
        progressRun.foreach { run =>
          markProgressRunFailed(spark.sparkContext.hadoopConfiguration, run, Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
        }
        throw e
    } finally {
      heartbeatExecutor.foreach(stopProgressHeartbeat)
      cleanupStagingPaths(spark.sparkContext.hadoopConfiguration, scanPlan.stagingPaths)
    }
  }

  private[privyspark] def scanGroups(
    spark: SparkSession,
    datasetPath: String,
    groups: Seq[ScanGroup],
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    groupParallelism: Int = -1,
    fileParallelism: Int = -1,
    fileSampleRatio: Option[Double] = None,
    progressRun: Option[ProgressRun] = None,
    retainPayloads: Boolean = true
  ): Seq[(ScanGroup, Seq[ScanResult], Seq[ScanError])] = {
    if (groups.isEmpty) {
      return Seq.empty
    }

    val parallelism = if (groupParallelism > 0) {
      resolveParallelism(groups.size, groupParallelism)
    } else {
      resolveGroupParallelism(spark, groups.size)
    }
    logDebug("group_scan_parallelism", "groups" -> groups.size, "parallelism" -> parallelism)

    executeInParallel(parallelism, groups.map { group =>
      () => {
        logDebug(
          "group_scan_dispatch",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "files" -> group.filePaths.size,
          "use_directory_identifier" -> group.useDirectoryIdentifier,
          "parallelism" -> parallelism
        )
        val (groupResults, groupErrors) =
          scanGroup(spark, datasetPath, group, rules, sampleRatio, timestamp, fileParallelism, fileSampleRatio, progressRun)
        logDebug(
          "group_scan_recorded",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "result_rows" -> groupResults.size,
          "error_rows" -> groupErrors.size
        )
        if (retainPayloads) {
          (group, groupResults, groupErrors)
        } else {
          (group, Seq.empty, Seq.empty)
        }
      }
    })
  }

  private def expandPhysicalSource(
    conf: org.apache.hadoop.conf.Configuration,
    datasetPath: String,
    timestamp: String,
    physicalPath: String,
    logicalIdentifier: String,
    groupingDirectoryPath: String,
    stagingPaths: ArrayBuffer[String],
    archiveExpansionDepth: Int = 0,
    forceDisableDirectoryIdentifier: Boolean = false
  ): (Seq[ScanFileEntry], Seq[ScanError]) = {
    try {
      if (isZeroBytePhysicalFile(conf, physicalPath)) {
        return (Seq.empty, Seq.empty)
      }
    } catch {
      case NonFatal(e) =>
        return (
          Seq.empty,
          Seq(ScanError(datasetPath, timestamp, logicalIdentifier, Option(e.getMessage).getOrElse(e.getClass.getSimpleName)))
        )
    }

    val detectedFormat =
      try {
        detectPhysicalFormat(conf, physicalPath)
      } catch {
        case NonFatal(e) =>
          return (
            Seq.empty,
            Seq(ScanError(datasetPath, timestamp, logicalIdentifier, Option(e.getMessage).getOrElse(e.getClass.getSimpleName)))
          )
      }
    detectedFormat match {
      case Some(format) if ArchiveFormats.contains(format) && archiveExpansionDepth < MaxArchiveExpansionDepth =>
        expandArchiveSource(
          conf,
          datasetPath,
          timestamp,
          physicalPath,
          logicalIdentifier,
          stagingPaths,
          archiveExpansionDepth + 1
        )
      case Some(format) if ArchiveFormats.contains(format) =>
        (
          Seq.empty,
          Seq(ScanError(datasetPath, timestamp, logicalIdentifier, s"Nested archive expansion is not supported: $logicalIdentifier"))
        )
      case Some(XlsxFormat) =>
        expandWorkbookSource(conf, datasetPath, timestamp, physicalPath, logicalIdentifier)
      case Some(format) =>
        (
          Seq(
            ScanFileEntry(
              sourceKey = physicalPath,
              physicalPath = physicalPath,
              directoryPath = groupingDirectoryPath,
              format = format,
              logicalIdentifier = logicalIdentifier,
              allowDirectoryIdentifier = !forceDisableDirectoryIdentifier && !NonDirectoryIdentifierFormats.contains(format)
            )
          ),
          Seq.empty
        )
      case None =>
        (
          Seq.empty,
          Seq(ScanError(datasetPath, timestamp, logicalIdentifier, s"Unsupported file format: $logicalIdentifier"))
        )
    }
  }

  private def expandWorkbookSource(
    conf: org.apache.hadoop.conf.Configuration,
    datasetPath: String,
    timestamp: String,
    physicalPath: String,
    logicalIdentifier: String
  ): (Seq[ScanFileEntry], Seq[ScanError]) = {
    listVisibleWorkbookSheets(conf, physicalPath) match {
      case Right(sheetNames) =>
        (
          sheetNames.map { sheetName =>
            ScanFileEntry(
              sourceKey = s"$physicalPath#$sheetName",
              physicalPath = physicalPath,
              directoryPath = logicalIdentifier,
              format = XlsxFormat,
              logicalIdentifier = s"$logicalIdentifier#$sheetName",
              readOptions = ScanReadOptions(sheetName = Some(sheetName)),
              allowDirectoryIdentifier = false
            )
          },
          Seq.empty
        )
      case Left(errorMessage) =>
        (
          Seq.empty,
          Seq(ScanError(datasetPath, timestamp, logicalIdentifier, s"Workbook read failed: $errorMessage"))
        )
    }
  }

  private def expandArchiveSource(
    conf: org.apache.hadoop.conf.Configuration,
    datasetPath: String,
    timestamp: String,
    archivePath: String,
    logicalIdentifier: String,
    stagingPaths: ArrayBuffer[String],
    archiveExpansionDepth: Int
  ): (Seq[ScanFileEntry], Seq[ScanError]) = {
    val sourcePath = new Path(archivePath)
    val fs = sourcePath.getFileSystem(conf)
    val extractedEntries = ArrayBuffer.empty[ScanFileEntry]
    val archiveErrors = ArrayBuffer.empty[ScanError]
    val stagingBase = new Path(fs.getHomeDirectory, ".privyspark-staging")
    val stagingRoot = new Path(
      stagingBase,
      s"archive-${System.currentTimeMillis()}-${math.abs(scala.util.Random.nextLong())}"
    )
    val archiveInputStream = fs.open(sourcePath)
    val zipInputStream = new ZipInputStream(archiveInputStream)
    val stagedTargetPaths = scala.collection.mutable.Set.empty[String]
    var stagingPrepared = false

    def ensureArchiveStagingReady(): Either[String, Unit] = {
      if (stagingPrepared) {
        Right(())
      } else if (!fs.exists(stagingBase) && !fs.mkdirs(stagingBase)) {
        Left(s"Archive staging base creation failed: ${stagingBase.toString}")
      } else if (!fs.mkdirs(stagingRoot) && !fs.exists(stagingRoot)) {
        Left(s"Archive staging directory creation failed: ${stagingRoot.toString}")
      } else {
        stagingPaths += stagingRoot.toString
        stagingPrepared = true
        Right(())
      }
    }

    def reserveStagedTargetPath(normalizedEntryName: String, targetPath: Path): Either[String, Unit] = {
      val targetComparablePath = canonicalizePath(targetPath.toString)
      if (stagedTargetPaths.add(targetComparablePath)) Right(()) else Left(s"Conflicting archive entry path: $normalizedEntryName")
    }

    try {
      var entry = zipInputStream.getNextEntry
      while (entry != null) {
        if (!entry.isDirectory) {
          if (entry.getSize == 0L) {
            logDebug(
              "archive_entry_skipped",
              "archive" -> logicalIdentifier,
              "entry" -> normalizeArchiveEntryName(entry.getName),
              "reason" -> "zero_byte"
            )
          } else {
            val normalizedEntryName = normalizeArchiveEntryName(entry.getName)
            val childLogicalIdentifier = s"$logicalIdentifier!$normalizedEntryName"
            safeResolveArchiveEntryPath(stagingRoot, normalizedEntryName) match {
              case Some(targetPath) =>
                try {
                  FormatDetector.infer(normalizedEntryName) match {
                    case Some(format) if ArchiveFormats.contains(format) && archiveExpansionDepth >= MaxArchiveExpansionDepth =>
                      if (zipInputStream.read() >= 0) {
                        archiveErrors += ScanError(
                          datasetPath,
                          timestamp,
                          childLogicalIdentifier,
                          s"Nested archive expansion is not supported: $childLogicalIdentifier"
                        )
                      } else {
                        logDebug(
                          "archive_entry_skipped",
                          "archive" -> logicalIdentifier,
                          "entry" -> childLogicalIdentifier,
                          "reason" -> "zero_byte"
                        )
                      }
                    case Some(_) =>
                      val buffer = new Array[Byte](8192)
                      var bytesRead = zipInputStream.read(buffer)
                      if (bytesRead < 0) {
                        logDebug(
                          "archive_entry_skipped",
                          "archive" -> logicalIdentifier,
                          "entry" -> childLogicalIdentifier,
                          "reason" -> "zero_byte"
                        )
                      } else {
                        ensureArchiveStagingReady() match {
                          case Left(errorMessage) =>
                            archiveErrors += ScanError(datasetPath, timestamp, childLogicalIdentifier, errorMessage)
                          case Right(_) =>
                            reserveStagedTargetPath(normalizedEntryName, targetPath) match {
                              case Left(errorMessage) =>
                                archiveErrors += ScanError(datasetPath, timestamp, childLogicalIdentifier, errorMessage)
                              case Right(_) =>
                                ensureArchiveEntryParent(fs, targetPath) match {
                                  case Left(errorMessage) =>
                                    archiveErrors += ScanError(datasetPath, timestamp, childLogicalIdentifier, errorMessage)
                                  case Right(_) =>
                                    val outputStream = fs.create(targetPath, true)
                                    try {
                                      while (bytesRead >= 0) {
                                        if (bytesRead > 0) {
                                          outputStream.write(buffer, 0, bytesRead)
                                        }
                                        bytesRead = zipInputStream.read(buffer)
                                      }
                                    } finally {
                                      outputStream.close()
                                    }

                                    val (childEntries, childErrors) = expandPhysicalSource(
                                      conf,
                                      datasetPath,
                                      timestamp,
                                      targetPath.toString,
                                      childLogicalIdentifier,
                                      logicalIdentifier,
                                      stagingPaths,
                                      archiveExpansionDepth = archiveExpansionDepth,
                                      forceDisableDirectoryIdentifier = true
                                    )
                                    extractedEntries ++= childEntries
                                    archiveErrors ++= childErrors
                                }
                            }
                        }
                      }
                    case None =>
                      val probeBuffer = new java.io.ByteArrayOutputStream()
                      val buffer = new Array[Byte](8192)
                      var outputStream: org.apache.hadoop.fs.FSDataOutputStream = null
                      var detectedFormat: Option[String] = None
                      var archiveEntryError: Option[String] = None
                      var probeRejected = false
                      var archiveEntryHasContent = false
                      var archiveEntrySkipped = false
                      var targetPathReserved = false
                      var bytesRead = zipInputStream.read(buffer)

                      def materializeDetectedEntry(format: String, bytesForProbe: Int, currentChunkSize: Int): Unit = {
                        ensureArchiveStagingReady() match {
                          case Left(errorMessage) =>
                            archiveEntryError = Some(errorMessage)
                          case Right(_) =>
                            if (!targetPathReserved) {
                              reserveStagedTargetPath(normalizedEntryName, targetPath) match {
                                case Left(errorMessage) =>
                                  archiveEntryError = Some(errorMessage)
                                case Right(_) =>
                                  targetPathReserved = true
                              }
                            }
                            if (archiveEntryError.isEmpty) {
                              ensureArchiveEntryParent(fs, targetPath) match {
                                case Left(errorMessage) =>
                                  archiveEntryError = Some(errorMessage)
                                case Right(_) =>
                                  outputStream = fs.create(targetPath, true)
                                  outputStream.write(probeBuffer.toByteArray)
                                  if (currentChunkSize > bytesForProbe) {
                                    outputStream.write(buffer, bytesForProbe, currentChunkSize - bytesForProbe)
                                  }
                                  detectedFormat = Some(format)
                              }
                            }
                        }
                      }

                      try {
                        while (bytesRead >= 0 && archiveEntryError.isEmpty) {
                          if (bytesRead > 0) {
                            archiveEntryHasContent = true
                            if (detectedFormat.isDefined) {
                              outputStream.write(buffer, 0, bytesRead)
                            } else if (!probeRejected) {
                              val remainingProbeSpace = TextProbeByteLimit - probeBuffer.size()
                              val bytesForProbe = math.min(bytesRead, math.max(0, remainingProbeSpace))
                              if (bytesForProbe > 0) {
                                probeBuffer.write(buffer, 0, bytesForProbe)
                              }

                              val probeBytes = probeBuffer.toByteArray
                              val probeComplete = probeBuffer.size() >= TextProbeByteLimit
                              val probeTruncated = probeComplete && bytesRead > bytesForProbe
                              val format = inferMagicByteFormat(probeBytes).orElse {
                                if (probeTruncated) inferTextFormat(probeBytes, allowIncompleteTrailingSequence = true) else None
                              }

                              format match {
                                case Some(value) =>
                                  materializeDetectedEntry(value, bytesForProbe, bytesRead)
                                case None if probeTruncated =>
                                  probeRejected = true
                                case None =>
                                  ()
                              }
                            }
                          }
                          bytesRead = zipInputStream.read(buffer)
                        }

                        if (archiveEntryError.isEmpty && detectedFormat.isEmpty && !probeRejected) {
                          if (archiveEntryHasContent || probeBuffer.size() > 0) {
                            inferMagicByteFormat(probeBuffer.toByteArray)
                              .orElse(inferTextFormat(probeBuffer.toByteArray, allowIncompleteTrailingSequence = false)) match {
                              case Some(format) =>
                                materializeDetectedEntry(format, bytesForProbe = probeBuffer.size(), currentChunkSize = probeBuffer.size())
                              case None =>
                                probeRejected = true
                            }
                          } else {
                            archiveEntrySkipped = true
                          }
                        }
                        archiveEntryError match {
                          case Some(errorMessage) =>
                            archiveErrors += ScanError(datasetPath, timestamp, childLogicalIdentifier, errorMessage)
                          case None =>
                            if (archiveEntrySkipped) {
                              ()
                            } else detectedFormat match {
                              case Some(format) =>
                                extractedEntries += ScanFileEntry(
                                  sourceKey = targetPath.toString,
                                  physicalPath = targetPath.toString,
                                  directoryPath = logicalIdentifier,
                                  format = format,
                                  logicalIdentifier = childLogicalIdentifier,
                                  allowDirectoryIdentifier = false
                                )
                              case None =>
                                archiveErrors += ScanError(
                                  datasetPath,
                                  timestamp,
                                  childLogicalIdentifier,
                                  s"Unsupported file format: $childLogicalIdentifier"
                                )
                            }
                        }
                      } finally {
                        if (outputStream != null) {
                          outputStream.close()
                        }
                      }
                  }
                } catch {
                  case NonFatal(e) =>
                    archiveErrors += ScanError(
                      datasetPath,
                      timestamp,
                      childLogicalIdentifier,
                      s"Archive entry materialization failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}"
                    )
                }
              case None =>
                if (zipInputStream.read() >= 0) {
                  archiveErrors += ScanError(
                    datasetPath,
                    timestamp,
                    childLogicalIdentifier,
                    s"Unsafe archive entry path: $normalizedEntryName"
                  )
                } else {
                  logDebug(
                    "archive_entry_skipped",
                    "archive" -> logicalIdentifier,
                    "entry" -> childLogicalIdentifier,
                    "reason" -> "zero_byte"
                  )
                }
            }
          }
        }
        zipInputStream.closeEntry()
        entry = zipInputStream.getNextEntry
      }
    } catch {
      case NonFatal(e) =>
        archiveErrors += ScanError(
          datasetPath,
          timestamp,
          logicalIdentifier,
          s"Archive read failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}"
        )
    } finally {
      zipInputStream.close()
      archiveInputStream.close()
    }

    (extractedEntries.toSeq, archiveErrors.toSeq)
  }

  private[privyspark] def scanDirectoryStructure(
    spark: SparkSession,
    inputPath: String,
    datasetPath: String,
    timestamp: String,
    preScanParallelism: Int = -1
  ): DirectoryScanPlan = {
    logDebug("scan_directory_structure_start", "input_path" -> inputPath, "dataset_path" -> datasetPath)
    val conf = spark.sparkContext.hadoopConfiguration
    val path = new Path(inputPath)
    val fs = path.getFileSystem(conf)
    val stagingPaths = ArrayBuffer.empty[String]
    val fileDiscoveryStartedAt = System.nanoTime()

    try {
      if (!fs.exists(path)) {
        throw new IllegalArgumentException(s"Input path not found: $inputPath")
      }

      val files = if (fs.getFileStatus(path).isFile) {
        Seq(path.toString)
      } else {
        val iter = fs.listFiles(path, true)
        val discoveredFiles = ArrayBuffer.empty[String]
        while (iter.hasNext) {
          val status = iter.next()
          if (status.isFile) {
            discoveredFiles += status.getPath.toString
          }
        }
        discoveredFiles.toSeq.sorted
      }
      logDebug(
        "scan_directory_files_discovered",
        "input_path" -> inputPath,
        "files" -> files.size,
        "duration_ms" -> elapsedMillis(fileDiscoveryStartedAt)
      )

      val supportedFiles = ArrayBuffer.empty[ScanFileEntry]
      val errors = ArrayBuffer.empty[ScanError]
      val directoriesWithPreScanErrors = scala.collection.mutable.Set.empty[String]
      val resolvedPreScanParallelism = if (preScanParallelism > 0) {
        resolveConfiguredPreScanParallelism(files.size, preScanParallelism, "--pre-scan-parallelism")
      } else {
        resolvePreScanParallelism(spark, files.size)
      }

      logDebug(
        "scan_directory_pre_scan_parallelism",
        "input_path" -> inputPath,
        "files" -> files.size,
        "parallelism" -> resolvedPreScanParallelism
      )

      val preScanStartedAt = System.nanoTime()
      val preScanProgressInterval = resolvePreScanProgressInterval(files.size)
      val completedPreScanFiles = new AtomicInteger(0)
      logDebug(
        "scan_directory_pre_scan_execute_start",
        "input_path" -> inputPath,
        "files" -> files.size,
        "parallelism" -> resolvedPreScanParallelism,
        "progress_interval" -> preScanProgressInterval
      )

      val preScanOutcomes = executeInParallel(resolvedPreScanParallelism, files.map { filePath =>
        () => {
          val parentDirectory = Option(new Path(filePath).getParent).map(_.toString).getOrElse(filePath)
          val logicalIdentifier = resolveRelativeIdentifier(datasetPath, filePath)
          val pathInferredFormat = FormatDetector.infer(filePath)
          val preScanErrorScope = pathInferredFormat match {
            case Some(format) if ArchiveFormats.contains(format) || format == XlsxFormat => logicalIdentifier
            case _ => parentDirectory
          }
          val localStagingPaths = ArrayBuffer.empty[String]

          val outcome =
            try {
              val zeroByteStatus = try {
                Right(isZeroBytePhysicalFile(conf, filePath))
              } catch {
                case NonFatal(e) => Left(e)
              }

              zeroByteStatus match {
                case Left(e) =>
                  PreScanFileOutcome(
                    filePath = filePath,
                    groupingDirectoryPath = parentDirectory,
                    preScanErrorScope = preScanErrorScope,
                    expandedEntries = Seq.empty,
                    expandedErrors = Seq(ScanError(datasetPath, timestamp, logicalIdentifier, Option(e.getMessage).getOrElse(e.getClass.getSimpleName))),
                    stagingPaths = localStagingPaths.toSeq,
                    pathInferredFormat = pathInferredFormat,
                    probeRequired = pathInferredFormat.isEmpty
                  )
                case Right(true) =>
                  PreScanFileOutcome(
                    filePath = filePath,
                    groupingDirectoryPath = parentDirectory,
                    preScanErrorScope = preScanErrorScope,
                    expandedEntries = Seq.empty,
                    expandedErrors = Seq.empty,
                    stagingPaths = localStagingPaths.toSeq,
                    pathInferredFormat = pathInferredFormat,
                    probeRequired = pathInferredFormat.isEmpty,
                    skipped = true
                  )
                case Right(false) =>
                  val (expandedEntries, expandedErrors) =
                    expandPhysicalSource(conf, datasetPath, timestamp, filePath, logicalIdentifier, parentDirectory, localStagingPaths)
                  PreScanFileOutcome(
                    filePath = filePath,
                    groupingDirectoryPath = parentDirectory,
                    preScanErrorScope = preScanErrorScope,
                    expandedEntries = expandedEntries,
                    expandedErrors = expandedErrors,
                    stagingPaths = localStagingPaths.toSeq,
                    pathInferredFormat = pathInferredFormat,
                    probeRequired = pathInferredFormat.isEmpty
                  )
              }
            } catch {
              case NonFatal(e) =>
                PreScanFileOutcome(
                  filePath = filePath,
                  groupingDirectoryPath = parentDirectory,
                  preScanErrorScope = preScanErrorScope,
                  expandedEntries = Seq.empty,
                  expandedErrors = Seq.empty,
                  stagingPaths = localStagingPaths.toSeq,
                  pathInferredFormat = pathInferredFormat,
                  probeRequired = pathInferredFormat.isEmpty,
                  failure = Some(e)
                )
            }

          val completedFiles = completedPreScanFiles.incrementAndGet()
          if (completedFiles == files.size || completedFiles % preScanProgressInterval == 0) {
            logDebug(
              "scan_directory_pre_scan_progress",
              "input_path" -> inputPath,
              "completed_files" -> completedFiles,
              "total_files" -> files.size,
              "elapsed_ms" -> elapsedMillis(preScanStartedAt)
            )
          }
          outcome
        }
      })

      logDebug(
        "scan_directory_pre_scan_execute_complete",
        "input_path" -> inputPath,
        "files" -> files.size,
        "parallelism" -> resolvedPreScanParallelism,
        "duration_ms" -> elapsedMillis(preScanStartedAt),
        "completed_files" -> preScanOutcomes.size,
        "skipped_files" -> preScanOutcomes.count(_.skipped),
        "expanded_entries" -> preScanOutcomes.map(_.expandedEntries.size.toLong).sum,
        "error_entries" -> preScanOutcomes.map(_.expandedErrors.size.toLong).sum,
        "failure_files" -> preScanOutcomes.count(_.failure.isDefined),
        "probe_candidates" -> preScanOutcomes.count(_.probeRequired),
        "archive_candidates" -> preScanOutcomes.count(_.pathInferredFormat.exists(ArchiveFormats.contains)),
        "xlsx_candidates" -> preScanOutcomes.count(_.pathInferredFormat.contains(XlsxFormat))
      )

      val preScanCollectStartedAt = System.nanoTime()
      logDebug("scan_directory_pre_scan_collect_start", "input_path" -> inputPath, "outcomes" -> preScanOutcomes.size)
      preScanOutcomes.foreach(outcome => stagingPaths ++= outcome.stagingPaths)
      preScanOutcomes.flatMap(_.failure).headOption.foreach { failure =>
        throw failure
      }

      preScanOutcomes.foreach { outcome =>
        supportedFiles ++= outcome.expandedEntries
        errors ++= outcome.expandedErrors

        if (outcome.skipped) {
          logDebug(
            "scan_directory_file_skipped",
            "file" -> outcome.filePath,
            "directory" -> outcome.groupingDirectoryPath,
            "reason" -> "zero_byte"
          )
        } else if (outcome.expandedEntries.nonEmpty) {
          logDebug(
            "scan_directory_file_supported",
            "file" -> outcome.filePath,
            "expanded_entries" -> outcome.expandedEntries.size,
            "formats" -> outcome.expandedEntries.map(_.format).distinct.sorted.mkString(","),
            "directory" -> outcome.groupingDirectoryPath
          )
        }
        if (outcome.expandedErrors.nonEmpty) {
          directoriesWithPreScanErrors += outcome.preScanErrorScope
          logDebug(
            "scan_directory_file_unsupported",
            "file" -> outcome.filePath,
            "directory" -> outcome.preScanErrorScope,
            "errors" -> outcome.expandedErrors.size
          )
        }
      }

      logDebug(
        "scan_directory_pre_scan_collect_complete",
        "input_path" -> inputPath,
        "duration_ms" -> elapsedMillis(preScanCollectStartedAt),
        "supported_files" -> supportedFiles.size,
        "errors" -> errors.size,
        "directories_with_pre_scan_errors" -> directoriesWithPreScanErrors.size
      )

      val groupBuildStartedAt = System.nanoTime()
      logDebug("scan_directory_group_build_start", "input_path" -> inputPath, "supported_files" -> supportedFiles.size)
      val groupedByDirectoryAndFormat = supportedFiles
        .groupBy(file => (file.directoryPath, file.format))
        .toSeq
        .sortBy { case ((directoryPath, format), _) => (directoryPath, format) }
        .map {
          case ((directoryPath, format), groupedFiles) =>
            val sortedFiles = groupedFiles.sortBy(_.sourceKey)
            ScanGroup(
              directoryPath = directoryPath,
              format = format,
              schemaSignature = "",
              filePaths = sortedFiles.map(_.sourceKey),
              physicalPathsByKey = sortedFiles.map(file => file.sourceKey -> file.physicalPath).toMap,
              logicalIdentifiersByKey = sortedFiles.map(file => file.sourceKey -> file.logicalIdentifier).toMap,
              readOptionsByKey = sortedFiles.collect {
                case file if file.readOptions != ScanReadOptions() => file.sourceKey -> file.readOptions
              }.toMap,
              allowDirectoryIdentifier = sortedFiles.forall(_.allowDirectoryIdentifier)
            )
        }
      logDebug(
        "scan_directory_initial_groups_ready",
        "groups" -> groupedByDirectoryAndFormat.size,
        "supported_files" -> supportedFiles.size,
        "duration_ms" -> elapsedMillis(groupBuildStartedAt)
      )

      val schemaAwareGroups = ArrayBuffer.empty[ScanGroup]
      val schemaSplitParallelism = resolveParallelism(groupedByDirectoryAndFormat.size, resolvedPreScanParallelism)
      logDebug(
        "scan_directory_schema_split_parallelism",
        "groups" -> groupedByDirectoryAndFormat.size,
        "parallelism" -> schemaSplitParallelism
      )
      val schemaSplitOutcomes = executeInParallel(schemaSplitParallelism, groupedByDirectoryAndFormat.map { group =>
        () =>
          val (splitGroups, splitErrors) = splitGroupBySchemaFast(spark, datasetPath, timestamp, group)
          (group, splitGroups, splitErrors)
      })

      schemaSplitOutcomes.foreach {
        case (group, splitGroups, splitErrors) =>
          schemaAwareGroups ++= splitGroups
          errors ++= splitErrors
          logDebug(
            "scan_directory_group_schema_split",
            "directory" -> group.directoryPath,
            "format" -> group.format,
            "input_files" -> group.filePaths.size,
            "split_groups" -> splitGroups.size,
            "split_errors" -> splitErrors.size
          )
          if (splitErrors.nonEmpty) {
            directoriesWithPreScanErrors += group.directoryPath
          }
      }

      val groupsPerDirectory = schemaAwareGroups.groupBy(_.directoryPath).map {
        case (directoryPath, groups) => directoryPath -> groups.size
      }

      val finalizedGroups = schemaAwareGroups.map { group =>
        val directoryIdentifierEligible =
          group.allowDirectoryIdentifier &&
            groupsPerDirectory.getOrElse(group.directoryPath, 0) == 1 &&
            group.filePaths.size > 1 &&
            !directoriesWithPreScanErrors.contains(group.directoryPath)
        val finalizedGroup = group.copy(
          useDirectoryIdentifier = directoryIdentifierEligible && !group.schemaSampled,
          directoryIdentifierEligible = directoryIdentifierEligible
        )
        logDebug(
          "scan_group_planned",
          "directory" -> finalizedGroup.directoryPath,
          "format" -> finalizedGroup.format,
          "schema" -> finalizedGroup.schemaSignature,
          "files" -> finalizedGroup.filePaths.size,
          "use_directory_identifier" -> finalizedGroup.useDirectoryIdentifier,
          "schema_sampled" -> finalizedGroup.schemaSampled,
          "csv_has_header" -> finalizedGroup.csvHasHeader
        )
        finalizedGroup
      }

      val nonSkippedPreScanOutcomes = preScanOutcomes.filterNot(_.skipped)
      val directoryCount = nonSkippedPreScanOutcomes
        .map(outcome => Option(new Path(outcome.filePath).getParent).map(_.toString).getOrElse(outcome.filePath))
        .distinct
        .size
      val totalFiles = nonSkippedPreScanOutcomes.size
      val plannedGroups = finalizedGroups.toSeq.sortBy(group => (group.directoryPath, group.format, group.schemaSignature))

      logDebug(
        "scan_directory_structure_complete",
        "input_path" -> inputPath,
        "total_files" -> totalFiles,
        "supported_files" -> supportedFiles.size,
        "groups" -> plannedGroups.size,
        "errors" -> errors.size,
        "directories" -> directoryCount
      )

      DirectoryScanPlan(
        groups = plannedGroups,
        errors = errors.toSeq,
        totalFiles = totalFiles,
        directoryCount = directoryCount,
        stagingPaths = stagingPaths.toSeq
      )
    } catch {
      case NonFatal(e) =>
        cleanupStagingPaths(conf, stagingPaths.toSeq)
        throw e
    }
  }

  private[privyspark] def splitGroupBySchemaFast(
    spark: SparkSession,
    datasetPath: String,
    timestamp: String,
    group: ScanGroup
  ): (Seq[ScanGroup], Seq[ScanError]) = {
    if (group.filePaths.size <= 1) {
      splitGroupBySchema(spark, datasetPath, timestamp, group)
    } else {
      logDebug(
        "scan_group_schema_sample_start",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "files" -> group.filePaths.size
      )

      val sampledSourceKey = group.filePaths.head
      val sampledPhysicalPath = resolvePhysicalPath(group, sampledSourceKey)
      val sampledReadOptions = resolveReadOptions(group, sampledSourceKey)
      val sampledSchemaResult = if (group.format == "csv") {
        inferCsvSchemaSignature(spark, sampledPhysicalPath)
      } else {
        inferSchemaSignature(spark, group.format, sampledPhysicalPath, sampledReadOptions).map(signature => (signature, true))
      }

      sampledSchemaResult match {
        case Right((schemaSignature, csvHasHeader)) =>
          val (validatedFilePaths, validationErrors) =
            if (group.format == "json") {
              validateSampledJsonFiles(spark, datasetPath, timestamp, group)
            } else {
              (group.filePaths, Seq.empty)
            }

          if (validatedFilePaths.isEmpty) {
            return (Seq.empty, validationErrors)
          }

          val sampledGroup = group.copy(
            schemaSignature = schemaSignature,
            filePaths = validatedFilePaths.sorted,
            schemaSampled = validatedFilePaths.size > 1,
            csvHasHeader = csvHasHeader
          )
          logDebug(
            "scan_group_schema_sample_complete",
            "directory" -> group.directoryPath,
            "format" -> group.format,
            "schema" -> schemaSignature,
            "files" -> validatedFilePaths.size,
            "filtered_errors" -> validationErrors.size,
            "csv_has_header" -> csvHasHeader
          )
          (Seq(sampledGroup), validationErrors)
        case Left(errorMessage) =>
          logDebug(
            "scan_group_schema_sample_fallback",
            "directory" -> group.directoryPath,
            "format" -> group.format,
            "files" -> group.filePaths.size,
            "reason" -> errorMessage
          )
          splitGroupBySchema(spark, datasetPath, timestamp, group)
      }
    }
  }

  private def validateSampledJsonFiles(
    spark: SparkSession,
    datasetPath: String,
    timestamp: String,
    group: ScanGroup
  ): (Seq[String], Seq[ScanError]) = {
    val validFilePaths = ArrayBuffer.empty[String]
    val errors = ArrayBuffer.empty[ScanError]

    group.filePaths.foreach { sourceKey =>
      val physicalPath = resolvePhysicalPath(group, sourceKey)
      val logicalIdentifier = resolveLogicalIdentifier(group, datasetPath, sourceKey)
      try {
        withFileReadRetry(spark, Seq(physicalPath), "schema_detection") {
          readSchemaSource(spark, group.format, physicalPath, group.csvHasHeader)
          ()
        }
        validFilePaths += sourceKey
      } catch {
        case NonFatal(e) =>
          val errorMessage = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
          logDebug(
            "group_schema_signature_failed",
            "directory" -> group.directoryPath,
            "file" -> physicalPath,
            "format" -> group.format,
            "reason" -> errorMessage
          )
          errors += ScanError(
            datasetPath,
            timestamp,
            logicalIdentifier,
            s"Schema detection failed: $errorMessage"
          )
      }
    }

    (validFilePaths.toSeq, errors.toSeq)
  }

  private[privyspark] def splitGroupBySchema(
    spark: SparkSession,
    datasetPath: String,
    timestamp: String,
    group: ScanGroup
  ): (Seq[ScanGroup], Seq[ScanError]) = {
    logDebug(
      "scan_group_schema_split_start",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "files" -> group.filePaths.size
    )
    val filesBySchema = scala.collection.mutable.Map.empty[(String, Boolean), ArrayBuffer[String]]
    val errors = ArrayBuffer.empty[ScanError]

    group.filePaths.foreach { sourceKey =>
      val physicalPath = resolvePhysicalPath(group, sourceKey)
      val readOptions = resolveReadOptions(group, sourceKey)
      val schemaResult = if (group.format == "csv") {
        inferCsvSchemaSignature(spark, physicalPath)
      } else {
        inferSchemaSignature(spark, group.format, physicalPath, readOptions).map(signature => (signature, true))
      }

      schemaResult match {
        case Right((schemaSignature, csvHasHeader)) =>
          val groupedFiles = filesBySchema.getOrElseUpdate((schemaSignature, csvHasHeader), ArrayBuffer.empty[String])
          groupedFiles += sourceKey
          logDebug(
            "group_schema_signature_detected",
            "directory" -> group.directoryPath,
            "file" -> physicalPath,
            "format" -> group.format,
            "schema" -> schemaSignature,
            "csv_has_header" -> csvHasHeader
          )
        case Left(errorMessage) =>
          logDebug(
            "group_schema_signature_failed",
            "directory" -> group.directoryPath,
            "file" -> physicalPath,
            "format" -> group.format,
            "reason" -> errorMessage
          )
          errors += ScanError(
            datasetPath,
            timestamp,
            resolveLogicalIdentifier(group, datasetPath, sourceKey),
            s"Schema detection failed: $errorMessage"
          )
      }
    }

    val groups = filesBySchema.toSeq
      .sortBy { case ((schemaSignature, csvHasHeader), _) => (schemaSignature, csvHasHeader) }
      .map {
        case ((schemaSignature, csvHasHeader), groupedFiles) =>
          group.copy(
            schemaSignature = schemaSignature,
            filePaths = groupedFiles.toSeq.sorted,
            schemaSampled = false,
            csvHasHeader = csvHasHeader
          )
      }

    logDebug(
      "scan_group_schema_split_complete",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema_groups" -> groups.size,
      "errors" -> errors.size
    )
    (groups, errors.toSeq)
  }

  private[privyspark] def parseHeaderFields(line: String): Seq[String] = {
    val sanitizedLine = Option(line).getOrElse("").stripPrefix("\uFEFF")
    val fields = ArrayBuffer.empty[String]
    val buffer = new java.lang.StringBuilder
    var index = 0
    var inQuotes = false

    while (index < sanitizedLine.length) {
      val ch = sanitizedLine.charAt(index)
      if (ch == '"') {
        val nextIsEscapedQuote = inQuotes && index + 1 < sanitizedLine.length && sanitizedLine.charAt(index + 1) == '"'
        if (nextIsEscapedQuote) {
          buffer.append('"')
          index += 1
        } else {
          inQuotes = !inQuotes
        }
      } else if (ch == ',' && !inQuotes) {
        fields += buffer.toString()
        buffer.setLength(0)
      } else {
        buffer.append(ch)
      }
      index += 1
    }

    fields += buffer.toString()
    fields.toSeq
  }

  private[privyspark] def inferCsvHeaderSignature(
    spark: SparkSession,
    filePath: String
  ): Either[String, String] = {
    try {
      val signature = withFileReadRetry(spark, Seq(filePath), "csv_header_signature") {
        val csvOptions = createCsvOptions(spark)
        val headerLine = readFirstNonBlankCsvLines(spark, filePath, maxLines = 1).headOption
          .getOrElse(throw new IllegalArgumentException("Empty or missing CSV header"))
        val headerColumns = CSVUtils.makeSafeHeader(
          parseCsvLine(spark, headerLine),
          spark.sessionState.conf.caseSensitiveAnalysis,
          csvOptions
        )
        headerColumns.map(_.toLowerCase).mkString("|")
      }
      Right(signature)
    } catch {
      case NonFatal(e) =>
        Left(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
    }
  }

  private[privyspark] def detectCsvHasHeader(
    spark: SparkSession,
    filePath: String
  ): Boolean = {
    val lines = readFirstNonBlankCsvLines(spark, filePath, maxLines = 2)
    val firstRowFields = lines.headOption.map(parseCsvLine(spark, _).toSeq).getOrElse(Seq.empty)
    if (firstRowFields.isEmpty) {
      false
    } else {
      val normalizedFields = firstRowFields.map(field => Option(field).getOrElse("").trim.toLowerCase)
      val hasDuplicateFields = normalizedFields.nonEmpty && normalizedFields.distinct.size != normalizedFields.size
      val allNumericFields = firstRowFields.nonEmpty && firstRowFields.forall(isNumericLikeField)
      val firstRowFieldKinds = firstRowFields.map(classifyCsvField)
      val firstRowHasStructuredData = firstRowFields.zip(firstRowFieldKinds).exists {
        case (field, kind) => isStructuredCsvFieldForHeaderHeuristic(field, kind)
      }
      if (hasDuplicateFields || allNumericFields || firstRowHasStructuredData || !looksLikeCsvHeaderRow(firstRowFields)) {
        return false
      }

      if (lines.size <= 1) {
        return firstRowFields.exists(hasStrongCsvHeaderSignal)
      }

      val secondRowFields = parseCsvLine(spark, lines(1)).toSeq
      if (firstRowFields.size != secondRowFields.size) {
        return true
      }

      val secondRowFieldKinds = secondRowFields.map(classifyCsvField)
      if (secondRowFieldKinds.exists(isStructuredCsvFieldKind)) {
        return true
      }

      val firstHeaderScore = scoreCsvHeaderRow(firstRowFields)
      val secondHeaderScore = scoreCsvHeaderRow(secondRowFields)
      secondHeaderScore <= firstHeaderScore
    }
  }

  private[privyspark] def inferCsvSchemaSignature(
    spark: SparkSession,
    filePath: String
  ): Either[String, (String, Boolean)] = {
    try {
      val csvHasHeader = detectCsvHasHeader(spark, filePath)
      if (csvHasHeader) {
        inferCsvHeaderSignature(spark, filePath).map(signature => (signature, true))
      } else {
        val firstDataLine = readFirstNonBlankCsvLines(spark, filePath, maxLines = 1).headOption
          .getOrElse(throw new IllegalArgumentException("Empty CSV file"))
        val columnCount = parseCsvLine(spark, firstDataLine).length
        Right((s"cols:$columnCount", false))
      }
    } catch {
      case NonFatal(e) =>
        Left(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
    }
  }

  private[privyspark] def inferSchemaSignature(
    spark: SparkSession,
    format: String,
    filePath: String,
    readOptions: ScanReadOptions = ScanReadOptions()
  ): Either[String, String] = {
    try {
      val schemaSignature = withFileReadRetry(spark, Seq(filePath), "schema_detection") {
        val schema = readSchemaSource(spark, format, filePath, readOptions = readOptions).schema
        val normalizedFieldNames = schema.fieldNames.map(_.toLowerCase)
        if (format == "csv") {
          // CSV는 헤더 순서가 데이터 매핑에 직접 영향을 주므로 순서를 유지한다.
          normalizedFieldNames.mkString("|")
        } else {
          normalizedFieldNames.sorted.mkString("|")
        }
      }
      Right(schemaSignature)
    } catch {
      case NonFatal(e) =>
        Left(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
    }
  }

  private def resolveFileIdentifierColumn(columns: Seq[String]): String = {
    val normalized = columns.map(_.toLowerCase).toSet
    var candidate = FileIdentifierColumn
    var index = 1

    while (normalized.contains(candidate.toLowerCase)) {
      candidate = s"${FileIdentifierColumn}_$index"
      index += 1
    }

    candidate
  }

  private def newJsonCorruptRecordColumnName(): String = {
    s"${FileIdentifierColumn}_json_corrupt_${UUID.randomUUID().toString.replace("-", "")}"
  }

  private def ensureReadableSourceColumns(
    format: String,
    sourcePaths: Seq[String],
    df: DataFrame,
    internalCorruptRecordColumnName: Option[String] = None
  ): DataFrame = {
    val normalizedFormat = Option(format).map(_.toLowerCase).getOrElse("")
    if (normalizedFormat == "json") {
      val schemaFieldNames = df.schema.fieldNames.toSeq
      internalCorruptRecordColumnName match {
        case Some(columnName) if schemaFieldNames.size == 1 && schemaFieldNames.head == columnName =>
          val sourceDescription = sourcePaths match {
            case Seq(singlePath) => singlePath
            case paths => s"${paths.size} files (first: ${paths.head})"
          }
          throw new IllegalArgumentException(
            s"Malformed json input contains only corrupt records: $sourceDescription"
          )
        case Some(columnName) if schemaFieldNames.contains(columnName) =>
          df.drop(columnName)
        case _ =>
          df
      }
    } else {
      df
    }
  }

  private def readSchemaSource(
    spark: SparkSession,
    format: String,
    filePath: String,
    csvHasHeader: Boolean = true,
    readOptions: ScanReadOptions = ScanReadOptions()
  ): DataFrame = {
    logDebug("read_schema_source_start", "format" -> format, "file" -> filePath)
    val (df, internalCorruptRecordColumnName) = format match {
      case "csv" =>
        (
          spark.read
            .option("header", csvHasHeader.toString)
            .option("inferSchema", "false")
            .option("mode", "PERMISSIVE")
            .csv(filePath),
          None
        )
      case "json" =>
        val corruptRecordColumnName = newJsonCorruptRecordColumnName()
        (
          spark.read
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", corruptRecordColumnName)
            .json(filePath),
          Some(corruptRecordColumnName)
        )
      case AvroFormat =>
        (spark.read.format("avro").load(filePath), None)
      case XlsxFormat =>
        (
          spark.read
            .format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("inferSchema", "false")
            .option("dataAddress", workbookDataAddress(readOptions.sheetName.getOrElse {
              throw new IllegalArgumentException("Sheet name is required for xlsx sources")
            }))
            .load(filePath),
          None
        )
      case TextFormat =>
        (spark.read.text(filePath), None)
      case "parquet" =>
        (spark.read.parquet(filePath), None)
      case "orc" =>
        (spark.read.orc(filePath), None)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported format: $format")
    }
    ensureReadableSourceColumns(format, Seq(filePath), df, internalCorruptRecordColumnName)
  }

  private[privyspark] def scanGroup(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    fileParallelism: Int = -1,
    fileSampleRatio: Option[Double] = None,
    progressRun: Option[ProgressRun] = None
  ): (Seq[ScanResult], Seq[ScanError]) = {
    logDebug(
      "group_scan_start",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "files" -> group.filePaths.size,
      "sample_ratio" -> sampleRatio,
      "file_sample_ratio" -> fileSampleRatio.getOrElse("none"),
      "use_directory_identifier" -> group.useDirectoryIdentifier,
      "schema_sampled" -> group.schemaSampled,
      "csv_has_header" -> group.csvHasHeader
    )
    if (group.schemaSampled && group.filePaths.size > 1) {
      val exactSplitResult = rescanSampledGroupWithExactSplit(
        spark,
        datasetPath,
        group,
        rules,
        sampleRatio,
        timestamp,
        "sampled_exact_split",
        fileParallelism,
        fileSampleRatio,
        progressRun
      )
      logDebug(
        "group_scan_complete",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "schema" -> group.schemaSignature,
        "result_rows" -> exactSplitResult._1.size,
        "error_rows" -> exactSplitResult._2.size,
        "mode" -> "sampled_exact_split"
      )
      return exactSplitResult
    }

    if (!supportsBatchScan(group)) {
      val fallbackResult = scanGroupByFile(spark, datasetPath, group, rules, sampleRatio, timestamp, fileParallelism, progressRun)
      logDebug(
        "group_scan_complete",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "schema" -> group.schemaSignature,
        "result_rows" -> fallbackResult._1.size,
        "error_rows" -> fallbackResult._2.size,
        "mode" -> "direct_file_scan"
      )
      return fallbackResult
    }

    try {
      val results = scanGroupBatch(spark, datasetPath, group, rules, sampleRatio, timestamp, fileSampleRatio)
          progressRun.foreach { run =>
            persistProgressRecords(
              spark.sparkContext.hadoopConfiguration,
              run,
              "group",
              group.directoryPath,
              results,
              Seq.empty
            )
      }
      logDebug(
        "group_scan_complete",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "schema" -> group.schemaSignature,
        "result_rows" -> results.size,
        "error_rows" -> 0,
        "mode" -> "group_batch_scan"
      )
      (results, Seq.empty)
    } catch {
      case NonFatal(e) =>
        logWarn(
          "group_scan_fallback",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "files" -> group.filePaths.size,
          "reason" -> Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        )
        val errorMessage = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        logDebug(
          "group_scan_fallback_requested",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "files" -> group.filePaths.size,
          "reason" -> errorMessage
        )
        if (group.schemaSampled) {
          logWarn(
            "group_scan_fallback_execute",
            "directory" -> group.directoryPath,
            "format" -> group.format,
            "schema" -> group.schemaSignature,
            "files" -> group.filePaths.size,
            "mode" -> "schema_resplit"
          )
          val exactSplitResult = rescanSampledGroupWithExactSplit(
            spark,
            datasetPath,
            group,
            rules,
            sampleRatio,
            timestamp,
            "fallback_schema_resplit",
            fileParallelism,
            fileSampleRatio,
            progressRun
          )

          logDebug(
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
          val fallbackResult = scanGroupByFile(
            spark,
            datasetPath,
            group,
            rules,
            sampleRatio,
            timestamp,
            fileParallelism,
            progressRun
          )
          logDebug(
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

  private[privyspark] def scanGroupByFile(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    fileParallelism: Int = -1,
    progressRun: Option[ProgressRun] = None
  ): (Seq[ScanResult], Seq[ScanError]) = {
    logWarn(
      "group_scan_fallback_execute",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "files" -> group.filePaths.size,
      "mode" -> "file_scan"
    )
    val parallelism = if (fileParallelism > 0) {
      resolveParallelism(group.filePaths.size, fileParallelism)
    } else {
      resolveFileParallelism(spark, group.filePaths.size)
    }
    logDebug(
      "group_scan_fallback_execute",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "files" -> group.filePaths.size,
      "use_directory_identifier" -> group.useDirectoryIdentifier,
      "parallelism" -> parallelism
    )
    val successfulFileMetrics = ArrayBuffer.empty[FileScanMetrics]
    val fallbackErrors = ArrayBuffer.empty[ScanError]
    executeInParallel(parallelism, group.filePaths.map { sourceKey =>
      () => {
        val physicalPath = resolvePhysicalPath(group, sourceKey)
        val readOptions = resolveReadOptions(group, sourceKey)
        val logicalIdentifier = resolveLogicalIdentifier(group, datasetPath, sourceKey)
        logDebug("group_scan_fallback_file_start", "file" -> physicalPath, "directory" -> group.directoryPath)
        val csvHasHeaderOverride =
          if (group.format == "csv" && group.schemaSampled) None else Some(group.csvHasHeader)
        sourceKey -> scanFileMetrics(
          spark,
          datasetPath,
          sourceKey,
          rules,
          sampleRatio,
          timestamp,
          csvHasHeaderOverride,
          formatOverride = Some(group.format),
          logicalIdentifierOverride = Some(logicalIdentifier),
          physicalPathOverride = Some(physicalPath),
          readOptions = readOptions
        )
          .fold(
            error => {
              if (!group.useDirectoryIdentifier) {
                progressRun.foreach { run =>
        persistProgressRecords(
          spark.sparkContext.hadoopConfiguration,
          run,
          "file",
          error.file_identifier,
          Seq.empty,
          Seq(error)
        )
                }
              }
              Left(error)
            },
            fileMetrics => {
              if (!group.useDirectoryIdentifier) {
                val fileResults = buildScanResults(
                  datasetPath,
                  timestamp,
                  fileMetrics.fileIdentifier,
                  fileMetrics.sampledRowCount,
                  fileMetrics.matchCounts
                )
                progressRun.foreach { run =>
                  persistProgressRecords(
                    spark.sparkContext.hadoopConfiguration,
                    run,
                    "file",
                    fileMetrics.fileIdentifier,
                    fileResults,
                    Seq.empty
                  )
                }
              }
              Right(fileMetrics)
            }
          )
      }
    }).foreach {
      case (sourceKey, fileResult) =>
        val physicalPath = resolvePhysicalPath(group, sourceKey)
        fileResult match {
        case Right(fileMetrics) =>
          successfulFileMetrics += fileMetrics
          logDebug(
            "group_scan_fallback_file_success",
            "file" -> physicalPath,
            "file_identifier" -> fileMetrics.fileIdentifier,
            "sampled_rows" -> fileMetrics.sampledRowCount,
            "matches" -> fileMetrics.matchCounts.size
          )
        case Left(error) =>
          fallbackErrors += error
          logDebug(
            "group_scan_fallback_file_error",
            "file" -> physicalPath,
            "file_identifier" -> error.file_identifier,
            "reason" -> error.error_message
          )
        }
    }

    val fallbackResults = if (group.useDirectoryIdentifier && fallbackErrors.isEmpty) {
      val sampledRowCount = successfulFileMetrics.map(_.sampledRowCount).sum
      val aggregatedMatchCounts = successfulFileMetrics
        .flatMap(_.matchCounts)
        .groupBy(matchCount => (matchCount.columnName, matchCount.piiType))
        .toSeq
        .sortBy { case ((columnName, piiType), _) => (columnName, piiType) }
        .map {
          case ((columnName, piiType), matchCounts) =>
            MatchCount(columnName, piiType, matchCounts.map(_.count).sum)
        }

      buildScanResults(
        datasetPath,
        timestamp,
        resolveDirectoryIdentifier(datasetPath, group.directoryPath),
        sampledRowCount,
        aggregatedMatchCounts
      )
    } else {
      if (group.useDirectoryIdentifier && fallbackErrors.nonEmpty) {
        logWarn(
          "group_scan_partial_results",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "failed_files" -> fallbackErrors.size,
          "mode" -> "file_identifier_preserved"
        )
        logDebug(
          "group_scan_partial_results",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "failed_files" -> fallbackErrors.size
        )
      }
      successfulFileMetrics.flatMap { fileMetrics =>
        buildScanResults(
          datasetPath,
          timestamp,
          fileMetrics.fileIdentifier,
          fileMetrics.sampledRowCount,
          fileMetrics.matchCounts
        )
      }
    }
    progressRun.foreach { run =>
      if (group.useDirectoryIdentifier) {
        persistProgressRecords(
          spark.sparkContext.hadoopConfiguration,
          run,
          "group",
          group.directoryPath,
          fallbackResults,
          fallbackErrors.toSeq
        )
      }
    }

    logDebug(
      "group_scan_fallback_complete",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "successful_files" -> successfulFileMetrics.size,
      "failed_files" -> fallbackErrors.size,
      "result_rows" -> fallbackResults.size
    )
    (fallbackResults.toSeq, fallbackErrors.toSeq)
  }

  private[privyspark] def scanGroupBatch(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    fileSampleRatio: Option[Double] = None
  ): Seq[ScanResult] = {
    logDebug(
      "group_scan_batch_start",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "files" -> group.filePaths.size,
      "sample_ratio" -> sampleRatio,
      "file_sample_ratio" -> fileSampleRatio.getOrElse("none"),
      "use_directory_identifier" -> group.useDirectoryIdentifier
    )
    val selectedSourceKeys = fileSampleRatio match {
      case Some(ratio) =>
        val sampledKeys = selectSampledFileKeys(group.filePaths, ratio)
        if (sampleRatio < 1.0) {
          logWarn(
            "group_scan_row_sampling_ignored",
            "directory" -> group.directoryPath,
            "format" -> group.format,
            "schema" -> group.schemaSignature,
            "sample_ratio" -> sampleRatio,
            "file_sample_ratio" -> ratio,
            "selected_files" -> sampledKeys.size,
            "total_files" -> group.filePaths.size
          )
        }
        logDebug(
          "group_scan_file_sampling_applied",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "schema" -> group.schemaSignature,
          "file_sample_ratio" -> ratio,
          "selected_files" -> sampledKeys.size,
          "total_files" -> group.filePaths.size
        )
        sampledKeys
      case None => group.filePaths
    }
    val physicalPaths = selectedSourceKeys.map(sourceKey => resolvePhysicalPath(group, sourceKey))
    withFileReadRetry(spark, physicalPaths, "group_batch_scan") {
      val effectiveRules = effectiveRulesForFormat(group.format, rules)
      val baseDf = readSource(spark, group.format, physicalPaths, group.csvHasHeader)
      val fileIdentifierColumn = if (group.useDirectoryIdentifier) {
        None
      } else {
        Some(resolveFileIdentifierColumn(baseDf.columns.toSeq))
      }
      val sourceDf = fileIdentifierColumn match {
        case Some(columnName) => baseDf.withColumn(columnName, input_file_name())
        case None => baseDf
      }
      logDebug(
        "group_scan_batch_source_ready",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "columns" -> sourceDf.columns.length,
        "file_identifier_mode" -> fileIdentifierColumn.fold("directory")(identity)
      )

      val sampledDf = if (fileSampleRatio.nonEmpty || sampleRatio >= 1.0) {
        sourceDf
      } else {
        sourceDf.sample(withReplacement = false, sampleRatio)
      }

      fileIdentifierColumn match {
        case None =>
          val sampledRowCount = sampledDf.count()
          logDebug(
            "group_scan_batch_sampled_rows",
            "directory" -> group.directoryPath,
            "sampled_rows" -> sampledRowCount,
            "mode" -> "directory_identifier"
          )
          if (sampledRowCount == 0L) {
            logDebug(
              "group_scan_batch_complete",
              "directory" -> group.directoryPath,
              "result_rows" -> 0,
              "mode" -> "directory_identifier"
            )
            Seq.empty
          } else {
            val results = buildScanResults(
              datasetPath,
              timestamp,
              resolveDirectoryIdentifier(datasetPath, group.directoryPath),
              sampledRowCount,
              DetectionAggregator.aggregate(sampledDf, effectiveRules)
            )
            logDebug(
              "group_scan_batch_complete",
              "directory" -> group.directoryPath,
              "result_rows" -> results.size,
              "mode" -> "directory_identifier"
            )
            results
          }
        case Some(columnName) =>
          val sampledRowsByFile = sampledDf
            .groupBy(col(columnName))
            .count()
            .collect()
            .flatMap { row =>
              val fileIdentifier = if (row.isNullAt(0)) null else row.getString(0)
              val rowCount = if (row.isNullAt(1)) 0L else row.getLong(1)
              if (fileIdentifier == null || fileIdentifier.isEmpty || rowCount <= 0L) {
                None
              } else {
                Some(fileIdentifier -> rowCount)
              }
            }
            .toMap
          logDebug(
            "group_scan_batch_sampled_file_rows",
            "directory" -> group.directoryPath,
            "files_with_rows" -> sampledRowsByFile.size,
            "mode" -> "file_identifier"
          )

          if (sampledRowsByFile.isEmpty) {
            logDebug(
              "group_scan_batch_complete",
              "directory" -> group.directoryPath,
              "result_rows" -> 0,
              "mode" -> "file_identifier"
            )
            Seq.empty
          } else {
            val results = DetectionAggregator.aggregateByFile(sampledDf, columnName, effectiveRules).flatMap { matchCount =>
              sampledRowsByFile.get(matchCount.fileIdentifier).flatMap { sampledRowCount =>
                buildScanResults(
                  datasetPath,
                  timestamp,
                  resolveLogicalIdentifierForPhysicalPath(group, datasetPath, matchCount.fileIdentifier),
                  sampledRowCount,
                  Seq(MatchCount(matchCount.columnName, matchCount.piiType, matchCount.count))
                ).headOption
              }
            }
            logDebug(
              "group_scan_batch_complete",
              "directory" -> group.directoryPath,
              "result_rows" -> results.size,
              "mode" -> "file_identifier"
            )
            results
          }
      }
    }
  }

  private def scanFileMetrics(
    spark: SparkSession,
    datasetPath: String,
    filePath: String,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    csvHasHeaderOverride: Option[Boolean] = None,
    formatOverride: Option[String] = None,
    logicalIdentifierOverride: Option[String] = None,
    physicalPathOverride: Option[String] = None,
    readOptions: ScanReadOptions = ScanReadOptions()
  ): Either[ScanError, FileScanMetrics] = {
    val physicalPath = physicalPathOverride.getOrElse(filePath)
    val fileIdentifier = logicalIdentifierOverride.getOrElse(resolveRelativeIdentifier(datasetPath, physicalPath))
    logDebug("scan_file_start", "file" -> physicalPath, "file_identifier" -> fileIdentifier, "sample_ratio" -> sampleRatio)

    try {
      withFileReadRetry(spark, Seq(physicalPath), "file_scan") {
        val format = formatOverride.orElse(detectPhysicalFormat(spark.sparkContext.hadoopConfiguration, physicalPath)).getOrElse {
          logDebug("scan_file_error", "file" -> physicalPath, "file_identifier" -> fileIdentifier, "reason" -> "Unsupported file format")
          return Left(ScanError(datasetPath, timestamp, fileIdentifier, s"Unsupported file format: $fileIdentifier"))
        }
        val effectiveRules = effectiveRulesForFormat(format, rules)

        val csvHasHeader = if (format == "csv") {
          csvHasHeaderOverride.getOrElse(detectCsvHasHeader(spark, physicalPath))
        } else {
          true
        }
        val sourceDf = readSource(spark, format, Seq(physicalPath), csvHasHeader, readOptions)
        val sampledDf = if (sampleRatio >= 1.0) sourceDf else sourceDf.sample(withReplacement = false, sampleRatio)

        val sampledRowCount = sampledDf.count()
        logDebug(
          "scan_file_sampled_rows",
          "file" -> physicalPath,
          "file_identifier" -> fileIdentifier,
          "sampled_rows" -> sampledRowCount
        )

        if (sampledRowCount == 0L) {
          logDebug("scan_file_complete", "file" -> physicalPath, "file_identifier" -> fileIdentifier, "matches" -> 0)
          Right(FileScanMetrics(fileIdentifier, sampledRowCount, Seq.empty))
        } else {
          val matchCounts = DetectionAggregator.aggregate(sampledDf, effectiveRules)
          logDebug(
            "scan_file_complete",
            "file" -> physicalPath,
            "file_identifier" -> fileIdentifier,
            "matches" -> matchCounts.size
          )
          Right(FileScanMetrics(fileIdentifier, sampledRowCount, matchCounts))
        }
      }
    } catch {
      case NonFatal(e) =>
        val errorMessage = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        logDebug("scan_file_error", "file" -> physicalPath, "file_identifier" -> fileIdentifier, "reason" -> errorMessage)
        Left(ScanError(datasetPath, timestamp, fileIdentifier, errorMessage))
    }
  }

  private def scanFile(
    spark: SparkSession,
    datasetPath: String,
    filePath: String,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String
  ): Either[ScanError, Seq[ScanResult]] = {
    scanFileMetrics(spark, datasetPath, filePath, rules, sampleRatio, timestamp).map { fileMetrics =>
      buildScanResults(
        datasetPath,
        timestamp,
        fileMetrics.fileIdentifier,
        fileMetrics.sampledRowCount,
        fileMetrics.matchCounts
      )
    }
  }

  private def readSource(
    spark: SparkSession,
    format: String,
    filePaths: Seq[String],
    csvHasHeader: Boolean = true,
    readOptions: ScanReadOptions = ScanReadOptions()
  ): DataFrame = {
    require(filePaths.nonEmpty, "filePaths must not be empty")
    logDebug("read_source_start", "format" -> format, "files" -> filePaths.size, "first_file" -> filePaths.head)

    val (df, internalCorruptRecordColumnName) = format match {
      case "csv" =>
        (
          spark.read
            .option("header", csvHasHeader.toString)
            .option("inferSchema", "false")
            .option("mode", "PERMISSIVE")
            .csv(filePaths: _*),
          None
        )
      case "json" =>
        val corruptRecordColumnName = newJsonCorruptRecordColumnName()
        (
          spark.read
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", corruptRecordColumnName)
            .json(filePaths: _*),
          Some(corruptRecordColumnName)
        )
      case AvroFormat =>
        (spark.read.format("avro").load(filePaths: _*), None)
      case XlsxFormat =>
        require(filePaths.size == 1, "xlsx sources must be read one sheet at a time")
        (
          spark.read
            .format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("inferSchema", "false")
            .option("dataAddress", workbookDataAddress(readOptions.sheetName.getOrElse {
              throw new IllegalArgumentException("Sheet name is required for xlsx sources")
            }))
            .load(filePaths.head),
          None
        )
      case TextFormat =>
        (spark.read.text(filePaths: _*), None)
      case "parquet" =>
        (spark.read.parquet(filePaths: _*), None)
      case "orc" =>
        (spark.read.orc(filePaths: _*), None)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported format: $format")
    }
    ensureReadableSourceColumns(format, filePaths, df, internalCorruptRecordColumnName)
  }

  private def createCsvOptions(spark: SparkSession): CSVOptions = {
    new CSVOptions(
      scala.collection.immutable.Map("header" -> "true", "inferSchema" -> "false"),
      false,
      spark.sessionState.conf.sessionLocalTimeZone,
      spark.sessionState.conf.columnNameOfCorruptRecord
    )
  }

  private def parseCsvLine(spark: SparkSession, line: String): Array[String] = {
    val parser = new CsvParser(createCsvOptions(spark).asParserSettings)
    Option(parser.parseLine(Option(line).getOrElse("").stripPrefix("\uFEFF"))).getOrElse(Array.empty[String])
  }

  private def readFirstNonBlankCsvLines(
    spark: SparkSession,
    filePath: String,
    maxLines: Int
  ): Seq[String] = {
    withFileReadRetry(spark, Seq(filePath), "csv_line_sample") {
      val path = new Path(filePath)
      val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
      val reader = new BufferedReader(new InputStreamReader(fs.open(path), StandardCharsets.UTF_8))
      try {
        val lines = ArrayBuffer.empty[String]
        var line: String = reader.readLine()
        while (line != null && lines.size < maxLines) {
          if (line.trim.nonEmpty) {
            lines += line
          }
          line = reader.readLine()
        }
        lines.toSeq
      } finally {
        reader.close()
      }
    }
  }

  private def isNumericLikeField(value: String): Boolean = {
    val trimmed = Option(value).getOrElse("").trim
    trimmed.nonEmpty && trimmed.matches("[-+]?\\d+(\\.\\d+)?")
  }

  private def classifyCsvField(value: String): String = {
    val trimmed = Option(value).getOrElse("").trim
    if (trimmed.isEmpty) {
      "empty"
    } else if (trimmed.matches("[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}")) {
      "email"
    } else if (trimmed.matches("\\d{2,3}-\\d{3,4}-\\d{4}")) {
      "phone"
    } else if (isNumericLikeField(trimmed)) {
      "numeric"
    } else if (trimmed.exists(_.isDigit)) {
      "mixed"
    } else {
      "plain_text"
    }
  }

  private def isStructuredCsvFieldKind(kind: String): Boolean = {
    kind match {
      case "email" | "phone" | "numeric" | "mixed" => true
      case _ => false
    }
  }

  private def isStructuredCsvFieldForHeaderHeuristic(value: String, kind: String): Boolean = {
    if (kind == "mixed" && looksLikeCsvHeaderField(value)) {
      false
    } else {
      isStructuredCsvFieldKind(kind)
    }
  }

  private def tokenizeCsvHeaderField(value: String): Seq[String] = {
    Option(value).getOrElse("").trim.toLowerCase
      .split("[\\s_./-]+")
      .filter(_.nonEmpty)
      .flatMap { token =>
        val normalizedToken = token.replaceAll("\\d+$", "")
        if (normalizedToken.nonEmpty && normalizedToken != token) Seq(token, normalizedToken) else Seq(token)
      }
      .toSeq
  }

  private def looksLikeCsvHeaderField(value: String): Boolean = {
    val trimmed = Option(value).getOrElse("").trim
    trimmed.nonEmpty &&
      !trimmed.contains("@") &&
      isCsvHeaderFieldShape(trimmed)
  }

  private[privyspark] def selectSampledFileKeys(fileKeys: Seq[String], fileSampleRatio: Double): Seq[String] = {
    require(fileKeys.nonEmpty, "fileKeys must not be empty")
    require(fileSampleRatio > 0.0 && fileSampleRatio <= 1.0, "fileSampleRatio must be > 0.0 and <= 1.0")

    val sampleSize = math.max(1, math.min(fileKeys.size, math.ceil(fileKeys.size * fileSampleRatio).toInt))
    val selectedKeySet = Random.shuffle(fileKeys.indices.toVector).take(sampleSize).map(fileKeys).toSet
    fileKeys.filter(selectedKeySet.contains)
  }

  private def hasStrongCsvHeaderSignal(value: String): Boolean = {
    val trimmed = Option(value).getOrElse("").trim
    val tokens = tokenizeCsvHeaderField(trimmed)
    tokens.exists(CommonCsvHeaderTokens.contains) ||
      trimmed.exists(ch => ch == '_' || ch == '-' || ch == ' ')
  }

  private def scoreCsvHeaderField(value: String): Int = {
    val trimmed = Option(value).getOrElse("").trim
    val fieldKind = classifyCsvField(trimmed)
    if (trimmed.isEmpty) {
      -2
    } else if (fieldKind != "plain_text" && !(fieldKind == "mixed" && looksLikeCsvHeaderField(trimmed))) {
      -2
    } else {
      val tokens = tokenizeCsvHeaderField(trimmed)
      val commonTokenScore = tokens.count(CommonCsvHeaderTokens.contains) * 2
      val separatorScore = if (trimmed.exists(ch => ch == '_' || ch == '-' || ch == ' ')) 1 else 0
      val lowercaseWordScore =
        if (trimmed.nonEmpty && trimmed.forall(isCsvHeaderLowercaseLikeChar)) 1
        else 0
      val alphaOnlyScore = if (isCsvHeaderFieldShape(trimmed)) 1 else 0
      commonTokenScore + separatorScore + lowercaseWordScore + alphaOnlyScore
    }
  }

  private def scoreCsvHeaderRow(fields: Seq[String]): Int = {
    fields.map(scoreCsvHeaderField).sum
  }

  private def looksLikeCsvHeaderRow(fields: Seq[String]): Boolean = {
    fields.nonEmpty &&
      fields.forall(looksLikeCsvHeaderField)
  }

  private def isCsvHeaderFieldShape(value: String): Boolean = {
    value.nonEmpty &&
      Character.isLetter(value.charAt(0)) &&
      value.forall(isCsvHeaderFieldChar)
  }

  private def isCsvHeaderFieldChar(ch: Char): Boolean = {
    Character.isLetterOrDigit(ch) || ch == '_' || ch == ' ' || ch == '.' || ch == '/' || ch == '-'
  }

  private def isCsvHeaderLowercaseLikeChar(ch: Char): Boolean = {
    ch.isWhitespace || ch == '_' || ch == '-' || ch == '.' || ch == '/' || !Character.isUpperCase(ch)
  }

  private def rescanSampledGroupWithExactSplit(
    spark: SparkSession,
    datasetPath: String,
    group: ScanGroup,
    rules: Seq[PiiRule],
    sampleRatio: Double,
    timestamp: String,
    mode: String,
    fileParallelism: Int,
    fileSampleRatio: Option[Double],
    progressRun: Option[ProgressRun]
  ): (Seq[ScanResult], Seq[ScanError]) = {
    val (splitGroups, splitErrors) = splitGroupBySchema(
      spark,
      datasetPath,
      timestamp,
      group.copy(schemaSampled = false)
    )
    val exactSplitCanUseDirectoryIdentifier =
      group.directoryIdentifierEligible &&
        splitGroups.size == 1 &&
        splitErrors.isEmpty &&
        splitGroups.head.filePaths.size > 1
    val rescannedGroups = splitGroups.map(_.copy(
      useDirectoryIdentifier = exactSplitCanUseDirectoryIdentifier,
      directoryIdentifierEligible = group.directoryIdentifierEligible,
      schemaSampled = false
    ))

    val rescannedResults = ArrayBuffer.empty[ScanResult]
    val rescannedErrors = ArrayBuffer.empty[ScanError] ++ splitErrors
    if (splitErrors.nonEmpty) {
      progressRun.foreach { run =>
        persistProgressRecords(
          spark.sparkContext.hadoopConfiguration,
          run,
          "schema-split",
          group.directoryPath,
          Seq.empty,
          splitErrors
        )
      }
    }
    rescannedGroups.foreach { rescannedGroup =>
      val (groupResults, groupErrors) = scanGroup(
        spark,
        datasetPath,
        rescannedGroup,
        rules,
        sampleRatio,
        timestamp,
        fileParallelism,
        fileSampleRatio,
        progressRun
      )
      rescannedResults ++= groupResults
      rescannedErrors ++= groupErrors
    }

    logDebug(
      "group_scan_exact_split_complete",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema" -> group.schemaSignature,
      "split_groups" -> splitGroups.size,
      "split_errors" -> splitErrors.size,
      "use_directory_identifier" -> exactSplitCanUseDirectoryIdentifier,
      "mode" -> mode
    )
    (rescannedResults.toSeq, rescannedErrors.toSeq)
  }

  private[privyspark] def writeReports(
    spark: SparkSession,
    outputRoot: String,
    results: Seq[ScanResult],
    errors: Seq[ScanError]
  ): Unit = {
    import spark.implicits._
    writeReports(
      spark,
      outputRoot,
      spark.createDataset(results).toDF(),
      spark.createDataset(errors).toDF()
    )
  }

  private def writeReports(
    spark: SparkSession,
    outputRoot: String,
    resultsDf: DataFrame,
    errorsDf: DataFrame
  ): Unit = {
    val root = outputRoot.stripSuffix("/")
    logDebug("write_reports_materialize", "output_root" -> root)
    val resultDf = resultsDf.coalesce(1)
    val errorDf = errorsDf.coalesce(1)

    val resultParquetPath = s"$root/parquet/scan_results"
    val errorParquetPath = s"$root/parquet/scan_errors"
    val resultCsvPath = s"$root/csv/scan_results"
    val errorCsvPath = s"$root/csv/scan_errors"

    resultDf.write.mode("overwrite").parquet(resultParquetPath)
    errorDf.write.mode("overwrite").parquet(errorParquetPath)

    resultDf.write
      .option("header", "true")
      .mode("overwrite")
      .csv(resultCsvPath)

    errorDf.write
      .option("header", "true")
      .mode("overwrite")
      .csv(errorCsvPath)

    logDebug(
      "write_reports_complete",
      "output_root" -> root,
      "result_parquet_path" -> resultParquetPath,
      "error_parquet_path" -> errorParquetPath,
      "result_csv_path" -> resultCsvPath,
      "error_csv_path" -> errorCsvPath
    )
  }

  private[privyspark] def prepareProgressRun(
    conf: org.apache.hadoop.conf.Configuration,
    outputRoot: String,
    datasetPath: String,
    timestamp: String
  ): ProgressRun = {
    val rootPath = s"${outputRoot.stripSuffix("/")}/$ProgressDirectoryName"
    val root = new Path(rootPath)
    val fs = root.getFileSystem(conf)
    val activeRunPath = s"$rootPath/active-run.json"
    val preparingRunPath = s"${outputRoot.stripSuffix("/")}/${ProgressDirectoryName}-preparing.json"
    cleanupProgressRoot(conf, rootPath, activeRunPath, preparingRunPath)

    val runId = s"${timestamp.replaceAll("[:.]", "-")}-${UUID.randomUUID().toString}"
    val runPath = s"$rootPath/$runId"
    val resultsPath = s"$runPath/results"
    val errorsPath = s"$runPath/errors"
    val metaPath = s"$runPath/meta"
    val completionsPath = s"$metaPath/completions"
    val progressRun = ProgressRun(
      runId,
      rootPath,
      runPath,
      activeRunPath,
      datasetPath,
      outputRoot,
      timestamp,
      resultsPath,
      errorsPath,
      metaPath,
      completionsPath
    )

    try {
      writePreparingRunMarker(conf, progressRun, preparingRunPath, overwrite = false)
      fs.mkdirs(root)
      writeActiveRunMarker(conf, progressRun, state = "RUNNING", overwrite = false)
      Seq(runPath, resultsPath, errorsPath, metaPath, completionsPath).foreach(path => fs.mkdirs(new Path(path)))
      writeJsonFile(
        conf,
        s"$metaPath/run.json",
        progressRunMetadataJson(progressRun, state = "RUNNING", errorMessage = None)
      )
      deleteOwnedPreparingRunMarker(conf, preparingRunPath, progressRun.runId)

      logDebug(
        "progress_run_prepared",
        "run_id" -> progressRun.runId,
        "root_path" -> progressRun.rootPath,
        "run_path" -> progressRun.runPath
      )
      progressRun
    } catch {
      case _: org.apache.hadoop.fs.FileAlreadyExistsException =>
        deleteOwnedPreparingRunMarker(conf, preparingRunPath, progressRun.runId)
        throw new IllegalStateException(s"Active progress run already exists under output root: $rootPath")
      case NonFatal(e) =>
        deleteOwnedPreparingRunMarker(conf, preparingRunPath, progressRun.runId)
        deleteOwnedActiveRunMarker(conf, progressRun)
        fs.delete(new Path(runPath), true)
        deleteEmptyProgressRoot(fs, root)
        throw e
    }
  }

  private[privyspark] def mergeProgressReports(
    spark: SparkSession,
    outputRoot: String,
    progressRun: ProgressRun
  ): (Long, Long) = {
    logDebug(
      "progress_merge_start",
      "run_id" -> progressRun.runId,
      "results_path" -> progressRun.resultsPath,
      "errors_path" -> progressRun.errorsPath
    )
    val resultDf = readProgressRecords(spark, progressRun.resultsPath, Encoders.product[ScanResult].schema)
    val errorDf = readProgressRecords(spark, progressRun.errorsPath, Encoders.product[ScanError].schema)
    val resultCount = resultDf.count()
    val errorCount = errorDf.count()
    writeReports(spark, outputRoot, resultDf, errorDf)
    deleteProgressRun(spark.sparkContext.hadoopConfiguration, progressRun)
    logDebug(
      "progress_merge_complete",
      "run_id" -> progressRun.runId,
      "results" -> resultCount,
      "errors" -> errorCount
    )
    (resultCount, errorCount)
  }

  private def persistProgressRecords(
    conf: org.apache.hadoop.conf.Configuration,
    progressRun: ProgressRun,
    scope: String,
    identifier: String,
    results: Seq[ScanResult],
    errors: Seq[ScanError]
  ): Unit = {
    if (results.nonEmpty) {
      writeProgressLines(conf, progressRun.resultsPath, scope, results.map(scanResultToJson))
    }
    if (errors.nonEmpty) {
      writeProgressLines(conf, progressRun.errorsPath, scope, errors.map(scanErrorToJson))
    }
    writeProgressLines(
      conf,
      progressRun.completionsPath,
      scope,
      Seq(progressCompletionToJson(scope, identifier, results.size, errors.size))
    )
    updateActiveRunHeartbeat(conf, progressRun)
    logDebug(
      "progress_write_complete",
      "run_id" -> progressRun.runId,
      "scope" -> scope,
      "identifier" -> identifier,
      "results" -> results.size,
      "errors" -> errors.size
    )
  }

  private def readProgressRecords(
    spark: SparkSession,
    directoryPath: String,
    schema: StructType
  ): DataFrame = {
    val conf = spark.sparkContext.hadoopConfiguration
    val directory = new Path(directoryPath)
    val fs = directory.getFileSystem(conf)
    val jsonPattern = new Path(s"${directoryPath.stripSuffix("/")}/*.jsonl")
    val files = Option(fs.globStatus(jsonPattern)).getOrElse(Array.empty)
    if (files.isEmpty) {
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    } else {
      spark.read.schema(schema).json(jsonPattern.toString)
    }
  }

  private def writeProgressLines(
    conf: org.apache.hadoop.conf.Configuration,
    directoryPath: String,
    scope: String,
    lines: Seq[String]
  ): Unit = {
    if (lines.isEmpty) {
      return
    }

    val filePath = new Path(s"${directoryPath.stripSuffix("/")}/$scope-${UUID.randomUUID().toString}.jsonl")
    val fs = filePath.getFileSystem(conf)
    val writer = new BufferedWriter(new OutputStreamWriter(fs.create(filePath, false), StandardCharsets.UTF_8))
    try {
      lines.foreach { line =>
        writer.write(line)
        writer.newLine()
      }
    } finally {
      writer.close()
    }
  }

  private def writeJsonFile(
    conf: org.apache.hadoop.conf.Configuration,
    filePath: String,
    line: String,
    overwrite: Boolean = true
  ): Unit = {
    val path = new Path(filePath)
    val fs = path.getFileSystem(conf)
    val writer = new BufferedWriter(new OutputStreamWriter(fs.create(path, overwrite), StandardCharsets.UTF_8))
    try {
      writer.write(line)
      writer.newLine()
    } finally {
      writer.close()
    }
  }

  private def deleteProgressRun(conf: org.apache.hadoop.conf.Configuration, progressRun: ProgressRun): Unit = {
    val runPath = new Path(progressRun.runPath)
    val fs = runPath.getFileSystem(conf)
    if (fs.exists(runPath)) {
      fs.delete(runPath, true)
    }

    deleteOwnedActiveRunMarker(conf, progressRun)

    val rootPath = new Path(progressRun.rootPath)
    deleteEmptyProgressRoot(fs, rootPath)
  }

  private def cleanupProgressRoot(
    conf: org.apache.hadoop.conf.Configuration,
    rootPath: String,
    activeRunPath: String,
    preparingRunPath: String
  ): Unit = {
    val root = new Path(rootPath)
    val fs = root.getFileSystem(conf)
    val preparingMarkerPath = new Path(preparingRunPath)
    if (fs.exists(preparingMarkerPath)) {
      val preparingModifiedAt = fs.getFileStatus(preparingMarkerPath).getModificationTime
      if (System.currentTimeMillis() - preparingModifiedAt > PreparingRunStaleThresholdMillis) {
        logWarn("progress_cleanup_stale", "path" -> rootPath, "reason" -> "stale_preparing_run_marker")
        fs.delete(preparingMarkerPath, false)
      } else {
        throw new IllegalStateException(s"Progress root is being prepared under output root: $rootPath")
      }
    }

    if (!fs.exists(root)) {
      return
    }

    val activeMarkerPath = new Path(activeRunPath)
    if (!fs.exists(activeMarkerPath)) {
      if (!progressRootHasRunMetadata(fs, root)) {
        logWarn("progress_cleanup_stale", "path" -> rootPath, "reason" -> "missing_active_run_marker_without_run_metadata")
        fs.delete(root, true)
      } else {
        val rootModifiedAt = fs.getFileStatus(root).getModificationTime
        if (System.currentTimeMillis() - rootModifiedAt > ActiveRunStaleThresholdMillis) {
          logWarn("progress_cleanup_stale", "path" -> rootPath, "reason" -> "missing_active_run_marker")
          fs.delete(root, true)
        } else {
          throw new IllegalStateException(s"Progress root is being prepared under output root: $rootPath")
        }
      }
      return
    }

    readActiveRunMarker(conf, activeRunPath) match {
      case Some(marker) if marker.state == "FAILED" || isStaleActiveRun(marker) =>
        logWarn(
          "progress_cleanup_stale",
          "path" -> rootPath,
          "run_id" -> marker.runId,
          "state" -> marker.state,
          "last_heartbeat_epoch_ms" -> marker.lastHeartbeatEpochMillis
        )
        fs.delete(root, true)
      case Some(marker) =>
        throw new IllegalStateException(s"Active progress run already exists under output root: $rootPath (run_id=${marker.runId})")
      case None =>
        if (progressRootHasFailedRunMetadata(conf, root)) {
          logWarn("progress_cleanup_stale", "path" -> rootPath, "reason" -> "failed_run_metadata_with_unreadable_active_run_marker")
          fs.delete(root, true)
        } else {
          val markerModifiedAt = fs.getFileStatus(activeMarkerPath).getModificationTime
          if (System.currentTimeMillis() - markerModifiedAt > ActiveRunStaleThresholdMillis) {
            logWarn("progress_cleanup_stale", "path" -> rootPath, "reason" -> "stale_unreadable_active_run_marker")
            fs.delete(root, true)
          } else {
            throw new IllegalStateException(s"Active progress marker is unreadable under output root: $rootPath")
          }
        }
    }
  }

  private def markProgressRunFailed(
    conf: org.apache.hadoop.conf.Configuration,
    progressRun: ProgressRun,
    errorMessage: String
  ): Unit = {
    writeJsonFile(
      conf,
      s"${progressRun.metaPath}/run.json",
      progressRunMetadataJson(progressRun, state = "FAILED", errorMessage = Some(errorMessage))
    )
    updateActiveRunMarker(conf, progressRun, state = "FAILED", errorMessage = Some(errorMessage))
  }

  private[privyspark] def updateActiveRunHeartbeat(
    conf: org.apache.hadoop.conf.Configuration,
    progressRun: ProgressRun
  ): Unit = updateActiveRunMarker(conf, progressRun, state = "RUNNING", errorMessage = None)

  private def startProgressHeartbeat(
    conf: org.apache.hadoop.conf.Configuration,
    progressRun: ProgressRun
  ): ScheduledExecutorService = {
    val executor = Executors.newSingleThreadScheduledExecutor()
    executor.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = {
          try {
            updateActiveRunHeartbeat(conf, progressRun)
          } catch {
            case NonFatal(_) =>
          }
        }
      },
      ActiveRunHeartbeatIntervalMillis,
      ActiveRunHeartbeatIntervalMillis,
      TimeUnit.MILLISECONDS
    )
    executor
  }

  private def stopProgressHeartbeat(executor: ScheduledExecutorService): Unit = {
    executor.shutdownNow()
    executor.awaitTermination(5L, TimeUnit.SECONDS)
  }

  private def updateActiveRunMarker(
    conf: org.apache.hadoop.conf.Configuration,
    progressRun: ProgressRun,
    state: String,
    errorMessage: Option[String]
  ): Unit = {
    ActiveRunMarkerLock.synchronized {
      val runMetadata = readProgressRunMetadata(conf, progressRun)
      val failedRunMetadata = runMetadata.exists(metadata => metadata.runId == progressRun.runId && metadata.state == "FAILED")
      readActiveRunMarker(conf, progressRun.activeRunPath) match {
        case Some(marker) if marker.runId == progressRun.runId && marker.state == "FAILED" && state == "RUNNING" =>
        case Some(marker) if marker.runId == progressRun.runId && failedRunMetadata && state == "RUNNING" =>
        case Some(marker) if marker.runId == progressRun.runId =>
          writeActiveRunMarker(conf, progressRun, state, overwrite = true, errorMessage)
        case None if failedRunMetadata && state == "RUNNING" =>
        case None if runMetadata.exists(_.runId == progressRun.runId) =>
          logWarn(
            "progress_active_run_marker_self_healed",
            "run_id" -> progressRun.runId,
            "path" -> progressRun.activeRunPath,
            "state" -> state
          )
          writeActiveRunMarker(conf, progressRun, state, overwrite = true, errorMessage)
        case _ =>
      }
    }
  }

  private def writeActiveRunMarker(
    conf: org.apache.hadoop.conf.Configuration,
    progressRun: ProgressRun,
    state: String,
    overwrite: Boolean,
    errorMessage: Option[String] = None
  ): Unit = {
    writeJsonFile(
      conf,
      progressRun.activeRunPath,
      activeRunMetadataJson(progressRun, state, System.currentTimeMillis(), errorMessage),
      overwrite = overwrite
    )
  }

  private def writePreparingRunMarker(
    conf: org.apache.hadoop.conf.Configuration,
    progressRun: ProgressRun,
    preparingRunPath: String,
    overwrite: Boolean
  ): Unit = {
    writeJsonFile(
      conf,
      preparingRunPath,
      activeRunMetadataJson(progressRun, state = "PREPARING", System.currentTimeMillis(), errorMessage = None),
      overwrite = overwrite
    )
  }

  private def deleteOwnedActiveRunMarker(
    conf: org.apache.hadoop.conf.Configuration,
    progressRun: ProgressRun
  ): Unit = deleteOwnedRunMarker(conf, progressRun.activeRunPath, progressRun.runId)

  private def deleteOwnedPreparingRunMarker(
    conf: org.apache.hadoop.conf.Configuration,
    preparingRunPath: String,
    runId: String
  ): Unit = deleteOwnedRunMarker(conf, preparingRunPath, runId)

  private def deleteOwnedRunMarker(
    conf: org.apache.hadoop.conf.Configuration,
    markerPath: String,
    runId: String
  ): Unit = {
    ActiveRunMarkerLock.synchronized {
      readActiveRunMarker(conf, markerPath) match {
        case Some(marker) if marker.runId == runId =>
          val path = new Path(markerPath)
          val fs = path.getFileSystem(conf)
          if (fs.exists(path)) {
            fs.delete(path, false)
          }
        case _ =>
      }
    }
  }

  private def progressRootHasRunMetadata(
    fs: org.apache.hadoop.fs.FileSystem,
    root: Path
  ): Boolean =
    Option(fs.listStatus(root)).getOrElse(Array.empty).exists { status =>
      status.isDirectory && fs.exists(new Path(status.getPath, "meta/run.json"))
    }

  private def progressRootHasFailedRunMetadata(
    conf: org.apache.hadoop.conf.Configuration,
    root: Path
  ): Boolean = {
    val fs = root.getFileSystem(conf)
    Option(fs.listStatus(root)).getOrElse(Array.empty).exists { status =>
      status.isDirectory && readRunMetadataFile(conf, new Path(status.getPath, "meta/run.json")).exists(_.state == "FAILED")
    }
  }

  private def readProgressRunMetadata(
    conf: org.apache.hadoop.conf.Configuration,
    progressRun: ProgressRun
  ): Option[ProgressRunMetadata] =
    readRunMetadataFile(conf, new Path(s"${progressRun.metaPath}/run.json"))

  private def readRunMetadataFile(
    conf: org.apache.hadoop.conf.Configuration,
    path: Path
  ): Option[ProgressRunMetadata] = {
    val fs = path.getFileSystem(conf)
    if (!fs.exists(path)) {
      return None
    }

    val reader = new BufferedReader(new InputStreamReader(fs.open(path), StandardCharsets.UTF_8))
    try {
      Option(reader.readLine())
        .flatMap { line =>
          for {
            runId <- extractJsonStringField(line, "run_id")
            state <- extractJsonStringField(line, "state")
          } yield ProgressRunMetadata(runId, state)
        }
    } finally {
      reader.close()
    }
  }

  private def deleteEmptyProgressRoot(
    fs: org.apache.hadoop.fs.FileSystem,
    root: Path
  ): Unit = {
    if (fs.exists(root) && Option(fs.listStatus(root)).getOrElse(Array.empty).isEmpty) {
      fs.delete(root, true)
    }
  }

  private def readActiveRunMarker(
    conf: org.apache.hadoop.conf.Configuration,
    activeRunPath: String
  ): Option[ActiveRunMarker] = {
    val path = new Path(activeRunPath)
    val fs = path.getFileSystem(conf)
    if (!fs.exists(path)) {
      return None
    }

    try {
      val reader = new BufferedReader(new InputStreamReader(fs.open(path), StandardCharsets.UTF_8))
      try {
        val line = Option(reader.readLine()).getOrElse("")
        for {
          runId <- extractJsonStringField(line, "run_id")
          state <- extractJsonStringField(line, "state")
          heartbeat <- extractJsonLongField(line, "last_heartbeat_epoch_ms")
        } yield ActiveRunMarker(runId, state, heartbeat)
      } finally {
        reader.close()
      }
    } catch {
      case NonFatal(_) => None
    }
  }

  private def isStaleActiveRun(marker: ActiveRunMarker): Boolean =
    System.currentTimeMillis() - marker.lastHeartbeatEpochMillis > ActiveRunStaleThresholdMillis

  private def scanResultToJson(result: ScanResult): String =
    s"""{"dataset_path":${jsonString(result.dataset_path)},"scan_timestamp":${jsonString(result.scan_timestamp)},"file_identifier":${jsonString(result.file_identifier)},"column_name":${jsonString(result.column_name)},"pii_type":${jsonString(result.pii_type)},"match_count":${result.match_count},"match_ratio":${result.match_ratio},"confidence":${result.confidence}}"""

  private def scanErrorToJson(error: ScanError): String =
    s"""{"dataset_path":${jsonString(error.dataset_path)},"scan_timestamp":${jsonString(error.scan_timestamp)},"file_identifier":${jsonString(error.file_identifier)},"error_message":${jsonString(error.error_message)}}"""

  private def progressCompletionToJson(scope: String, identifier: String, resultCount: Int, errorCount: Int): String =
    s"""{"scope":${jsonString(scope)},"identifier":${jsonString(identifier)},"result_count":$resultCount,"error_count":$errorCount,"state":"completed"}"""

  private def activeRunMetadataJson(
    progressRun: ProgressRun,
    state: String,
    lastHeartbeatEpochMillis: Long,
    errorMessage: Option[String]
  ): String =
    s"""{"run_id":${jsonString(progressRun.runId)},"dataset_path":${jsonString(progressRun.datasetPath)},"output_root":${jsonString(progressRun.outputRoot)},"scan_timestamp":${jsonString(progressRun.scanTimestamp)},"state":${jsonString(state)},"last_heartbeat_epoch_ms":$lastHeartbeatEpochMillis,"error_message":${jsonNullableString(errorMessage)}}"""

  private def progressRunMetadataJson(
    progressRun: ProgressRun,
    state: String,
    errorMessage: Option[String]
  ): String =
    s"""{"run_id":${jsonString(progressRun.runId)},"dataset_path":${jsonString(progressRun.datasetPath)},"output_root":${jsonString(progressRun.outputRoot)},"scan_timestamp":${jsonString(progressRun.scanTimestamp)},"state":${jsonString(state)},"error_message":${jsonNullableString(errorMessage)}}"""

  private def jsonString(value: String): String = "\"" + escapeJson(Option(value).getOrElse("")) + "\""

  private def jsonNullableString(value: Option[String]): String = value.map(jsonString).getOrElse("null")

  private def extractJsonStringField(json: String, field: String): Option[String] = {
    val pattern = (""""""" + java.util.regex.Pattern.quote(field) + """":"([^"]*)"""").r
    pattern.findFirstMatchIn(json).map(_.group(1))
  }

  private def extractJsonLongField(json: String, field: String): Option[Long] = {
    val pattern = (""""""" + java.util.regex.Pattern.quote(field) + """":([0-9]+)""").r
    pattern.findFirstMatchIn(json).flatMap(m => Try(m.group(1).toLong).toOption)
  }

  private def escapeJson(value: String): String = {
    val builder = new StringBuilder
    value.foreach {
      case '"' => builder.append("\\\"")
      case '\\' => builder.append("\\\\")
      case '\b' => builder.append("\\b")
      case '\f' => builder.append("\\f")
      case '\n' => builder.append("\\n")
      case '\r' => builder.append("\\r")
      case '\t' => builder.append("\\t")
      case ch if ch < ' ' => builder.append(f"\\u${ch.toInt}%04x")
      case ch => builder.append(ch)
    }
    builder.toString()
  }
}
