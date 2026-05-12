package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.model.ScanResult
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.util.Locale
import scala.collection.mutable.ArrayBuffer

private[privyspark] object ReviewHtmlWriter {
  val DefaultSampleMode = "masked"
  val SupportedSampleModes: Set[String] = Set("raw", "masked", "none")
  val MaxReviewHtmlBytes: Long = 2L * 1024L * 1024L
  private val ReviewPartFilePattern = """review-part-\d{4}\.html""".r
  private val SplitSafetyBytes = 16L * 1024L

  def normalizeSampleMode(value: String): Option[String] = {
    val normalized = Option(value).map(_.trim.toLowerCase(Locale.ROOT)).getOrElse("")
    if (SupportedSampleModes.contains(normalized)) Some(normalized) else None
  }

  def write(
    conf: Configuration,
    outputRoot: String,
    scanPath: String,
    results: Seq[ScanResult],
    sampleMode: String
  ): Unit =
    write(conf, outputRoot, scanPath, results, sampleMode, None)

  def write(
    conf: Configuration,
    outputRoot: String,
    scanPath: String,
    results: Seq[ScanResult],
    sampleMode: String,
    reviewHtmlDir: Option[String]
  ): Unit = {
    val normalizedSampleMode = normalizeSampleMode(sampleMode).getOrElse(DefaultSampleMode)
    val findings = ReviewFindingBuilder.fromScanResultsIterator(
      results.iterator,
      ReviewFindingBuilder.DefaultMaxEvidenceSamples
    )
    writeFindings(conf, outputRoot, scanPath, findings, normalizedSampleMode, reviewHtmlDir, None)
  }

  def write(
    conf: Configuration,
    outputRoot: String,
    scanPath: String,
    results: Seq[ScanResult],
    sampleMode: String,
    reviewHtmlDir: Option[String],
    reviewStateRoot: Option[String]
  ): Unit = {
    val normalizedSampleMode = normalizeSampleMode(sampleMode).getOrElse(DefaultSampleMode)
    val findings = ReviewFindingBuilder.fromScanResultsIterator(
      results.iterator,
      ReviewFindingBuilder.DefaultMaxEvidenceSamples
    )
    writeFindings(conf, outputRoot, scanPath, findings, normalizedSampleMode, reviewHtmlDir, reviewStateRoot)
  }

  def write(
    conf: Configuration,
    outputRoot: String,
    scanPath: String,
    resultDf: DataFrame,
    sampleMode: String
  ): Unit =
    write(conf, outputRoot, scanPath, resultDf, sampleMode, None)

  def write(
    conf: Configuration,
    outputRoot: String,
    scanPath: String,
    resultDf: DataFrame,
    sampleMode: String,
    reviewHtmlDir: Option[String]
  ): Unit = {
    val normalizedSampleMode = normalizeSampleMode(sampleMode).getOrElse(DefaultSampleMode)
    val findings = ReviewFindingBuilder.fromScanResultsIterator(
      ScanResultsReader.iterateScanResults(resultDf, ordered = true),
      ReviewFindingBuilder.DefaultMaxEvidenceSamples
    )
    writeFindings(conf, outputRoot, scanPath, findings, normalizedSampleMode, reviewHtmlDir, None)
  }

  def write(
    conf: Configuration,
    outputRoot: String,
    scanPath: String,
    resultDf: DataFrame,
    sampleMode: String,
    reviewHtmlDir: Option[String],
    reviewStateRoot: Option[String]
  ): Unit = {
    val normalizedSampleMode = normalizeSampleMode(sampleMode).getOrElse(DefaultSampleMode)
    val findings = ReviewFindingBuilder.fromScanResultsIterator(
      ScanResultsReader.iterateScanResults(resultDf, ordered = true),
      ReviewFindingBuilder.DefaultMaxEvidenceSamples
    )
    writeFindings(conf, outputRoot, scanPath, findings, normalizedSampleMode, reviewHtmlDir, reviewStateRoot)
  }

  private def writeFindings(
    conf: Configuration,
    outputRoot: String,
    scanPath: String,
    findings: Seq[ReviewFinding],
    sampleMode: String,
    reviewHtmlDir: Option[String],
    reviewStateRoot: Option[String]
  ): Unit = {
    val scanResultsFingerprint = ReviewFindingBuilder.scanResultsFingerprint(findings)
    val actionPlanStates = ReviewActionPlanStatus.matchFindings(
      findings,
      ReviewActionPlanStatus.load(conf, reviewStateRoot)
    )
    val reviewDir = resolveReviewDirectory(outputRoot, reviewHtmlDir)
    val htmlPath = new Path(reviewDir, "review.html")
    val fs = htmlPath.getFileSystem(conf)
    Option(htmlPath.getParent).foreach(fs.mkdirs)
    deleteExistingPartFiles(fs, reviewDir)

    val chunks =
      if (findings.isEmpty) Seq.empty
      else splitFindings(scanPath, scanResultsFingerprint, findings, sampleMode, actionPlanStates)
    if (chunks.size <= 1) {
      val html = ReviewHtmlRenderer.render(scanPath, scanResultsFingerprint, findings, sampleMode, actionPlanStates)
      writeText(fs, htmlPath, html)
    } else {
      writeSplitReviewHtml(fs, reviewDir, htmlPath, scanPath, scanResultsFingerprint, chunks, sampleMode, actionPlanStates)
    }
  }

  private def writeSplitReviewHtml(
    fs: FileSystem,
    reviewDir: Path,
    indexPath: Path,
    scanPath: String,
    scanResultsFingerprint: String,
    chunks: Seq[Seq[ReviewFinding]],
    sampleMode: String,
    actionPlanStates: Map[String, ReviewActionPlanStatus]
  ): Unit = {
    val parts = toPartInfos(chunks)
    chunks.zip(parts).foreach {
      case (chunk, partInfo) =>
        val partHtml = ReviewHtmlRenderer.render(
          scanPath,
          scanResultsFingerprint,
          chunk,
          sampleMode,
          actionPlanStates,
          Some(partInfo)
        )
        require(
          utf8Size(partHtml) <= MaxReviewHtmlBytes,
          s"review part ${partInfo.fileName} exceeds ${MaxReviewHtmlBytes} bytes"
        )
        writeText(fs, new Path(reviewDir, partInfo.fileName), partHtml)
    }
    val indexHtml = ReviewHtmlRenderer.renderIndex(scanPath, scanResultsFingerprint, parts)
    require(utf8Size(indexHtml) <= MaxReviewHtmlBytes, s"review.html index exceeds ${MaxReviewHtmlBytes} bytes")
    writeText(fs, indexPath, indexHtml)
  }

  private def splitFindings(
    scanPath: String,
    scanResultsFingerprint: String,
    findings: Seq[ReviewFinding],
    sampleMode: String,
    actionPlanStates: Map[String, ReviewActionPlanStatus]
  ): Seq[Seq[ReviewFinding]] = {
    val partDataBudget = MaxReviewHtmlBytes -
      renderedEmptyPartSize(scanPath, scanResultsFingerprint, sampleMode, actionPlanStates) -
      SplitSafetyBytes
    require(partDataBudget > 0, s"review HTML template overhead exceeds ${MaxReviewHtmlBytes} bytes")

    val chunks = ArrayBuffer.empty[Vector[ReviewFinding]]
    var current = Vector.empty[ReviewFinding]
    var currentSize = 0L

    findings.foreach { finding =>
      val findingSize = renderedFindingDataSize(finding, sampleMode, actionPlanStates)
      require(findingSize <= partDataBudget, s"single review finding exceeds ${MaxReviewHtmlBytes} bytes: ${finding.findingKey}")
      val separatorSize = if (current.isEmpty) 0L else 1L
      if (current.nonEmpty && currentSize + separatorSize + findingSize > partDataBudget) {
        chunks += current
        current = Vector(finding)
        currentSize = findingSize
      } else {
        current = current :+ finding
        currentSize += separatorSize + findingSize
      }
    }

    if (current.nonEmpty) {
      chunks += current
    }
    chunks.toSeq
  }

  private def renderedEmptyPartSize(
    scanPath: String,
    scanResultsFingerprint: String,
    sampleMode: String,
    actionPlanStates: Map[String, ReviewActionPlanStatus]
  ): Long = {
    val probePart = ReviewHtmlPartInfo(
      partNumber = 9999,
      partCount = 9999,
      findingStart = 1,
      findingEnd = 0,
      findingCount = 0,
      totalFindings = 0,
      fileName = "review-part-9999.html"
    )
    utf8Size(ReviewHtmlRenderer.render(scanPath, scanResultsFingerprint, Seq.empty, sampleMode, actionPlanStates, Some(probePart)))
  }

  private def renderedFindingDataSize(
    finding: ReviewFinding,
    sampleMode: String,
    actionPlanStates: Map[String, ReviewActionPlanStatus]
  ): Long =
    utf8Size(ReviewSampleMasker.findingToJson(finding, sampleMode, actionPlanStates.get(finding.findingKey)).replace("</", "<\\/"))

  private def toPartInfos(chunks: Seq[Seq[ReviewFinding]]): Seq[ReviewHtmlPartInfo] = {
    val totalFindings = chunks.map(_.size).sum
    var start = 1
    chunks.zipWithIndex.map {
      case (chunk, index) =>
        val end = start + chunk.size - 1
        val info = ReviewHtmlPartInfo(
          partNumber = index + 1,
          partCount = chunks.size,
          findingStart = start,
          findingEnd = end,
          findingCount = chunk.size,
          totalFindings = totalFindings,
          fileName = f"review-part-${index + 1}%04d.html"
        )
        start = end + 1
        info
    }
  }

  private def deleteExistingPartFiles(fs: FileSystem, reviewDir: Path): Unit = {
    if (fs.exists(reviewDir)) {
      fs.listStatus(reviewDir)
        .filter(status => status.isFile && ReviewPartFilePattern.pattern.matcher(status.getPath.getName).matches())
        .foreach(status => fs.delete(status.getPath, false))
    }
  }

  private def writeText(fs: FileSystem, path: Path, value: String): Unit = {
    val writer = new BufferedWriter(new OutputStreamWriter(fs.create(path, true), StandardCharsets.UTF_8))
    try {
      writer.write(value)
    } finally {
      writer.close()
    }
  }

  private def utf8Size(value: String): Long =
    value.getBytes(StandardCharsets.UTF_8).length.toLong

  private def resolveReviewDirectory(outputRoot: String, reviewHtmlDir: Option[String]): Path =
    reviewHtmlDir
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(directory => new Path(directory))
      .getOrElse(new Path(new Path(outputRoot), "review"))
}
