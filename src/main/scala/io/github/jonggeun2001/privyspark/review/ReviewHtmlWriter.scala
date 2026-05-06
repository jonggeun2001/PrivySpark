package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.model.ScanResult
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.util.Locale

private[privyspark] object ReviewHtmlWriter {
  val DefaultSampleMode = "masked"
  val SupportedSampleModes: Set[String] = Set("raw", "masked", "none")

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
    writeFindings(conf, outputRoot, scanPath, findings, normalizedSampleMode, reviewHtmlDir)
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
    writeFindings(conf, outputRoot, scanPath, findings, normalizedSampleMode, reviewHtmlDir)
  }

  private def writeFindings(
    conf: Configuration,
    outputRoot: String,
    scanPath: String,
    findings: Seq[ReviewFinding],
    sampleMode: String,
    reviewHtmlDir: Option[String]
  ): Unit = {
    val scanResultsFingerprint = ReviewFindingBuilder.scanResultsFingerprint(findings)
    val html = ReviewHtmlRenderer.render(scanPath, scanResultsFingerprint, findings, sampleMode)
    val htmlPath = resolveHtmlPath(outputRoot, reviewHtmlDir)
    val fs = htmlPath.getFileSystem(conf)
    Option(htmlPath.getParent).foreach(fs.mkdirs)
    val writer = new BufferedWriter(new OutputStreamWriter(fs.create(htmlPath, true), StandardCharsets.UTF_8))
    try {
      writer.write(html)
    } finally {
      writer.close()
    }
  }

  private def resolveHtmlPath(outputRoot: String, reviewHtmlDir: Option[String]): Path =
    reviewHtmlDir
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(directory => new Path(new Path(directory), "review.html"))
      .getOrElse(new Path(new Path(outputRoot), "review/review.html"))
}
