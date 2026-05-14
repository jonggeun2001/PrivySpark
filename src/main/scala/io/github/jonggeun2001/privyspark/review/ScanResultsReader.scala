package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.format.ByteProbe.detectPhysicalFormat
import io.github.jonggeun2001.privyspark.format.CsvInference.{XlsxFormat, readSource}
import io.github.jonggeun2001.privyspark.model.{ScanReadOptions, ScanResult}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col

import scala.collection.JavaConverters._
import scala.util.Try

private[privyspark] object ScanResultsReader {
  def read(spark: SparkSession, scanResultsPath: String): DataFrame = {
    val conf = spark.sparkContext.hadoopConfiguration
    resolveScanResultsFormat(conf, scanResultsPath) match {
      case XlsxFormat =>
        readSource(
          spark,
          XlsxFormat,
          Seq(scanResultsPath),
          readOptions = ScanReadOptions(sheetName = Some("scan_results"))
        )
      case format =>
        readSource(spark, format, Seq(scanResultsPath), csvHasHeader = true)
    }
  }

  def toScanResults(df: DataFrame): Seq[ScanResult] = {
    iterateScanResults(df).toSeq
  }

  def iterateScanResults(df: DataFrame, ordered: Boolean = false): Iterator[ScanResult] = {
    val normalizedColumns = df.columns.map(columnName => columnName.toLowerCase -> columnName).toMap
    val requiredColumns = Seq(
      "dataset_path",
      "scan_timestamp",
      "file_identifier",
      "column_name",
      "pii_type",
      "match_count",
      "sampled_row_count",
      "match_ratio",
      "non_empty_match_ratio",
      "confidence",
      "sample_raw_value",
      "sample_matched_fragment",
      "file_size",
      "file_mtime_epoch_ms"
    )
    requiredColumns.foreach { columnName =>
      require(normalizedColumns.contains(columnName), s"scan_results is missing required column: $columnName")
    }

    val sourceDf =
      if (ordered) {
        val sortColumns = Seq(
          "dataset_path",
          "hive_table_fqn",
          "column_name",
          "pii_type",
          "file_identifier",
          "scan_timestamp",
          "file_size",
          "file_mtime_epoch_ms",
          "match_count",
          "sampled_row_count",
          "match_ratio",
          "non_empty_match_ratio",
          "confidence"
        ).flatMap(normalizedColumns.get)
        if (sortColumns.nonEmpty) df.sort(sortColumns.map(name => col(name)): _*) else df
      } else {
        df
      }

    sourceDf.toLocalIterator().asScala.map(row => toScanResult(row, normalizedColumns))
  }

  def resolveScanResultsFormat(
    conf: org.apache.hadoop.conf.Configuration,
    scanResultsPath: String
  ): String = {
    val path = new Path(scanResultsPath)
    val fs = path.getFileSystem(conf)

    if (fs.exists(path) && fs.getFileStatus(path).isDirectory) {
      Option(path.getParent)
        .map(_.getName.toLowerCase)
        .collect {
          case "csv" => "csv"
          case "parquet" => "parquet"
        }
        .getOrElse(throw new IllegalArgumentException(s"Unsupported scan_results directory format: $scanResultsPath"))
    } else {
      detectPhysicalFormat(conf, scanResultsPath)
        .getOrElse(throw new IllegalArgumentException(s"Unsupported scan_results format: $scanResultsPath"))
    }
  }

  private def valueOf(row: Row, columnName: String): String =
    if (row.isNullAt(row.fieldIndex(columnName))) "" else Option(row.get(row.fieldIndex(columnName))).map(_.toString).getOrElse("")

  private def toScanResult(row: Row, normalizedColumns: Map[String, String]): ScanResult = {
    val matchCount = longValue(row, normalizedColumns("match_count"))
    val sampledRowCount = longValue(row, normalizedColumns("sampled_row_count"))
    val nonEmptyMatchRatio = doubleValue(row, normalizedColumns("non_empty_match_ratio"))
    ScanResult(
      dataset_path = valueOf(row, normalizedColumns("dataset_path")),
      scan_timestamp = valueOf(row, normalizedColumns("scan_timestamp")),
      file_identifier = valueOf(row, normalizedColumns("file_identifier")),
      column_name = valueOf(row, normalizedColumns("column_name")),
      pii_type = valueOf(row, normalizedColumns("pii_type")),
      match_count = matchCount,
      sampled_row_count = sampledRowCount,
      non_empty_value_count = optionalLongValue(row, normalizedColumns, "non_empty_value_count")
        .filter(_ > 0L)
        .getOrElse(deriveNonEmptyValueCount(matchCount, sampledRowCount, nonEmptyMatchRatio)),
      match_ratio = doubleValue(row, normalizedColumns("match_ratio")),
      non_empty_match_ratio = nonEmptyMatchRatio,
      confidence = doubleValue(row, normalizedColumns("confidence")),
      sample_raw_value = valueOf(row, normalizedColumns("sample_raw_value")),
      sample_matched_fragment = valueOf(row, normalizedColumns("sample_matched_fragment")),
      file_size = longValue(row, normalizedColumns("file_size")),
      file_mtime_epoch_ms = longValue(row, normalizedColumns("file_mtime_epoch_ms")),
      hive_table_fqn = optionalValue(row, normalizedColumns, "hive_table_fqn"),
      aggregated = booleanValue(row, normalizedColumns.get("aggregated")),
      aggregated_file_count = optionalLongValue(row, normalizedColumns, "aggregated_file_count").map(_.toInt).getOrElse(1),
      aggregated_partition_count = optionalLongValue(row, normalizedColumns, "aggregated_partition_count").map(_.toInt).getOrElse(0),
      review_status = optionalValue(row, normalizedColumns, "review_status", "pending"),
      review_reason = optionalValue(row, normalizedColumns, "review_reason"),
      review_invalidated = booleanValue(row, normalizedColumns.get("review_invalidated")),
      review_scope_file_identifiers = optionalValue(row, normalizedColumns, "review_scope_file_identifiers"),
      review_scope_file_fingerprints = optionalValue(row, normalizedColumns, "review_scope_file_fingerprints")
    )
  }

  private def optionalValue(row: Row, columns: Map[String, String], normalizedColumnName: String, defaultValue: String = ""): String =
    columns.get(normalizedColumnName).map(valueOf(row, _)).getOrElse(defaultValue)

  private def optionalLongValue(row: Row, columns: Map[String, String], normalizedColumnName: String): Option[Long] =
    columns.get(normalizedColumnName).map(longValue(row, _))

  private def longValue(row: Row, columnName: String): Long =
    Try(row.getAs[Long](columnName)).orElse(Try(valueOf(row, columnName).toLong)).getOrElse(0L)

  private def doubleValue(row: Row, columnName: String): Double =
    Try(row.getAs[Double](columnName)).orElse(Try(valueOf(row, columnName).toDouble)).getOrElse(0.0)

  private def booleanValue(row: Row, columnName: Option[String]): Boolean =
    columnName.exists(name => Try(row.getAs[Boolean](name)).orElse(Try(valueOf(row, name).toBoolean)).getOrElse(false))

  private def deriveNonEmptyValueCount(matchCount: Long, sampledRowCount: Long, nonEmptyMatchRatio: Double): Long =
    if (nonEmptyMatchRatio > 0.0 && matchCount > 0L) math.round(matchCount.toDouble / nonEmptyMatchRatio)
    else sampledRowCount
}
