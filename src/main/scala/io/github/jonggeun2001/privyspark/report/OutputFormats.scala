package io.github.jonggeun2001.privyspark.report

import java.util.Locale

private[privyspark] object OutputFormats {
  val Parquet = "parquet"
  val Csv = "csv"
  val Excel = "excel"

  val Default: Seq[String] = Seq(Parquet)
  val All: Seq[String] = Seq(Parquet, Csv, Excel)

  private val Allowed = All.toSet

  def normalize(value: String): String =
    Option(value).getOrElse("").trim.toLowerCase(Locale.ROOT)

  def validate(value: String): Either[String, String] = {
    val normalized = normalize(value)
    if (Allowed.contains(normalized)) Right(normalized)
    else Left(s"output-format must be one of: ${All.mkString(", ")}")
  }

  def normalizeAll(values: Seq[String]): Seq[String] = {
    val normalized = values.iterator.map(normalize).filter(_.nonEmpty).toSeq.distinct
    if (normalized.isEmpty) Default else normalized
  }

  def requireSupported(values: Seq[String]): Seq[String] = {
    val normalized = normalizeAll(values)
    val unsupported = normalized.filterNot(Allowed.contains)
    require(unsupported.isEmpty, s"Unsupported output formats: ${unsupported.mkString(", ")}")
    normalized
  }
}
