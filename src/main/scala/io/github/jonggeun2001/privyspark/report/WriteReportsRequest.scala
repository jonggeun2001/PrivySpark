package io.github.jonggeun2001.privyspark.report

import org.apache.spark.sql.DataFrame

private[privyspark] final case class WriteReportsRequest(
  outputRoot: String,
  resultsDf: DataFrame,
  errorsDf: DataFrame,
  outputFormats: Seq[String] = OutputFormats.Default,
  beforePromote: () => Unit = () => ()
)
