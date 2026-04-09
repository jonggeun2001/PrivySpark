package io.github.jonggeun2001.privyspark

object FormatDetector {
  def infer(filePath: String): Option[String] = {
    val lower = Option(filePath).getOrElse("").toLowerCase
    val withoutSheetSuffix = lower.split("#", 2).headOption.getOrElse(lower)
    if (withoutSheetSuffix.endsWith(".csv")) Some("csv")
    else if (withoutSheetSuffix.endsWith(".json") || withoutSheetSuffix.endsWith(".jsonl") || withoutSheetSuffix.endsWith(".ndjson")) Some("json")
    else if (withoutSheetSuffix.endsWith(".parquet")) Some("parquet")
    else if (withoutSheetSuffix.endsWith(".orc")) Some("orc")
    else if (withoutSheetSuffix.endsWith(".avro")) Some("avro")
    else if (withoutSheetSuffix.endsWith(".xlsx")) Some("xlsx")
    else if (withoutSheetSuffix.endsWith(".zip")) Some("zip")
    else if (withoutSheetSuffix.endsWith(".jar")) Some("jar")
    else None
  }
}
