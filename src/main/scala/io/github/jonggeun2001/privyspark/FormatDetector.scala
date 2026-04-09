package io.github.jonggeun2001.privyspark

object FormatDetector {
  def infer(filePath: String): Option[String] = {
    val lower = Option(filePath).getOrElse("").toLowerCase
    if (lower.endsWith(".csv")) Some("csv")
    else if (lower.endsWith(".json") || lower.endsWith(".jsonl") || lower.endsWith(".ndjson")) Some("json")
    else if (lower.endsWith(".parquet")) Some("parquet")
    else if (lower.endsWith(".orc")) Some("orc")
    else if (lower.endsWith(".avro")) Some("avro")
    else if (lower.endsWith(".xlsx")) Some("xlsx")
    else if (lower.endsWith(".zip")) Some("zip")
    else if (lower.endsWith(".jar")) Some("jar")
    else None
  }
}
