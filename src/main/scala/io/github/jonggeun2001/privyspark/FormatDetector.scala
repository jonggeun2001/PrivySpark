package io.github.jonggeun2001.privyspark

object FormatDetector {
  def infer(filePath: String): Option[String] = {
    val lower = filePath.toLowerCase
    if (lower.endsWith(".csv")) Some("csv")
    else if (lower.endsWith(".json") || lower.endsWith(".jsonl") || lower.endsWith(".ndjson")) Some("json")
    else if (lower.endsWith(".parquet")) Some("parquet")
    else if (lower.endsWith(".orc")) Some("orc")
    else None
  }
}
