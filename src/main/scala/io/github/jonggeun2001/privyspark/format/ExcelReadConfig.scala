package io.github.jonggeun2001.privyspark.format

import io.github.jonggeun2001.privyspark.model.ScanReadOptions
import org.apache.spark.SparkConf

private[privyspark] object ExcelReadConfig {
  val MaxRowsInMemoryConfKey = "spark.privyspark.excel.maxRowsInMemory"
  val MaxRowsInMemoryReaderOption = "maxRowsInMemory"

  def readerOptions(conf: SparkConf, readOptions: ScanReadOptions): Seq[(String, String)] = {
    resolveMaxRowsInMemory(conf, readOptions)
      .map(value => Seq(MaxRowsInMemoryReaderOption -> value.toString))
      .getOrElse(Seq.empty)
  }

  def renderConfiguredMaxRowsInMemory(configured: Option[Int]): String = {
    configured.map(_.toString).getOrElse(s"${MaxRowsInMemoryConfKey}_or_unset")
  }

  private def resolveMaxRowsInMemory(conf: SparkConf, readOptions: ScanReadOptions): Option[Int] = {
    readOptions.excelMaxRowsInMemory.orElse {
      conf.getOption(MaxRowsInMemoryConfKey).map(value => parsePositiveInt(value, MaxRowsInMemoryConfKey))
    }
  }

  private def parsePositiveInt(rawValue: String, source: String): Int = {
    val trimmed = Option(rawValue).map(_.trim).getOrElse("")
    val value =
      try {
        trimmed.toInt
      } catch {
        case _: NumberFormatException =>
          throw new IllegalArgumentException(s"$source must be > 0")
      }

    if (value > 0) {
      value
    } else {
      throw new IllegalArgumentException(s"$source must be > 0")
    }
  }
}
