package io.github.jonggeun2001.privyspark.format

import org.apache.poi.util.IOUtils
import org.apache.spark.SparkConf

private[privyspark] object ExcelReadConfig {
  val MaxRowsInMemoryConfKey = "spark.privyspark.excel.maxRowsInMemory"
  val ByteArrayMaxOverrideConfKey = "spark.privyspark.excel.byteArrayMaxOverride"
  val DefaultByteArrayMaxOverride = 300000000

  def renderConfiguredMaxRowsInMemory(configured: Option[Int]): String = {
    configured.map(_.toString).getOrElse(s"${MaxRowsInMemoryConfKey}_or_unset")
  }

  def renderConfiguredByteArrayMaxOverride(configured: Option[Int]): String = {
    configured.map(_.toString).getOrElse(s"${ByteArrayMaxOverrideConfKey}_or_unset")
  }

  def resolveByteArrayMaxOverride(conf: SparkConf, configured: Option[Int]): Int = {
    configured.map(value => validatePositiveInt(value, ByteArrayMaxOverrideConfKey)).orElse {
      conf.getOption(ByteArrayMaxOverrideConfKey).map(value => parsePositiveInt(value, ByteArrayMaxOverrideConfKey))
    }.getOrElse(DefaultByteArrayMaxOverride)
  }

  def applyByteArrayMaxOverride(value: Int): Unit = {
    IOUtils.setByteArrayMaxOverride(value)
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

    validatePositiveInt(value, source)
  }

  private def validatePositiveInt(value: Int, source: String): Int = {
    if (value <= 0) {
      throw new IllegalArgumentException(s"$source must be > 0")
    }
    value
  }
}
