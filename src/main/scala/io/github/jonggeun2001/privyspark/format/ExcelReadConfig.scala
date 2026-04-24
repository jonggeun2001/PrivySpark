package io.github.jonggeun2001.privyspark.format

import io.github.jonggeun2001.privyspark.model.ScanReadOptions
import org.apache.poi.util.IOUtils
import org.apache.spark.SparkConf

private[privyspark] object ExcelReadConfig {
  val MaxRowsInMemoryConfKey = "spark.privyspark.excel.maxRowsInMemory"
  val ByteArrayMaxOverrideConfKey = "spark.privyspark.excel.byteArrayMaxOverride"
  val MaxRowsInMemoryReaderOption = "maxRowsInMemory"
  val ByteArrayMaxOverrideReaderOption = "maxByteArraySize"
  val DefaultMaxRowsInMemory = 2048
  val DefaultByteArrayMaxOverride = 300000000

  def readerOptions(conf: SparkConf, readOptions: ScanReadOptions): Seq[(String, String)] = {
    val maxRowsInMemory = resolveMaxRowsInMemory(conf, readOptions)
      .map(value => MaxRowsInMemoryReaderOption -> value.toString)
    val byteArrayMaxOverride = Some(
      ByteArrayMaxOverrideReaderOption -> resolveByteArrayMaxOverride(conf, readOptions.excelByteArrayMaxOverride).toString
    )

    maxRowsInMemory.toSeq ++ byteArrayMaxOverride.toSeq
  }

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

  private def resolveMaxRowsInMemory(conf: SparkConf, readOptions: ScanReadOptions): Option[Int] = {
    readOptions.excelMaxRowsInMemory.orElse {
      conf.getOption(MaxRowsInMemoryConfKey).map(value => parsePositiveInt(value, MaxRowsInMemoryConfKey))
    }.orElse(Some(DefaultMaxRowsInMemory))
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
