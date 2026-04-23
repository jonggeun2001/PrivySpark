package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.format.ExcelReadConfig
import io.github.jonggeun2001.privyspark.model.ScanReadOptions
import io.github.jonggeun2001.privyspark.scan.SourceExpansion
import org.apache.spark.SparkConf
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExcelReadConfigSpec extends AnyFunSuite {
  test("reader options include maxRowsInMemory from Spark conf") {
    val conf = new SparkConf(false)
      .set(ExcelReadConfig.MaxRowsInMemoryConfKey, "2048")

    val options = ExcelReadConfig.readerOptions(conf, ScanReadOptions()).toMap

    assert(options.get("maxRowsInMemory").contains("2048"))
  }

  test("reader options prefer explicit read options over Spark conf") {
    val conf = new SparkConf(false)
      .set(ExcelReadConfig.MaxRowsInMemoryConfKey, "1024")

    val options = ExcelReadConfig
      .readerOptions(conf, ScanReadOptions(excelMaxRowsInMemory = Some(4096)))
      .toMap

    assert(options.get("maxRowsInMemory").contains("4096"))
  }

  test("reader options reject invalid Spark conf values") {
    val conf = new SparkConf(false)
      .set(ExcelReadConfig.MaxRowsInMemoryConfKey, "0")

    val error = intercept[IllegalArgumentException] {
      ExcelReadConfig.readerOptions(conf, ScanReadOptions())
    }

    assert(error.getMessage.contains(ExcelReadConfig.MaxRowsInMemoryConfKey))
    assert(error.getMessage.contains("> 0"))
  }

  test("workbook sheet read options preserve excel maxRowsInMemory") {
    val readOptions = SourceExpansion.workbookSheetReadOptions(
      ScanReadOptions(excelMaxRowsInMemory = Some(4096)),
      "Contacts"
    )

    assert(readOptions.sheetName.contains("Contacts"))
    assert(readOptions.excelMaxRowsInMemory.contains(4096))
  }
}
