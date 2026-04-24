package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.fsio.ManagedPaths.cleanupStagingPaths
import io.github.jonggeun2001.privyspark.format.ExcelReadConfig
import io.github.jonggeun2001.privyspark.model.ScanReadOptions
import io.github.jonggeun2001.privyspark.scan.SourceExpansion
import org.apache.hadoop.conf.Configuration
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.SparkConf
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.ByteArrayOutputStream
import java.nio.file.Files
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class ExcelReadConfigSpec extends AnyFunSuite {
  test("reader options include default maxRowsInMemory when unset") {
    val conf = new SparkConf(false)

    val options = ExcelReadConfig.readerOptions(conf, ScanReadOptions()).toMap

    assert(options.get("maxRowsInMemory").contains("2048"))
  }

  test("reader options include maxRowsInMemory from Spark conf") {
    val conf = new SparkConf(false)
      .set(ExcelReadConfig.MaxRowsInMemoryConfKey, "8192")

    val options = ExcelReadConfig.readerOptions(conf, ScanReadOptions()).toMap

    assert(options.get("maxRowsInMemory").contains("8192"))
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

  test("archive expansion preserves excel maxRowsInMemory for nested xlsx sheets") {
    val tempDir = Files.createTempDirectory("privyspark-excel-archive-read-options-")
    val archivePath = tempDir.resolve("bundle.zip")
    val conf = new Configuration()
    val stagingPaths = ArrayBuffer.empty[String]

    try {
      writeZipWithWorkbook(archivePath, "nested/contacts.xlsx")

      val (entries, errors, ignoredEntries) = SourceExpansion.expandPhysicalSource(
        conf,
        datasetPath = archivePath.toString,
        timestamp = "2026-04-23T00:00:00Z",
        physicalPath = archivePath.toString,
        logicalIdentifier = "bundle.zip",
        groupingDirectoryPath = tempDir.toString,
        stagingPaths = stagingPaths,
        readOptions = ScanReadOptions(excelMaxRowsInMemory = Some(4096))
      )

      assert(errors.isEmpty)
      assert(ignoredEntries == 0)
      val workbookEntry = entries.find(_.logicalIdentifier == "bundle.zip!nested/contacts.xlsx#Contacts")
        .getOrElse(fail(s"Expected nested workbook sheet entry, got: ${entries.map(_.logicalIdentifier).mkString(",")}"))
      assert(workbookEntry.readOptions.sheetName.contains("Contacts"))
      assert(workbookEntry.readOptions.excelMaxRowsInMemory.contains(4096))
    } finally {
      cleanupStagingPaths(conf, stagingPaths.toSeq)
      Files.deleteIfExists(archivePath)
      Files.deleteIfExists(tempDir)
    }
  }

  private def writeZipWithWorkbook(path: java.nio.file.Path, entryName: String): Unit = {
    val zipOutputStream = new ZipOutputStream(Files.newOutputStream(path))
    try {
      zipOutputStream.putNextEntry(new ZipEntry(entryName))
      zipOutputStream.write(workbookBytes())
      zipOutputStream.closeEntry()
    } finally {
      zipOutputStream.close()
    }
  }

  private def workbookBytes(): Array[Byte] = {
    val workbook = new XSSFWorkbook()
    try {
      val sheet = workbook.createSheet("Contacts")
      sheet.createRow(0).createCell(0).setCellValue("email")
      val outputStream = new ByteArrayOutputStream()
      try {
        workbook.write(outputStream)
        outputStream.toByteArray
      } finally {
        outputStream.close()
      }
    } finally {
      workbook.close()
    }
  }
}
