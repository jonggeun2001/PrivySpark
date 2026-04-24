package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.fsio.ManagedPaths.cleanupStagingPaths
import io.github.jonggeun2001.privyspark.format.ExcelReadConfig
import io.github.jonggeun2001.privyspark.model.ScanReadOptions
import io.github.jonggeun2001.privyspark.scan.SourceExpansion
import io.github.jonggeun2001.privyspark.util.DriverLogger
import org.apache.hadoop.conf.Configuration
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.SparkConf
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class ExcelReadConfigSpec extends AnyFunSuite {
  test("byte array max override defaults to 300MB when unset") {
    val conf = new SparkConf(false)

    val value = ExcelReadConfig.resolveByteArrayMaxOverride(conf, None)

    assert(value == ExcelReadConfig.DefaultByteArrayMaxOverride)
  }

  test("byte array max override prefers Spark conf when CLI value is unset") {
    val conf = new SparkConf(false)
      .set(ExcelReadConfig.ByteArrayMaxOverrideConfKey, "123456789")

    val value = ExcelReadConfig.resolveByteArrayMaxOverride(conf, None)

    assert(value == 123456789)
  }

  test("byte array max override prefers CLI value over Spark conf") {
    val conf = new SparkConf(false)
      .set(ExcelReadConfig.ByteArrayMaxOverrideConfKey, "123456789")

    val value = ExcelReadConfig.resolveByteArrayMaxOverride(conf, Some(234567890))

    assert(value == 234567890)
  }

  test("byte array max override rejects invalid Spark conf values") {
    val conf = new SparkConf(false)
      .set(ExcelReadConfig.ByteArrayMaxOverrideConfKey, "0")

    val error = intercept[IllegalArgumentException] {
      ExcelReadConfig.resolveByteArrayMaxOverride(conf, None)
    }

    assert(error.getMessage.contains(ExcelReadConfig.ByteArrayMaxOverrideConfKey))
    assert(error.getMessage.contains("> 0"))
  }

  test("workbook sheet read options preserve excel read options") {
    val readOptions = SourceExpansion.workbookSheetReadOptions(
      ScanReadOptions(
        excelMaxRowsInMemory = Some(4096),
        excelByteArrayMaxOverride = Some(123456789)
      ),
      "Contacts"
    )

    assert(readOptions.sheetName.contains("Contacts"))
    assert(readOptions.excelMaxRowsInMemory.contains(4096))
    assert(readOptions.excelByteArrayMaxOverride.contains(123456789))
  }

  test("excel max rows in memory emits a deprecation warning only when explicitly configured") {
    val configuredLogs = withWarnLogging {
      captureStderr {
        PrivySparkApp.warnUnusedExcelMaxRowsInMemory(Some(4096))
      }
    }
    val defaultLogs = withWarnLogging {
      captureStderr {
        PrivySparkApp.warnUnusedExcelMaxRowsInMemory(None)
      }
    }

    assert(configuredLogs.contains("excel_max_rows_in_memory_unused"))
    assert(configuredLogs.contains("argument=--excel-max-rows-in-memory"))
    assert(configuredLogs.contains("value=4096"))
    assert(defaultLogs.isEmpty)
  }

  test("archive expansion preserves compatibility excel read options for nested xlsx sheets") {
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
        readOptions = ScanReadOptions(
          excelMaxRowsInMemory = Some(4096),
          excelByteArrayMaxOverride = Some(123456789)
        )
      )

      assert(errors.isEmpty)
      assert(ignoredEntries == 0)
      val workbookEntry = entries.find(_.logicalIdentifier == "bundle.zip!nested/contacts.xlsx#Contacts")
        .getOrElse(fail(s"Expected nested workbook sheet entry, got: ${entries.map(_.logicalIdentifier).mkString(",")}"))
      assert(workbookEntry.readOptions.sheetName.contains("Contacts"))
      assert(workbookEntry.readOptions.excelMaxRowsInMemory.contains(4096))
      assert(workbookEntry.readOptions.excelByteArrayMaxOverride.contains(123456789))
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

  private def captureStderr(block: => Unit): String = {
    val output = new ByteArrayOutputStream()
    val originalErr = System.err
    val captureErr = new PrintStream(output, true, StandardCharsets.UTF_8.name())
    try {
      System.setErr(captureErr)
      block
    } finally {
      captureErr.flush()
      System.setErr(originalErr)
    }
    output.toString(StandardCharsets.UTF_8.name())
  }

  private def withWarnLogging[A](block: => A): A = {
    val previous = sys.props.get(DriverLogger.PropertyName)
    DriverLogger.resetCache()
    System.setProperty(DriverLogger.PropertyName, "warn")
    try {
      block
    } finally {
      previous match {
        case Some(value) => System.setProperty(DriverLogger.PropertyName, value)
        case None => System.clearProperty(DriverLogger.PropertyName)
      }
      DriverLogger.resetCache()
    }
  }
}
