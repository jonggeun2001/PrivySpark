package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.format.{CsvInference, WorkbookHelpers, WorkbookSheetStreamer}
import io.github.jonggeun2001.privyspark.model.ScanReadOptions
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.xssf.streaming.SXSSFWorkbook
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class WorkbookSheetStreamerSpec extends AnyFunSuite with BeforeAndAfterAll {
  private val spark = SparkSession.builder()
    .appName("WorkbookSheetStreamerSpec")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("streams workbook rows with spark-excel compatible string values") {
    val tempDir = Files.createTempDirectory("privyspark-workbook-streamer-")
    val workbookPath = tempDir.resolve("contacts.xlsx")

    try {
      writeWorkbook(workbookPath) { workbook =>
        val sheet = workbook.createSheet("Contacts")
        val header = sheet.createRow(0)
        header.createCell(0).setCellValue("email")
        header.createCell(1).setCellValue("score")
        header.createCell(2).setCellValue("active")
        header.createCell(3).setCellValue("note")

        val first = sheet.createRow(1)
        first.createCell(0).setCellValue("alice@example.com")
        first.createCell(1).setCellValue(42.5)
        first.createCell(2).setCellValue(true)
        first.createCell(3).setCellValue("shared")

        val second = sheet.createRow(2)
        second.createCell(0).setCellValue("bob@example.com")
        second.createCell(2).setCellValue(false)
      }

      val expected = collectRows(sparkExcelRead(workbookPath, "Contacts"))
      val actual = collectRows(WorkbookSheetStreamer.readSheetDataFrame(spark, workbookPath.toString, "Contacts"))

      assert(actual == expected)
    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("normalizes blank and duplicate headers and drops cells beyond header width") {
    val tempDir = Files.createTempDirectory("privyspark-workbook-streamer-headers-")
    val workbookPath = tempDir.resolve("headers.xlsx")

    try {
      writeWorkbook(workbookPath) { workbook =>
        val sheet = workbook.createSheet("MixedHeaders")
        val header = sheet.createRow(0)
        header.createCell(0).setCellValue("")
        header.createCell(1).setCellValue("email")
        header.createCell(2).setCellValue("email")

        val row = sheet.createRow(1)
        row.createCell(0).setCellValue("id-1")
        row.createCell(1).setCellValue("alice@example.com")
        row.createCell(2).setCellValue("alice.alt@example.com")
        row.createCell(5).setCellValue("ignored")
      }

      val df = WorkbookSheetStreamer.readSheetDataFrame(spark, workbookPath.toString, "MixedHeaders")

      assert(df.schema.fieldNames.toSeq == Seq("_c0", "email1", "email2"))
      assert(collectRows(df) == Seq(Seq("id-1", "alice@example.com", "alice.alt@example.com")))
    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("preserves header column gaps instead of compressing xlsx columns") {
    val tempDir = Files.createTempDirectory("privyspark-workbook-streamer-header-gaps-")
    val workbookPath = tempDir.resolve("header-gaps.xlsx")

    try {
      writeHeaderGapWorkbook(workbookPath)

      val missingA = WorkbookSheetStreamer.readSheetDataFrame(spark, workbookPath.toString, "MissingA")
      val missingB = WorkbookSheetStreamer.readSheetDataFrame(spark, workbookPath.toString, "MissingB")

      assert(missingA.schema.fieldNames.toSeq == Seq("_c0", "email", "phone"))
      assert(collectRows(missingA) == Seq(Seq(null, "alice@example.com", "010-1234-5678")))
      assert(missingB.schema.fieldNames.toSeq == Seq("email", "_c1", "phone"))
      assert(collectRows(missingB) == Seq(Seq("alice@example.com", null, "010-1234-5678")))
    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("formats numeric cells with workbook display styles") {
    val tempDir = Files.createTempDirectory("privyspark-workbook-streamer-formats-")
    val workbookPath = tempDir.resolve("formats.xlsx")

    try {
      writeWorkbook(workbookPath) { workbook =>
        val dataFormat = workbook.createDataFormat()
        val phoneStyle = workbook.createCellStyle()
        phoneStyle.setDataFormat(dataFormat.getFormat("000-0000-0000"))
        val dateStyle = workbook.createCellStyle()
        dateStyle.setDataFormat(dataFormat.getFormat("yyyy-mm-dd"))

        val sheet = workbook.createSheet("Formats")
        val header = sheet.createRow(0)
        header.createCell(0).setCellValue("phone")
        header.createCell(1).setCellValue("birth_date")

        val row = sheet.createRow(1)
        val phone = row.createCell(0)
        phone.setCellValue(1012345678d)
        phone.setCellStyle(phoneStyle)
        val date = row.createCell(1)
        date.setCellValue(java.sql.Date.valueOf("2026-04-24"))
        date.setCellStyle(dateStyle)
      }

      val expected = collectRows(sparkExcelRead(workbookPath, "Formats"))
      val actual = collectRows(WorkbookSheetStreamer.readSheetDataFrame(spark, workbookPath.toString, "Formats"))

      assert(actual == expected)
      assert(actual == Seq(Seq("010-1234-5678", "2026-04-24")))
    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("formats date cells with workbook 1904 date windowing") {
    val tempDir = Files.createTempDirectory("privyspark-workbook-streamer-date1904-")
    val workbookPath = tempDir.resolve("date1904.xlsx")

    try {
      writeWorkbook(workbookPath) { workbook =>
        val workbookPr =
          if (workbook.getCTWorkbook.isSetWorkbookPr) workbook.getCTWorkbook.getWorkbookPr
          else workbook.getCTWorkbook.addNewWorkbookPr()
        workbookPr.setDate1904(true)

        val dataFormat = workbook.createDataFormat()
        val dateStyle = workbook.createCellStyle()
        dateStyle.setDataFormat(dataFormat.getFormat("yyyy-mm-dd"))

        val sheet = workbook.createSheet("Dates")
        val header = sheet.createRow(0)
        header.createCell(0).setCellValue("birth_date")

        val row = sheet.createRow(1)
        val date = row.createCell(0)
        date.setCellValue(java.sql.Date.valueOf("2026-04-24"))
        date.setCellStyle(dateStyle)
      }

      val expected = collectRows(sparkExcelRead(workbookPath, "Dates"))
      val actual = collectRows(WorkbookSheetStreamer.readSheetDataFrame(spark, workbookPath.toString, "Dates"))

      assert(actual == expected)
      assert(actual == Seq(Seq("2026-04-24")))
    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("returns schema-only dataframe when a sheet has no data rows") {
    val tempDir = Files.createTempDirectory("privyspark-workbook-streamer-empty-")
    val workbookPath = tempDir.resolve("empty.xlsx")

    try {
      writeWorkbook(workbookPath) { workbook =>
        val sheet = workbook.createSheet("EmptyData")
        val header = sheet.createRow(0)
        header.createCell(0).setCellValue("email")
      }

      val df = WorkbookSheetStreamer.readSheetDataFrame(spark, workbookPath.toString, "EmptyData")

      assert(df.schema.fieldNames.toSeq == Seq("email"))
      assert(df.count() == 0L)
    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("reports missing sheets with workbook schema error message") {
    val tempDir = Files.createTempDirectory("privyspark-workbook-streamer-missing-")
    val workbookPath = tempDir.resolve("missing.xlsx")

    try {
      writeWorkbook(workbookPath) { workbook =>
        val sheet = workbook.createSheet("Contacts")
        sheet.createRow(0).createCell(0).setCellValue("email")
      }

      val error = intercept[IllegalArgumentException] {
        WorkbookSheetStreamer.readSheetDataFrame(spark, workbookPath.toString, "Missing")
      }

      assert(error.getMessage == "Sheet not found: Missing")
    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("streams inline string and formula string cells from worksheet XML") {
    val tempDir = Files.createTempDirectory("privyspark-workbook-streamer-inline-")
    val workbookPath = tempDir.resolve("inline.xlsx")

    try {
      writeInlineWorkbook(workbookPath)

      val df = WorkbookSheetStreamer.readSheetDataFrame(spark, workbookPath.toString, "Inline")

      assert(df.schema.fieldNames.toSeq == Seq("name", "formula_text", "active"))
      assert(collectRows(df) == Seq(Seq("Alice", "computed", "TRUE")))
    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("streams ten thousand rows from an SXSSFWorkbook fixture") {
    val tempDir = Files.createTempDirectory("privyspark-workbook-streamer-smoke-")
    val workbookPath = tempDir.resolve("large.xlsx")

    try {
      writeStreamingWorkbook(workbookPath, rows = 10000)

      val df = WorkbookSheetStreamer.readSheetDataFrame(spark, workbookPath.toString, "Rows")

      assert(df.count() == 10000L)
      assert(collectRows(df.orderBy("id").limit(1)) == Seq(Seq("00001", "user00001@example.com")))
    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("CsvInference reads xlsx paths containing spaces and glob-special characters") {
    val tempDir = Files.createTempDirectory("privyspark-workbook-streamer-special-")
    val workbookPath = tempDir.resolve("customer list [final] #1.xlsx")

    try {
      writeWorkbook(workbookPath) { workbook =>
        val sheet = workbook.createSheet("Contacts")
        val header = sheet.createRow(0)
        header.createCell(0).setCellValue("email")
        val row = sheet.createRow(1)
        row.createCell(0).setCellValue("alice@example.com")
      }

      val df = CsvInference.readSource(
        spark,
        CsvInference.XlsxFormat,
        Seq(workbookPath.toString),
        readOptions = ScanReadOptions(sheetName = Some("Contacts"))
      )

      assert(collectRows(df) == Seq(Seq("alice@example.com")))
    } finally {
      deleteRecursively(tempDir)
    }
  }

  private def sparkExcelRead(path: Path, sheetName: String): DataFrame = {
    spark.read
      .format("com.crealytics.spark.excel")
      .option("header", "true")
      .option("inferSchema", "false")
      .option("dataAddress", WorkbookHelpers.workbookDataAddress(sheetName))
      .load(path.toString)
  }

  private def collectRows(df: DataFrame): Seq[Seq[String]] = {
    df.collect().toSeq.map { row =>
      row.toSeq.map {
        case null => null
        case value => value.toString
      }
    }
  }

  private def writeWorkbook(path: Path)(populate: XSSFWorkbook => Unit): Unit = {
    val workbook = new XSSFWorkbook()
    try {
      populate(workbook)
      val outputStream = Files.newOutputStream(path)
      try {
        workbook.write(outputStream)
      } finally {
        outputStream.close()
      }
    } finally {
      workbook.close()
    }
  }

  private def writeStreamingWorkbook(path: Path, rows: Int): Unit = {
    val workbook = new SXSSFWorkbook(100)
    try {
      val sheet = workbook.createSheet("Rows")
      val header = sheet.createRow(0)
      header.createCell(0).setCellValue("id")
      header.createCell(1).setCellValue("email")

      (1 to rows).foreach { index =>
        val row = sheet.createRow(index)
        row.createCell(0, CellType.STRING).setCellValue(f"$index%05d")
        row.createCell(1).setCellValue(f"user$index%05d@example.com")
      }

      val outputStream = Files.newOutputStream(path)
      try {
        workbook.write(outputStream)
      } finally {
        outputStream.close()
      }
    } finally {
      workbook.dispose()
      workbook.close()
    }
  }

  private def writeInlineWorkbook(path: Path): Unit = {
    val outputStream = new ZipOutputStream(Files.newOutputStream(path))
    try {
      addZipEntry(outputStream, "[Content_Types].xml", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        |<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
        |  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
        |  <Default Extension="xml" ContentType="application/xml"/>
        |  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
        |  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
        |</Types>""".stripMargin)
      addZipEntry(outputStream, "_rels/.rels", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        |<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
        |  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
        |</Relationships>""".stripMargin)
      addZipEntry(outputStream, "xl/workbook.xml", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        |<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
        |          xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
        |  <sheets>
        |    <sheet name="Inline" sheetId="1" r:id="rId1"/>
        |  </sheets>
        |</workbook>""".stripMargin)
      addZipEntry(outputStream, "xl/_rels/workbook.xml.rels", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        |<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
        |  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
        |</Relationships>""".stripMargin)
      addZipEntry(outputStream, "xl/worksheets/sheet1.xml", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        |<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
        |  <dimension ref="A1:C2"/>
        |  <sheetData>
        |    <row r="1">
        |      <c r="A1" t="inlineStr"><is><t>name</t></is></c>
        |      <c r="B1" t="inlineStr"><is><t>formula_text</t></is></c>
        |      <c r="C1" t="inlineStr"><is><t>active</t></is></c>
        |    </row>
        |    <row r="2">
        |      <c r="A2" t="inlineStr"><is><t>Alice</t></is></c>
        |      <c r="B2" t="str"><f>CONCAT("computed")</f><v>computed</v></c>
        |      <c r="C2" t="b"><v>1</v></c>
        |    </row>
        |  </sheetData>
        |</worksheet>""".stripMargin)
    } finally {
      outputStream.close()
    }
  }

  private def writeHeaderGapWorkbook(path: Path): Unit = {
    val outputStream = new ZipOutputStream(Files.newOutputStream(path))
    try {
      addZipEntry(outputStream, "[Content_Types].xml", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        |<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
        |  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
        |  <Default Extension="xml" ContentType="application/xml"/>
        |  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
        |  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
        |  <Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
        |</Types>""".stripMargin)
      addZipEntry(outputStream, "_rels/.rels", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        |<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
        |  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
        |</Relationships>""".stripMargin)
      addZipEntry(outputStream, "xl/workbook.xml", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        |<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
        |          xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
        |  <sheets>
        |    <sheet name="MissingA" sheetId="1" r:id="rId1"/>
        |    <sheet name="MissingB" sheetId="2" r:id="rId2"/>
        |  </sheets>
        |</workbook>""".stripMargin)
      addZipEntry(outputStream, "xl/_rels/workbook.xml.rels", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        |<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
        |  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
        |  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/>
        |</Relationships>""".stripMargin)
      addZipEntry(outputStream, "xl/worksheets/sheet1.xml", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        |<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
        |  <sheetData>
        |    <row r="1">
        |      <c r="B1" t="inlineStr"><is><t>email</t></is></c>
        |      <c r="C1" t="inlineStr"><is><t>phone</t></is></c>
        |    </row>
        |    <row r="2">
        |      <c r="B2" t="inlineStr"><is><t>alice@example.com</t></is></c>
        |      <c r="C2" t="inlineStr"><is><t>010-1234-5678</t></is></c>
        |    </row>
        |  </sheetData>
        |</worksheet>""".stripMargin)
      addZipEntry(outputStream, "xl/worksheets/sheet2.xml", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        |<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
        |  <dimension ref="A1:C2"/>
        |  <sheetData>
        |    <row r="1">
        |      <c r="A1" t="inlineStr"><is><t>email</t></is></c>
        |      <c r="C1" t="inlineStr"><is><t>phone</t></is></c>
        |    </row>
        |    <row r="2">
        |      <c r="A2" t="inlineStr"><is><t>alice@example.com</t></is></c>
        |      <c r="C2" t="inlineStr"><is><t>010-1234-5678</t></is></c>
        |    </row>
        |  </sheetData>
        |</worksheet>""".stripMargin)
    } finally {
      outputStream.close()
    }
  }

  private def addZipEntry(outputStream: ZipOutputStream, name: String, content: String): Unit = {
    outputStream.putNextEntry(new ZipEntry(name))
    outputStream.write(content.getBytes(StandardCharsets.UTF_8))
    outputStream.closeEntry()
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      val stream = Files.walk(path)
      try {
        stream
          .iterator()
          .asScala
          .toSeq
          .reverse
          .foreach(Files.deleteIfExists)
      } finally {
        stream.close()
      }
    }
  }
}
