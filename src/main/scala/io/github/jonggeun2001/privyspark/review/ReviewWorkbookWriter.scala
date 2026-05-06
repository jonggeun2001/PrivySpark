package io.github.jonggeun2001.privyspark.review

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.poi.ss.usermodel.{BorderStyle, Cell, CellStyle, FillPatternType, HorizontalAlignment, IndexedColors, Row, VerticalAlignment}
import org.apache.poi.ss.util.CellRangeAddressList
import org.apache.poi.xssf.usermodel.{XSSFCellStyle, XSSFColor, XSSFWorkbook, XSSFWorkbookType}

import java.awt.Color
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.util.zip.{ZipEntry, ZipInputStream, ZipOutputStream}
import scala.collection.mutable
import scala.util.matching.Regex

private[privyspark] object ReviewWorkbookWriter {
  private val VbaProjectResource = "/review/vbaProject.bin"

  def write(
    conf: Configuration,
    workbookPath: Path,
    scanPath: String,
    scanResultsFingerprint: String,
    findings: Seq[ReviewFinding],
    sampleMode: String,
    actionPlanStates: Map[String, ReviewActionPlanStatus]
  ): Unit = {
    val workbook = new XSSFWorkbook(XSSFWorkbookType.XLSM)
    try {
      withResource(VbaProjectResource)(workbook.setVBAProject)
      val styles = WorkbookStyles(workbook)
      val reviewSheet = workbook.createSheet(ReviewWorkbookLayout.ReviewSheetName)
      val metadataSheet = workbook.createSheet(ReviewWorkbookLayout.MetadataSheetName)
      workbook.setSheetHidden(workbook.getSheetIndex(metadataSheet), true)

      writeMetadata(metadataSheet, scanPath, scanResultsFingerprint)
      writeIntroRows(reviewSheet, scanPath, styles)
      writeHeader(reviewSheet, styles)
      writeFindings(reviewSheet, findings, sampleMode, actionPlanStates, styles)
      configureSheet(reviewSheet, findings.size)

      val fs = workbookPath.getFileSystem(conf)
      Option(workbookPath.getParent).foreach(fs.mkdirs)
      val outputStream = fs.create(workbookPath, true)
      try {
        val workbookBytes = new ByteArrayOutputStream()
        workbook.write(workbookBytes)
        outputStream.write(ReviewWorkbookPackage.addJsonExportButton(workbookBytes.toByteArray))
      } finally {
        outputStream.close()
      }
    } finally {
      workbook.close()
    }
  }

  private def writeMetadata(sheet: org.apache.poi.ss.usermodel.Sheet, scanPath: String, scanResultsFingerprint: String): Unit = {
    writeKeyValue(sheet, 0, "schema_version", "1")
    writeKeyValue(sheet, 1, "scan_path", scanPath)
    writeKeyValue(sheet, 2, "scan_results_fingerprint", scanResultsFingerprint)
    writeKeyValue(sheet, 3, "response_format", "review-xlsm")
  }

  private def writeIntroRows(sheet: org.apache.poi.ss.usermodel.Sheet, scanPath: String, styles: WorkbookStyles): Unit = {
    val titleRow = sheet.createRow(0)
    val title = titleRow.createCell(0)
    title.setCellValue("PrivySpark Review")
    title.setCellStyle(styles.title)

    writeLabelValue(sheet.createRow(1), "Scan path", scanPath, styles)
    writeLabelValue(sheet.createRow(ReviewWorkbookLayout.ResponderRowIndex), "응답자", "", styles)
    sheet.getRow(ReviewWorkbookLayout.ResponderRowIndex).getCell(ReviewWorkbookLayout.ResponderColumnIndex).setCellStyle(styles.input)

    val guide = sheet.createRow(3).createCell(0)
    guide.setCellValue("판정은 오탐 또는 정탐 중 하나를 선택합니다. 오탐은 오탐 사유, 정탐은 정탐 조치 계획과 조치 예정일을 입력한 뒤 review.json 생성 버튼으로 만든 JSON 파일을 review-state-root/inbox에 넣고 review collect를 실행합니다.")
    guide.setCellStyle(styles.guide)
  }

  private def writeHeader(sheet: org.apache.poi.ss.usermodel.Sheet, styles: WorkbookStyles): Unit = {
    val headerRow = sheet.createRow(ReviewWorkbookLayout.HeaderRowIndex)
    ReviewWorkbookLayout.Columns.zipWithIndex.foreach { case (column, index) =>
      val cell = headerRow.createCell(index)
      cell.setCellValue(column.header)
      cell.setCellStyle(styles.header)
      sheet.setColumnWidth(index, column.width * 256)
      sheet.setColumnHidden(index, column.hidden)
    }
  }

  private def writeFindings(
    sheet: org.apache.poi.ss.usermodel.Sheet,
    findings: Seq[ReviewFinding],
    sampleMode: String,
    actionPlanStates: Map[String, ReviewActionPlanStatus],
    styles: WorkbookStyles
  ): Unit = {
    findings.zipWithIndex.foreach { case (finding, offset) =>
      val row = sheet.createRow(ReviewWorkbookLayout.FirstDataRowIndex + offset)
      row.setHeightInPoints(72)
      val actionStatus = actionPlanStates.get(finding.findingKey)
      writeCell(row, 0, finding.fileIdentifier, styles.text)
      writeCell(row, 1, finding.hiveTableFqn, styles.text)
      writeCell(row, 2, finding.columnName, styles.text)
      writeCell(row, 3, displayPiiType(finding.piiType), styles.text)
      writeNumberCell(row, 4, finding.sampledRowCount.toDouble, styles.integer)
      writeNumberCell(row, 5, finding.matchCount.toDouble, styles.integer)
      writeNumberCell(row, 6, finding.nonEmptyMatchRatio * 100.0, styles.percent)
      writeCell(row, 7, ReviewSampleMasker.evidenceSampleText(finding, sampleMode), styles.sample)
      writeCell(row, 8, "", styles.input)
      writeCell(row, 9, actionStatus.map(_.statusLabel).getOrElse(""), styles.text)
      writeCell(row, 10, "", styles.input)
      writeCell(row, 11, "", styles.input)
      writeCell(row, 12, "", styles.dateInput)
      writeHiddenFields(row, finding)
    }
  }

  private def writeHiddenFields(row: Row, finding: ReviewFinding): Unit = {
    writeHidden(row, "scan_path", finding.scanPath)
    writeHidden(row, "finding_key", finding.findingKey)
    writeHidden(row, "finding_hash", finding.findingHash)
    writeHidden(row, "file_identifier", finding.fileIdentifier)
    writeHidden(row, "hive_database", finding.hiveDatabase)
    writeHidden(row, "hive_table", finding.hiveTable)
    writeHidden(row, "hive_table_fqn", finding.hiveTableFqn)
    writeHidden(row, "column_name", finding.columnName)
    writeHidden(row, "pii_type", finding.piiType)
    writeHidden(row, "sample_row_count_raw", finding.sampledRowCount.toString)
    writeHidden(row, "match_count_raw", finding.matchCount.toString)
    writeHidden(row, "non_empty_match_ratio_raw", finding.nonEmptyMatchRatio.toString)
  }

  private def configureSheet(sheet: org.apache.poi.ss.usermodel.Sheet, findingCount: Int): Unit = {
    sheet.createFreezePane(0, ReviewWorkbookLayout.FirstDataRowIndex)
    sheet.setAutoFilter(new org.apache.poi.ss.util.CellRangeAddress(
      ReviewWorkbookLayout.HeaderRowIndex,
      ReviewWorkbookLayout.HeaderRowIndex,
      0,
      ReviewWorkbookLayout.Columns.size - 1
    ))
    if (findingCount > 0) {
      val lastRow = ReviewWorkbookLayout.FirstDataRowIndex + findingCount - 1
      val helper = sheet.getDataValidationHelper
      val decisionConstraint = helper.createExplicitListConstraint(Array("오탐", "정탐"))
      val decisionRange = new CellRangeAddressList(
        ReviewWorkbookLayout.FirstDataRowIndex,
        lastRow,
        ReviewWorkbookLayout.DecisionColumnIndex,
        ReviewWorkbookLayout.DecisionColumnIndex
      )
      val decisionValidation = helper.createValidation(decisionConstraint, decisionRange)
      decisionValidation.setShowErrorBox(true)
      decisionValidation.createErrorBox("판정 입력 오류", "오탐 또는 정탐만 선택할 수 있습니다.")
      sheet.addValidationData(decisionValidation)
    }
  }

  private def writeLabelValue(row: Row, label: String, value: String, styles: WorkbookStyles): Unit = {
    writeCell(row, 0, label, styles.label)
    writeCell(row, 1, value, styles.text)
  }

  private def writeKeyValue(sheet: org.apache.poi.ss.usermodel.Sheet, rowIndex: Int, key: String, value: String): Unit = {
    val row = sheet.createRow(rowIndex)
    row.createCell(0).setCellValue(key)
    row.createCell(1).setCellValue(value)
  }

  private def writeHidden(row: Row, field: String, value: String): Unit =
    ReviewWorkbookLayout.HiddenColumnIndexByField.get(field).foreach(index => row.createCell(index).setCellValue(value))

  private def writeCell(row: Row, index: Int, value: String, style: CellStyle): Cell = {
    val cell = row.createCell(index)
    cell.setCellValue(Option(value).getOrElse(""))
    cell.setCellStyle(style)
    cell
  }

  private def writeNumberCell(row: Row, index: Int, value: Double, style: CellStyle): Cell = {
    val cell = row.createCell(index)
    cell.setCellValue(value)
    cell.setCellStyle(style)
    cell
  }

  private def displayPiiType(value: String): String =
    Map(
      "phone_number" -> "전화번호",
      "email" -> "이메일",
      "resident_registration_number" -> "주민등록번호",
      "foreign_registration_number" -> "외국인등록번호",
      "driver_license_number" -> "운전면허번호",
      "address" -> "주소",
      "bank_account_number" -> "계좌번호",
      "credit_card_number" -> "신용카드번호",
      "passport_number" -> "여권번호",
      "ip_address" -> "IP 주소"
    ).getOrElse(value, value)

  private def withResource[T](resourcePath: String)(use: InputStream => T): T = {
    val stream = Option(getClass.getResourceAsStream(resourcePath))
      .getOrElse(throw new IllegalStateException(s"Missing review workbook resource: $resourcePath"))
    try {
      use(stream)
    } finally {
      stream.close()
    }
  }

  private object ReviewWorkbookPackage {
    private val WorksheetPath = "xl/worksheets/sheet1.xml"
    private val WorksheetRelsPath = "xl/worksheets/_rels/sheet1.xml.rels"
    private val VmlPath = "xl/drawings/vmlDrawing1.vml"
    private val ContentTypesPath = "[Content_Types].xml"
    private val RelationshipsNamespace = "http://schemas.openxmlformats.org/package/2006/relationships"
    private val OfficeRelationshipsNamespace = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
    private val VmlRelationshipType = s"$OfficeRelationshipsNamespace/vmlDrawing"
    private val SpreadsheetNamespace = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    private val OfficeDocumentRelationshipsNamespace = OfficeRelationshipsNamespace
    private val RelationshipIdPattern: Regex = """Id="rId([0-9]+)"""".r

    def addJsonExportButton(workbookBytes: Array[Byte]): Array[Byte] = {
      val entries = readZipEntries(workbookBytes)
      val sheetRelationshipId = appendVmlRelationship(entries)
      entries.update(WorksheetPath, addLegacyDrawing(entries(WorksheetPath), sheetRelationshipId))
      entries.update(ContentTypesPath, addVmlContentType(entries(ContentTypesPath)))
      entries.update(VmlPath, vmlDrawingXml.getBytes(StandardCharsets.UTF_8))
      writeZipEntries(entries)
    }

    private def appendVmlRelationship(entries: mutable.LinkedHashMap[String, Array[Byte]]): String = {
      val existing = entries.get(WorksheetRelsPath).map(bytes => new String(bytes, StandardCharsets.UTF_8))
      val relationshipId = nextRelationshipId(existing.getOrElse(""))
      val relationship =
        s"""<Relationship Id="$relationshipId" Type="$VmlRelationshipType" Target="../drawings/vmlDrawing1.vml"/>"""
      val updated = existing match {
        case Some(xml) if xml.contains(VmlRelationshipType) => xml
        case Some(xml) => xml.replace("</Relationships>", relationship + "</Relationships>")
        case None =>
          s"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="$RelationshipsNamespace">$relationship</Relationships>"""
      }
      entries.update(WorksheetRelsPath, updated.getBytes(StandardCharsets.UTF_8))
      relationshipId
    }

    private def nextRelationshipId(xml: String): String = {
      val maxId = RelationshipIdPattern.findAllMatchIn(xml).map(_.group(1).toInt).toSeq.foldLeft(0)(math.max)
      s"rId${maxId + 1}"
    }

    private def addLegacyDrawing(sheetBytes: Array[Byte], relationshipId: String): Array[Byte] = {
      val original = new String(sheetBytes, StandardCharsets.UTF_8)
      val withRelationshipNamespace =
        if (original.contains("xmlns:r=")) {
          original
        } else {
          original.replace(
            s"""<worksheet xmlns="$SpreadsheetNamespace"""",
            s"""<worksheet xmlns="$SpreadsheetNamespace" xmlns:r="$OfficeDocumentRelationshipsNamespace""""
          )
        }
      val updated =
        if (withRelationshipNamespace.contains("<legacyDrawing ")) {
          withRelationshipNamespace
        } else {
          withRelationshipNamespace.replace("</worksheet>", s"""<legacyDrawing r:id="$relationshipId"/></worksheet>""")
        }
      updated.getBytes(StandardCharsets.UTF_8)
    }

    private def addVmlContentType(contentTypesBytes: Array[Byte]): Array[Byte] = {
      val original = new String(contentTypesBytes, StandardCharsets.UTF_8)
      val updated =
        if (original.contains("""Extension="vml"""")) {
          original
        } else {
          original.replace(
            "</Types>",
            """<Default Extension="vml" ContentType="application/vnd.openxmlformats-officedocument.vmlDrawing"/></Types>"""
          )
        }
      updated.getBytes(StandardCharsets.UTF_8)
    }

    private def vmlDrawingXml: String =
      """<xml xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office" xmlns:x="urn:schemas-microsoft-com:office:excel"><o:shapelayout v:ext="edit"><o:idmap v:ext="edit" data="1"/></o:shapelayout><v:shapetype id="_x0000_t201" coordsize="21600,21600" o:spt="201" path="m,l,21600r21600,l21600,xe"><v:stroke joinstyle="miter"/><v:path shadowok="f" o:extrusionok="f" strokeok="f" fillok="f" o:connecttype="rect"/><o:lock v:ext="edit" shapetype="t"/></v:shapetype><v:shape id="_x0000_s1025" type="#_x0000_t201" style="position:absolute;margin-left:8pt;margin-top:38pt;width:110pt;height:24pt;z-index:1;mso-wrap-style:tight" o:button="t" fillcolor="buttonFace [67]" strokecolor="windowText [64]" o:insetmode="auto"><v:fill color2="buttonFace [67]" o:detectmouseclick="t"/><o:lock v:ext="edit" rotation="t"/><v:textbox style="mso-direction-alt:auto" o:singleclick="f"><div style="text-align:center"><font face="Calibri" size="220" color="#000000">review.json 생성</font></div></v:textbox><x:ClientData ObjectType="Button"><x:Anchor>0, 8, 2, 6, 1, 78, 3, 12</x:Anchor><x:PrintObject>False</x:PrintObject><x:AutoFill>False</x:AutoFill><x:FmlaMacro>[0]!say_hello</x:FmlaMacro><x:TextHAlign>Center</x:TextHAlign><x:TextVAlign>Center</x:TextVAlign></x:ClientData></v:shape></xml>"""

    private def readZipEntries(bytes: Array[Byte]): mutable.LinkedHashMap[String, Array[Byte]] = {
      val entries = mutable.LinkedHashMap.empty[String, Array[Byte]]
      val zip = new ZipInputStream(new ByteArrayInputStream(bytes))
      try {
        var entry = zip.getNextEntry
        while (entry != null) {
          entries.update(entry.getName, readCurrentEntry(zip))
          zip.closeEntry()
          entry = zip.getNextEntry
        }
      } finally {
        zip.close()
      }
      entries
    }

    private def readCurrentEntry(inputStream: InputStream): Array[Byte] = {
      val output = new ByteArrayOutputStream()
      val buffer = new Array[Byte](8192)
      var read = inputStream.read(buffer)
      while (read >= 0) {
        if (read > 0) {
          output.write(buffer, 0, read)
        }
        read = inputStream.read(buffer)
      }
      output.toByteArray
    }

    private def writeZipEntries(entries: mutable.LinkedHashMap[String, Array[Byte]]): Array[Byte] = {
      val output = new ByteArrayOutputStream()
      val zip = new ZipOutputStream(output)
      try {
        entries.foreach { case (name, bytes) =>
          zip.putNextEntry(new ZipEntry(name))
          zip.write(bytes)
          zip.closeEntry()
        }
      } finally {
        zip.close()
      }
      output.toByteArray
    }
  }

  private final case class WorkbookStyles(
    title: CellStyle,
    guide: CellStyle,
    header: CellStyle,
    label: CellStyle,
    text: CellStyle,
    input: CellStyle,
    dateInput: CellStyle,
    sample: CellStyle,
    integer: CellStyle,
    percent: CellStyle
  )

  private object WorkbookStyles {
    def apply(workbook: XSSFWorkbook): WorkbookStyles = {
      val titleFont = workbook.createFont()
      titleFont.setBold(true)
      titleFont.setFontHeightInPoints(16)

      val headerFont = workbook.createFont()
      headerFont.setBold(true)

      val title = workbook.createCellStyle()
      title.setFont(titleFont)

      val guide = wrap(workbook.createCellStyle())

      val header = bordered(workbook.createCellStyle()).asInstanceOf[XSSFCellStyle]
      header.setFont(headerFont)
      header.setFillForegroundColor(new XSSFColor(new Color(244, 246, 247), null))
      header.setFillPattern(FillPatternType.SOLID_FOREGROUND)
      header.setAlignment(HorizontalAlignment.CENTER)
      header.setVerticalAlignment(VerticalAlignment.CENTER)

      val label = bordered(workbook.createCellStyle())
      label.setFont(headerFont)

      val text = wrap(bordered(workbook.createCellStyle()))

      val input = wrap(bordered(workbook.createCellStyle())).asInstanceOf[XSSFCellStyle]
      input.setFillForegroundColor(new XSSFColor(new Color(255, 248, 225), null))
      input.setFillPattern(FillPatternType.SOLID_FOREGROUND)

      val dateInput = workbook.createCellStyle()
      dateInput.cloneStyleFrom(input)
      dateInput.setDataFormat(workbook.createDataFormat().getFormat("yyyy-mm-dd"))

      val sample = wrap(bordered(workbook.createCellStyle()))
      sample.setVerticalAlignment(VerticalAlignment.TOP)

      val integer = bordered(workbook.createCellStyle())
      integer.setDataFormat(workbook.createDataFormat().getFormat("#,##0"))
      integer.setAlignment(HorizontalAlignment.RIGHT)

      val percent = bordered(workbook.createCellStyle())
      percent.setDataFormat(workbook.createDataFormat().getFormat("0.00"))
      percent.setAlignment(HorizontalAlignment.RIGHT)

      WorkbookStyles(title, guide, header, label, text, input, dateInput, sample, integer, percent)
    }

    private def wrap(style: CellStyle): CellStyle = {
      style.setWrapText(true)
      style.setVerticalAlignment(VerticalAlignment.TOP)
      style
    }

    private def bordered(style: CellStyle): CellStyle = {
      style.setBorderTop(BorderStyle.THIN)
      style.setBorderRight(BorderStyle.THIN)
      style.setBorderBottom(BorderStyle.THIN)
      style.setBorderLeft(BorderStyle.THIN)
      style.setTopBorderColor(IndexedColors.GREY_25_PERCENT.getIndex)
      style.setRightBorderColor(IndexedColors.GREY_25_PERCENT.getIndex)
      style.setBottomBorderColor(IndexedColors.GREY_25_PERCENT.getIndex)
      style.setLeftBorderColor(IndexedColors.GREY_25_PERCENT.getIndex)
      style
    }
  }
}
