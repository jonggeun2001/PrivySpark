package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.format.WorkbookHelpers
import org.apache.hadoop.conf.Configuration
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.file.Files

@RunWith(classOf[JUnitRunner])
class WorkbookHelpersSpec extends AnyFunSuite {
  test("listVisibleWorkbookSheets reads sheet metadata without filtering empty visible sheets") {
    val tempDir = Files.createTempDirectory("privyspark-workbook-metadata-")
    val workbookPath = tempDir.resolve("contacts.xlsx")

    try {
      val workbook = new XSSFWorkbook()
      try {
        workbook.createSheet("Empty")
        val contacts = workbook.createSheet("Contacts")
        contacts.createRow(0).createCell(0).setCellValue("email")
        workbook.createSheet("  Spaced  ")
        workbook.createSheet("Hidden")
        workbook.setSheetHidden(workbook.getSheetIndex("Hidden"), true)

        val outputStream = Files.newOutputStream(workbookPath)
        try {
          workbook.write(outputStream)
        } finally {
          outputStream.close()
        }
      } finally {
        workbook.close()
      }

      val sheets = WorkbookHelpers.listVisibleWorkbookSheets(new Configuration(), workbookPath.toString)

      assert(sheets == Right(Seq("Empty", "Contacts", "  Spaced  ")))
    } finally {
      Files.deleteIfExists(workbookPath)
      Files.deleteIfExists(tempDir)
    }
  }
}
