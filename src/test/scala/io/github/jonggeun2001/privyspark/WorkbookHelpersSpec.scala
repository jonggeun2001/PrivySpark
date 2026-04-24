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

  test("inferWorkbookSheetSchemaSignature reads sheet headers from workbook XML") {
    val tempDir = Files.createTempDirectory("privyspark-workbook-schema-")
    val workbookPath = tempDir.resolve("contacts.xlsx")

    try {
      val workbook = new XSSFWorkbook()
      try {
        val contacts = workbook.createSheet("Contacts")
        val contactsHeader = contacts.createRow(0)
        contactsHeader.createCell(0).setCellValue("Email")
        contactsHeader.createCell(1).setCellValue("Phone")
        contacts.createRow(1).createCell(0).setCellValue("alice@example.com")

        val accounts = workbook.createSheet("Accounts")
        val accountsHeader = accounts.createRow(0)
        accountsHeader.createCell(0).setCellValue("account_id")
        accountsHeader.createCell(1).setCellValue("Status")
        accounts.createRow(1).createCell(0).setCellValue("acct-1")

        val empty = workbook.createSheet("Empty")
        empty.createRow(1).createCell(0).setCellValue("data_without_header")

        val outputStream = Files.newOutputStream(workbookPath)
        try {
          workbook.write(outputStream)
        } finally {
          outputStream.close()
        }
      } finally {
        workbook.close()
      }

      val conf = new Configuration()

      assert(WorkbookHelpers.inferWorkbookSheetSchemaSignature(conf, workbookPath.toString, "Contacts") == Right("email|phone"))
      assert(WorkbookHelpers.inferWorkbookSheetSchemaSignature(conf, workbookPath.toString, "Accounts") == Right("account_id|status"))
      assert(WorkbookHelpers.inferWorkbookSheetSchemaSignature(conf, workbookPath.toString, "Empty") == Left("head of empty list"))
      assert(WorkbookHelpers.inferWorkbookSheetSchemaSignature(conf, workbookPath.toString, "Missing") == Left("Sheet not found: Missing"))
    } finally {
      Files.deleteIfExists(workbookPath)
      Files.deleteIfExists(tempDir)
    }
  }
}
