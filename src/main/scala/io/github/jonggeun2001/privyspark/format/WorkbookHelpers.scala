package io.github.jonggeun2001.privyspark.format

import org.apache.hadoop.fs.Path
import org.apache.poi.ss.usermodel.WorkbookFactory

import scala.util.control.NonFatal

private[privyspark] object WorkbookHelpers {
  def workbookDataAddress(sheetName: String): String = {
    s"'${sheetName.replace("'", "''")}'!A1"
  }

  private def sheetHasContent(sheet: org.apache.poi.ss.usermodel.Sheet): Boolean = {
    val rowIterator = sheet.rowIterator()
    while (rowIterator.hasNext) {
      val row = rowIterator.next()
      val cellIterator = row.cellIterator()
      while (cellIterator.hasNext) {
        val cell = cellIterator.next()
        if (cell != null && cell.toString.trim.nonEmpty) {
          return true
        }
      }
    }
    false
  }

  def listVisibleWorkbookSheets(
    conf: org.apache.hadoop.conf.Configuration,
    filePath: String
  ): Either[String, Seq[String]] = {
    val sourcePath = new Path(filePath)
    val fs = sourcePath.getFileSystem(conf)
    val inputStream = fs.open(sourcePath)
    try {
      val workbook = WorkbookFactory.create(inputStream)
      try {
        val sheetNames = (0 until workbook.getNumberOfSheets).flatMap { index =>
          val hidden = workbook.isSheetHidden(index) || workbook.isSheetVeryHidden(index)
          val sheet = workbook.getSheetAt(index)
          if (!hidden && sheetHasContent(sheet)) Some(sheet.getSheetName) else None
        }
        if (sheetNames.nonEmpty) {
          Right(sheetNames)
        } else {
          Left("No non-empty visible sheets found")
        }
      } finally {
        workbook.close()
      }
    } catch {
      case NonFatal(e) =>
        Left(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
    } finally {
      inputStream.close()
    }
  }
}
