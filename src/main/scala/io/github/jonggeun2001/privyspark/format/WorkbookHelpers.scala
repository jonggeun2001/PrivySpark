package io.github.jonggeun2001.privyspark.format

import org.apache.hadoop.fs.Path

import java.io.InputStream
import java.util.zip.ZipInputStream
import javax.xml.stream.{XMLInputFactory, XMLStreamConstants}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

private[privyspark] object WorkbookHelpers {
  private val WorkbookXmlEntry = "xl/workbook.xml"

  def workbookDataAddress(sheetName: String): String = {
    s"'${sheetName.replace("'", "''")}'!A1"
  }

  def listVisibleWorkbookSheets(
    conf: org.apache.hadoop.conf.Configuration,
    filePath: String
  ): Either[String, Seq[String]] = {
    val sourcePath = new Path(filePath)
    val fs = sourcePath.getFileSystem(conf)
    val inputStream = fs.open(sourcePath)
    val zipInputStream = new ZipInputStream(inputStream)
    try {
      var entry = zipInputStream.getNextEntry
      while (entry != null) {
        if (!entry.isDirectory && normalizeEntryName(entry.getName) == WorkbookXmlEntry) {
          val sheetNames = readVisibleSheetNames(zipInputStream)
          if (sheetNames.nonEmpty) {
            return Right(sheetNames)
          } else {
            return Left("No visible sheets found")
          }
        }
        zipInputStream.closeEntry()
        entry = zipInputStream.getNextEntry
      }
      Left("Workbook metadata not found")
    } catch {
      case NonFatal(e) =>
        Left(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
    } finally {
      zipInputStream.close()
    }
  }

  private def normalizeEntryName(name: String): String =
    Option(name).getOrElse("").stripPrefix("/")

  private def readVisibleSheetNames(inputStream: InputStream): Seq[String] = {
    val factory = XMLInputFactory.newFactory()
    disableXmlExternalEntities(factory)
    val reader = factory.createXMLStreamReader(inputStream)
    val sheetNames = ArrayBuffer.empty[String]
    try {
      while (reader.hasNext) {
        if (reader.next() == XMLStreamConstants.START_ELEMENT && reader.getLocalName == "sheet") {
          val name = Option(reader.getAttributeValue(null, "name")).getOrElse("")
          val state = Option(reader.getAttributeValue(null, "state")).map(_.trim.toLowerCase).getOrElse("")
          if (name.nonEmpty && state != "hidden" && state != "veryhidden") {
            sheetNames += name
          }
        }
      }
    } finally {
      reader.close()
    }
    sheetNames.toSeq
  }

  private def disableXmlExternalEntities(factory: XMLInputFactory): Unit = {
    try {
      factory.setProperty(XMLInputFactory.SUPPORT_DTD, false)
    } catch {
      case _: IllegalArgumentException =>
    }
    try {
      factory.setProperty("javax.xml.stream.isSupportingExternalEntities", false)
    } catch {
      case _: IllegalArgumentException =>
    }
  }
}
