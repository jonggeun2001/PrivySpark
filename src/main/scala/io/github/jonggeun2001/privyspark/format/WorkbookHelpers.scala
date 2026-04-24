package io.github.jonggeun2001.privyspark.format

import org.apache.hadoop.fs.Path

import java.io.InputStream
import java.net.URI
import java.util.zip.ZipInputStream
import javax.xml.stream.{XMLInputFactory, XMLStreamConstants}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

private[privyspark] object WorkbookHelpers {
  private val WorkbookXmlEntry = "xl/workbook.xml"
  private val WorkbookRelationshipsEntry = "xl/_rels/workbook.xml.rels"
  private val SharedStringsEntry = "xl/sharedStrings.xml"
  private val RelationshipsNamespace = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"

  private case class WorkbookSheet(name: String, state: String, relationshipId: Option[String]) {
    def visible: Boolean = state != "hidden" && state != "veryhidden"
  }

  private case class HeaderCell(columnIndex: Int, value: HeaderCellValue)

  private sealed trait HeaderCellValue
  private case class LiteralHeaderValue(value: String) extends HeaderCellValue
  private case class SharedStringHeaderValue(index: Int) extends HeaderCellValue

  def workbookDataAddress(sheetName: String): String = {
    s"'${sheetName.replace("'", "''")}'!A1"
  }

  def listVisibleWorkbookSheets(
    conf: org.apache.hadoop.conf.Configuration,
    filePath: String
  ): Either[String, Seq[String]] = {
    readWorkbookSheets(conf, filePath) match {
      case Right(sheets) =>
        val sheetNames = sheets.filter(_.visible).map(_.name)
        if (sheetNames.nonEmpty) {
          Right(sheetNames)
        } else {
          Left("No visible sheets found")
        }
      case Left(errorMessage) =>
        Left(errorMessage)
    }
  }

  def inferWorkbookSheetSchemaSignature(
    conf: org.apache.hadoop.conf.Configuration,
    filePath: String,
    sheetName: String
  ): Either[String, String] = {
    try {
      val sheetTarget = for {
        sheets <- readWorkbookSheets(conf, filePath).right
        sheet <- sheets.find(_.name == sheetName).toRight(s"Sheet not found: $sheetName").right
        relationshipId <- sheet.relationshipId.toRight(s"Sheet relationship not found: $sheetName").right
        relationships <- readWorkbookRelationships(conf, filePath).right
        rawTarget <- relationships.get(relationshipId).toRight(s"Worksheet target not found: $sheetName").right
      } yield resolveWorkbookPartTarget(rawTarget)

      sheetTarget.right.flatMap { targetEntry =>
        readSheetHeaderCells(conf, filePath, targetEntry).right.flatMap { headerCells =>
          if (headerCells.isEmpty) {
            Left("head of empty list")
          } else {
            val sharedStringIndexes = headerCells.collect {
              case HeaderCell(_, SharedStringHeaderValue(index)) => index
            }.toSet
            val sharedStrings =
              if (sharedStringIndexes.isEmpty) Right(Map.empty[Int, String])
              else readSharedStrings(conf, filePath, sharedStringIndexes)

            sharedStrings.right.flatMap { resolvedSharedStrings =>
              val headerNames = headerCells
                .sortBy(_.columnIndex)
                .map(cell => resolveHeaderCellValue(cell.value, resolvedSharedStrings))
                .filter(_.nonEmpty)

              if (headerNames.isEmpty) {
                Left("head of empty list")
              } else {
                Right(deduplicateHeaderNames(headerNames).map(_.toLowerCase).sorted.mkString("|"))
              }
            }
          }
        }
      }
    } catch {
      case NonFatal(e) =>
        Left(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
    }
  }

  private def normalizeEntryName(name: String): String =
    Option(name).getOrElse("").stripPrefix("/")

  private def withZipEntry[A](
    conf: org.apache.hadoop.conf.Configuration,
    filePath: String,
    entryName: String
  )(reader: InputStream => A): Either[String, A] = {
    val sourcePath = new Path(filePath)
    val fs = sourcePath.getFileSystem(conf)
    val inputStream = fs.open(sourcePath)
    val zipInputStream = new ZipInputStream(inputStream)
    try {
      var entry = zipInputStream.getNextEntry
      while (entry != null) {
        if (!entry.isDirectory && normalizeEntryName(entry.getName) == entryName) {
          return Right(reader(zipInputStream))
        }
        zipInputStream.closeEntry()
        entry = zipInputStream.getNextEntry
      }
      Left(s"Workbook part not found: $entryName")
    } catch {
      case NonFatal(e) =>
        Left(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
    } finally {
      zipInputStream.close()
    }
  }

  private def readWorkbookSheets(
    conf: org.apache.hadoop.conf.Configuration,
    filePath: String
  ): Either[String, Seq[WorkbookSheet]] = {
    withZipEntry(conf, filePath, WorkbookXmlEntry)(readWorkbookSheets).right.flatMap { sheets =>
      if (sheets.nonEmpty) Right(sheets) else Left("Workbook metadata not found")
    }
  }

  private def readWorkbookSheets(inputStream: InputStream): Seq[WorkbookSheet] = {
    val factory = XMLInputFactory.newFactory()
    disableXmlExternalEntities(factory)
    val reader = factory.createXMLStreamReader(inputStream)
    val sheets = ArrayBuffer.empty[WorkbookSheet]
    try {
      while (reader.hasNext) {
        if (reader.next() == XMLStreamConstants.START_ELEMENT && reader.getLocalName == "sheet") {
          val name = attributeValue(reader, "name").getOrElse("")
          val state = Option(reader.getAttributeValue(null, "state")).map(_.trim.toLowerCase).getOrElse("")
          if (name.nonEmpty) {
            sheets += WorkbookSheet(
              name = name,
              state = state,
              relationshipId = relationshipId(reader)
            )
          }
        }
      }
    } finally {
      reader.close()
    }
    sheets.toSeq
  }

  private def readWorkbookRelationships(
    conf: org.apache.hadoop.conf.Configuration,
    filePath: String
  ): Either[String, Map[String, String]] = {
    withZipEntry(conf, filePath, WorkbookRelationshipsEntry)(readRelationships)
  }

  private def readRelationships(inputStream: InputStream): Map[String, String] = {
    val factory = XMLInputFactory.newFactory()
    disableXmlExternalEntities(factory)
    val reader = factory.createXMLStreamReader(inputStream)
    val relationships = scala.collection.mutable.Map.empty[String, String]
    try {
      while (reader.hasNext) {
        if (reader.next() == XMLStreamConstants.START_ELEMENT && reader.getLocalName == "Relationship") {
          for {
            id <- attributeValue(reader, "Id")
            target <- attributeValue(reader, "Target")
          } relationships += id -> target
        }
      }
    } finally {
      reader.close()
    }
    relationships.toMap
  }

  private def readSheetHeaderCells(
    conf: org.apache.hadoop.conf.Configuration,
    filePath: String,
    sheetEntry: String
  ): Either[String, Seq[HeaderCell]] = {
    withZipEntry(conf, filePath, sheetEntry)(readSheetHeaderCells)
  }

  private def readSheetHeaderCells(inputStream: InputStream): Seq[HeaderCell] = {
    val factory = XMLInputFactory.newFactory()
    disableXmlExternalEntities(factory)
    val reader = factory.createXMLStreamReader(inputStream)
    val headerCells = ArrayBuffer.empty[HeaderCell]

    var inSheetData = false
    var inHeaderRow = false
    var finished = false
    var rowOrdinal = 0
    var nextImplicitColumnIndex = 0
    var currentCellColumnIndex = 0
    var currentCellType = ""
    var currentTextElement: Option[String] = None
    var currentText = new StringBuilder
    var currentValue = ""
    var currentInlineText = new StringBuilder

    try {
      while (reader.hasNext && !finished) {
        reader.next() match {
          case XMLStreamConstants.START_ELEMENT =>
            reader.getLocalName match {
              case "sheetData" =>
                inSheetData = true
              case "row" if inSheetData =>
                rowOrdinal += 1
                val rowIndex = attributeValue(reader, "r").flatMap(parseInt).getOrElse(rowOrdinal)
                if (rowIndex == 1) {
                  inHeaderRow = true
                  nextImplicitColumnIndex = 0
                } else if (rowIndex > 1) {
                  finished = true
                }
              case "c" if inHeaderRow =>
                currentCellType = attributeValue(reader, "t").map(_.trim).getOrElse("")
                currentCellColumnIndex = attributeValue(reader, "r")
                  .flatMap(cellReferenceColumnIndex)
                  .getOrElse {
                    val index = nextImplicitColumnIndex
                    nextImplicitColumnIndex += 1
                    index
                  }
                nextImplicitColumnIndex = math.max(nextImplicitColumnIndex, currentCellColumnIndex + 1)
                currentValue = ""
                currentInlineText = new StringBuilder
              case "v" if inHeaderRow =>
                currentTextElement = Some("v")
                currentText = new StringBuilder
              case "t" if inHeaderRow && currentCellType == "inlineStr" =>
                currentTextElement = Some("t")
                currentText = new StringBuilder
              case _ =>
            }

          case XMLStreamConstants.CHARACTERS | XMLStreamConstants.CDATA =>
            if (currentTextElement.isDefined) {
              currentText.append(reader.getText)
            }

          case XMLStreamConstants.END_ELEMENT =>
            reader.getLocalName match {
              case "v" if currentTextElement.contains("v") =>
                currentValue = currentText.toString
                currentTextElement = None
              case "t" if currentTextElement.contains("t") =>
                currentInlineText.append(currentText.toString)
                currentTextElement = None
              case "c" if inHeaderRow =>
                headerCellValue(currentCellType, currentValue, currentInlineText.toString).foreach { value =>
                  headerCells += HeaderCell(currentCellColumnIndex, value)
                }
              case "row" if inHeaderRow =>
                inHeaderRow = false
                finished = true
              case "sheetData" =>
                inSheetData = false
              case _ =>
            }

          case _ =>
        }
      }
    } finally {
      reader.close()
    }

    headerCells.toSeq
  }

  private def readSharedStrings(
    conf: org.apache.hadoop.conf.Configuration,
    filePath: String,
    requiredIndexes: Set[Int]
  ): Either[String, Map[Int, String]] = {
    withZipEntry(conf, filePath, SharedStringsEntry)(inputStream => readSharedStrings(inputStream, requiredIndexes))
  }

  private def readSharedStrings(inputStream: InputStream, requiredIndexes: Set[Int]): Map[Int, String] = {
    val factory = XMLInputFactory.newFactory()
    disableXmlExternalEntities(factory)
    val reader = factory.createXMLStreamReader(inputStream)
    val sharedStrings = scala.collection.mutable.Map.empty[Int, String]
    var currentIndex = -1
    var inSharedString = false
    var inText = false
    var currentText = new StringBuilder
    try {
      while (reader.hasNext && sharedStrings.size < requiredIndexes.size) {
        reader.next() match {
          case XMLStreamConstants.START_ELEMENT =>
            reader.getLocalName match {
              case "si" =>
                currentIndex += 1
                inSharedString = true
                currentText = new StringBuilder
              case "t" if inSharedString =>
                inText = true
              case _ =>
            }
          case XMLStreamConstants.CHARACTERS | XMLStreamConstants.CDATA =>
            if (inText) {
              currentText.append(reader.getText)
            }
          case XMLStreamConstants.END_ELEMENT =>
            reader.getLocalName match {
              case "t" =>
                inText = false
              case "si" =>
                if (requiredIndexes.contains(currentIndex)) {
                  sharedStrings += currentIndex -> currentText.toString
                }
                inSharedString = false
              case _ =>
            }
          case _ =>
        }
      }
    } finally {
      reader.close()
    }
    sharedStrings.toMap
  }

  private def headerCellValue(cellType: String, value: String, inlineText: String): Option[HeaderCellValue] = {
    cellType match {
      case "s" =>
        parseInt(value.trim).map(SharedStringHeaderValue)
      case "inlineStr" =>
        Some(LiteralHeaderValue(inlineText))
      case _ if value.nonEmpty =>
        Some(LiteralHeaderValue(value))
      case _ =>
        None
    }
  }

  private def resolveHeaderCellValue(value: HeaderCellValue, sharedStrings: Map[Int, String]): String = {
    value match {
      case LiteralHeaderValue(header) => header
      case SharedStringHeaderValue(index) => sharedStrings.getOrElse(index, "")
    }
  }

  private def deduplicateHeaderNames(names: Seq[String]): Seq[String] = {
    val duplicateNames = names.groupBy(identity).collect {
      case (name, occurrences) if occurrences.size > 1 => name
    }.toSet
    names.zipWithIndex.map {
      case (name, index) if duplicateNames.contains(name) => s"${name}_$index"
      case (name, _) => name
    }
  }

  private def resolveWorkbookPartTarget(target: String): String = {
    val resolved = URI.create(WorkbookXmlEntry).resolve(Option(target).getOrElse("")).normalize().toString
    normalizeEntryName(resolved)
  }

  private def relationshipId(reader: javax.xml.stream.XMLStreamReader): Option[String] = {
    Option(reader.getAttributeValue(RelationshipsNamespace, "id"))
      .orElse(attributeValue(reader, "id"))
      .orElse(attributeValue(reader, "r:id"))
  }

  private def attributeValue(reader: javax.xml.stream.XMLStreamReader, localName: String): Option[String] = {
    (0 until reader.getAttributeCount).collectFirst {
      case index if reader.getAttributeLocalName(index) == localName => reader.getAttributeValue(index)
    }
  }

  private def parseInt(value: String): Option[Int] = {
    try {
      Some(value.toInt)
    } catch {
      case _: NumberFormatException => None
    }
  }

  private def cellReferenceColumnIndex(reference: String): Option[Int] = {
    val letters = Option(reference).getOrElse("").takeWhile(_.isLetter)
    if (letters.isEmpty) {
      None
    } else {
      Some(letters.toUpperCase.foldLeft(0) { (acc, char) =>
        acc * 26 + (char - 'A' + 1)
      } - 1)
    }
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
