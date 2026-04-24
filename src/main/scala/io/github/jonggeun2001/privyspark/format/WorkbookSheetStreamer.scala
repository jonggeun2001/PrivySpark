package io.github.jonggeun2001.privyspark.format

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream
import org.apache.spark.TaskContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.SerializableConfiguration

import java.io.InputStream
import javax.xml.stream.{XMLInputFactory, XMLStreamConstants, XMLStreamReader}
import scala.util.control.NonFatal

private[privyspark] object WorkbookSheetStreamer {
  def readSheetDataFrame(spark: SparkSession, filePath: String, sheetName: String): DataFrame = {
    val conf = spark.sparkContext.hadoopConfiguration
    val (sheetEntry, headers) = WorkbookHelpers.resolveWorkbookSheetHeaders(conf, filePath, sheetName) match {
      case Right(value) => value
      case Left(errorMessage) => throw new IllegalArgumentException(errorMessage)
    }
    val schema = StructType(headers.map(name => StructField(name, StringType, nullable = true)))
    val task = SheetReadTask(
      filePath = filePath,
      sheetEntry = sheetEntry,
      headerColumnCount = headers.length,
      serializableConf = new SerializableConfiguration(conf)
    )
    val rows = spark.sparkContext.parallelize(Seq(task), numSlices = 1).mapPartitions(_.flatMap(_.rows()))

    spark.createDataFrame(rows, schema)
  }

  private[format] final case class SheetReadTask(
    filePath: String,
    sheetEntry: String,
    headerColumnCount: Int,
    serializableConf: SerializableConfiguration
  ) extends Serializable {
    def rows(): Iterator[Row] = {
      val conf = serializableConf.value
      val sharedStrings = readSharedStrings(conf, filePath)
      val iterator = SheetRowIterator.open(conf, filePath, sheetEntry, headerColumnCount, sharedStrings)
      Option(TaskContext.get()).foreach { context =>
        context.addTaskCompletionListener[Unit](_ => iterator.close())
      }
      iterator
    }
  }

  private def readSharedStrings(conf: Configuration, filePath: String): Map[Int, String] = {
    WorkbookHelpers
      .withZipEntry(conf, filePath, WorkbookHelpers.SharedStringsEntry)(WorkbookHelpers.readAllSharedStrings) match {
      case Right(sharedStrings) => sharedStrings
      case Left(errorMessage) if errorMessage.startsWith("Workbook part not found:") => Map.empty
      case Left(errorMessage) => throw new IllegalArgumentException(errorMessage)
    }
  }

  private final case class OpenZipEntry(zipInputStream: ZipArchiveInputStream) extends AutoCloseable {
    override def close(): Unit = zipInputStream.close()
  }

  private object OpenZipEntry {
    def open(conf: Configuration, filePath: String, entryName: String): OpenZipEntry = {
      val sourcePath = new Path(filePath)
      val fs = sourcePath.getFileSystem(conf)
      val zipInputStream = new ZipArchiveInputStream(fs.open(sourcePath))
      try {
        var entry = zipInputStream.getNextEntry
        while (entry != null) {
          if (!entry.isDirectory && normalizeEntryName(entry.getName) == entryName) {
            return OpenZipEntry(zipInputStream)
          }
          entry = zipInputStream.getNextEntry
        }
        throw new IllegalArgumentException(s"Workbook part not found: $entryName")
      } catch {
        case NonFatal(e) =>
          zipInputStream.close()
          throw e
      }
    }

    private def normalizeEntryName(name: String): String =
      Option(name).getOrElse("").stripPrefix("/")
  }

  private final class SheetRowIterator(
    input: OpenZipEntry,
    reader: XMLStreamReader,
    headerColumnCount: Int,
    sharedStrings: Map[Int, String]
  ) extends Iterator[Row] with AutoCloseable {
    private var nextRow: Option[Row] = None
    private var loaded = false
    private var closed = false
    private var inSheetData = false
    private var skipCurrentRow = false
    private var rowOrdinal = 0
    private var nextImplicitColumnIndex = 0
    private var currentRowValues = Array.fill[String](headerColumnCount)(null)
    private var inCell = false
    private var currentCellColumnIndex = 0
    private var currentCellType = ""
    private var currentTextElement: Option[String] = None
    private var currentText = new StringBuilder
    private var currentValue = ""
    private var currentInlineText = new StringBuilder

    override def hasNext: Boolean = {
      if (!loaded) {
        loadNext()
      }
      nextRow.isDefined
    }

    override def next(): Row = {
      if (!hasNext) {
        throw new NoSuchElementException("next on empty iterator")
      }
      val row = nextRow.get
      nextRow = None
      loaded = false
      row
    }

    override def close(): Unit = {
      if (!closed) {
        closed = true
        try {
          reader.close()
        } finally {
          input.close()
        }
      }
    }

    private def loadNext(): Unit = {
      loaded = true
      nextRow = None
      try {
        while (!closed && reader.hasNext && nextRow.isEmpty) {
          reader.next() match {
            case XMLStreamConstants.START_ELEMENT =>
              handleStartElement()
            case XMLStreamConstants.CHARACTERS | XMLStreamConstants.CDATA =>
              if (currentTextElement.isDefined) {
                currentText.append(reader.getText)
              }
            case XMLStreamConstants.END_ELEMENT =>
              handleEndElement()
            case _ =>
          }
        }
        if (nextRow.isEmpty) {
          close()
        }
      } catch {
        case NonFatal(e) =>
          close()
          throw e
      }
    }

    private def handleStartElement(): Unit = {
      reader.getLocalName match {
        case "sheetData" =>
          inSheetData = true
        case "row" if inSheetData =>
          rowOrdinal += 1
          val rowIndex = attributeValue(reader, "r").flatMap(parseInt).getOrElse(rowOrdinal)
          skipCurrentRow = rowIndex == 1
          currentRowValues = Array.fill[String](headerColumnCount)(null)
          nextImplicitColumnIndex = 0
        case "c" if inSheetData =>
          inCell = true
          currentCellType = attributeValue(reader, "t").map(_.trim).getOrElse("")
          currentCellColumnIndex = attributeValue(reader, "r")
            .flatMap(WorkbookHelpers.cellReferenceColumnIndex)
            .getOrElse {
              val index = nextImplicitColumnIndex
              nextImplicitColumnIndex += 1
              index
            }
          nextImplicitColumnIndex = math.max(nextImplicitColumnIndex, currentCellColumnIndex + 1)
          currentTextElement = None
          currentText = new StringBuilder
          currentValue = ""
          currentInlineText = new StringBuilder
        case "v" if inCell =>
          currentTextElement = Some("v")
          currentText = new StringBuilder
        case "t" if inCell && currentCellType == "inlineStr" =>
          currentTextElement = Some("t")
          currentText = new StringBuilder
        case _ =>
      }
    }

    private def handleEndElement(): Unit = {
      reader.getLocalName match {
        case "v" if currentTextElement.contains("v") =>
          currentValue = currentText.toString
          currentTextElement = None
        case "t" if currentTextElement.contains("t") =>
          currentInlineText.append(currentText.toString)
          currentTextElement = None
        case "c" if inCell =>
          if (!skipCurrentRow && currentCellColumnIndex >= 0 && currentCellColumnIndex < headerColumnCount) {
            currentRowValues(currentCellColumnIndex) = cellValue()
          }
          inCell = false
        case "row" if inSheetData =>
          if (!skipCurrentRow && currentRowValues.exists(_ != null)) {
            nextRow = Some(Row.fromSeq(currentRowValues.toSeq))
          }
          skipCurrentRow = false
        case "sheetData" =>
          inSheetData = false
        case _ =>
      }
    }

    private def cellValue(): String = {
      currentCellType match {
        case "s" =>
          parseInt(currentValue.trim).flatMap(sharedStrings.get).getOrElse("")
        case "b" =>
          currentValue.trim match {
            case "1" => "TRUE"
            case "0" => "FALSE"
            case other => other
          }
        case "inlineStr" =>
          currentInlineText.toString
        case _ if currentValue.nonEmpty =>
          currentValue
        case _ =>
          null
      }
    }

    private def attributeValue(reader: XMLStreamReader, localName: String): Option[String] = {
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
  }

  private object SheetRowIterator {
    def open(
      conf: Configuration,
      filePath: String,
      sheetEntry: String,
      headerColumnCount: Int,
      sharedStrings: Map[Int, String]
    ): SheetRowIterator = {
      val input = OpenZipEntry.open(conf, filePath, sheetEntry)
      val reader =
        try {
          createXmlReader(input.zipInputStream)
        } catch {
          case NonFatal(e) =>
            input.close()
            throw e
        }
      new SheetRowIterator(input, reader, headerColumnCount, sharedStrings)
    }

    private def createXmlReader(inputStream: InputStream): XMLStreamReader = {
      val factory = XMLInputFactory.newFactory()
      WorkbookHelpers.disableXmlExternalEntities(factory)
      factory.createXMLStreamReader(inputStream)
    }
  }
}
