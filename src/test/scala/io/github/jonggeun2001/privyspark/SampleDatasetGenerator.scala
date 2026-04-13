package io.github.jonggeun2001.privyspark

import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.sql.SparkSession

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.util.Comparator
import java.util.zip.{ZipEntry, ZipOutputStream}

object SampleDatasetGenerator {
  final case class Scenario(
    caseId: String,
    relativePath: String,
    expectedResultRows: Int,
    expectedErrorRows: Int,
    expectedIdentifierFragment: String = "",
    expectedErrorFragment: String = ""
  )

  private val DefaultOutputRoot = Paths.get("samples", "input-cases")
  private val FilesDirectoryName = "files"
  private val SampleRules =
    """rules:
      |  - pii_type: email
      |    regex: '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}'
      |  - pii_type: phone_number
      |    regex: '(?<![0-9])(?:01[016789]|0[2-9][0-9]?)-?[0-9]{3,4}-?[0-9]{4}(?![0-9])'
      |""".stripMargin
  private val CsvContent =
    """email,phone,message
      |alpha@example.com,010-1111-2222,ok
      |invalid-email,not-phone,skip
      |beta@example.com,031-555-7777,ok
      |""".stripMargin
  private val JsonLineRecords = Seq(
    """{"email":"alpha@example.com","phone":"010-1111-2222","message":"ok"}""",
    """{"email":"invalid-email","phone":"not-phone","message":"skip"}""",
    """{"email":"beta@example.com","phone":"031-555-7777","message":"ok"}"""
  )
  private val TextFallbackContent =
    """Reach alpha@example.com at 010-1111-2222
      |Reach beta@example.com at 031-555-7777
      |ignore
      |""".stripMargin

  def main(args: Array[String]): Unit = {
    val outputRoot = args.headOption.map(Paths.get(_)).getOrElse(DefaultOutputRoot).toAbsolutePath.normalize()
    val spark = SparkSession.builder()
      .appName("SampleDatasetGenerator")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      generate(outputRoot, spark)
    } finally {
      spark.stop()
    }
  }

  private[privyspark] def generate(outputRoot: Path, spark: SparkSession): Seq[Scenario] = {
    Files.createDirectories(outputRoot)
    val filesRoot = outputRoot.resolve(FilesDirectoryName)
    recreateDirectory(filesRoot)
    writeText(outputRoot.resolve("sample-rules.yaml"), SampleRules)

    val scenarios = Seq(
      generateFlatCsv(filesRoot),
      generateFlatJson(filesRoot),
      generateFlatJsonl(filesRoot),
      generateFlatNdjson(filesRoot),
      generateFlatParquet(spark, filesRoot),
      generateFlatOrc(spark, filesRoot),
      generateFlatAvro(spark, filesRoot),
      generateFlatXlsx(filesRoot),
      generateFlatTextFallback(filesRoot),
      generateFlatExtensionlessText(filesRoot),
      generateFlatExtensionlessParquet(spark, filesRoot),
      generateFlatExtensionlessOrc(spark, filesRoot),
      generateFlatParquetAlias(spark, filesRoot),
      generateArchiveZipMixed(spark, filesRoot),
      generateArchiveJar(filesRoot),
      generateArchiveZeroByteEntry(filesRoot),
      generateZeroByteSiblingDirectory(filesRoot),
      generateUnsupportedBinary(filesRoot),
      generateBrokenWorkbook(filesRoot),
      generateNestedArchive(filesRoot),
      generateUnsafeArchive(filesRoot)
    )
    writeManifest(outputRoot.resolve("scenario-manifest.tsv"), scenarios)
    scenarios
  }

  private def generateFlatCsv(filesRoot: Path): Scenario = {
    val path = filesRoot.resolve("flat/csv/customers.csv")
    writeText(path, CsvContent)
    Scenario("flat_csv", relativePath(filesRoot, path), expectedResultRows = 2, expectedErrorRows = 0, expectedIdentifierFragment = "customers.csv")
  }

  private def generateFlatJson(filesRoot: Path): Scenario = {
    val path = filesRoot.resolve("flat/json/records.json")
    writeText(path, JsonLineRecords.mkString("", "\n", "\n"))
    Scenario("flat_json", relativePath(filesRoot, path), 2, 0, "records.json")
  }

  private def generateFlatJsonl(filesRoot: Path): Scenario = {
    val path = filesRoot.resolve("flat/jsonl/events.jsonl")
    writeText(path, JsonLineRecords.mkString("", "\n", "\n"))
    Scenario("flat_jsonl", relativePath(filesRoot, path), 2, 0, "events.jsonl")
  }

  private def generateFlatNdjson(filesRoot: Path): Scenario = {
    val path = filesRoot.resolve("flat/ndjson/events.ndjson")
    writeText(path, JsonLineRecords.mkString("", "\n", "\n"))
    Scenario("flat_ndjson", relativePath(filesRoot, path), 2, 0, "events.ndjson")
  }

  private def generateFlatParquet(spark: SparkSession, filesRoot: Path): Scenario = {
    val path = filesRoot.resolve("flat/parquet/contacts.parquet")
    createColumnarDataFile(spark, path, "parquet")
    Scenario("flat_parquet", relativePath(filesRoot, path), 2, 0, "contacts.parquet")
  }

  private def generateFlatOrc(spark: SparkSession, filesRoot: Path): Scenario = {
    val path = filesRoot.resolve("flat/orc/contacts.orc")
    createColumnarDataFile(spark, path, "orc")
    Scenario("flat_orc", relativePath(filesRoot, path), 2, 0, "contacts.orc")
  }

  private def generateFlatAvro(spark: SparkSession, filesRoot: Path): Scenario = {
    val path = filesRoot.resolve("flat/avro/contacts.avro")
    createColumnarDataFile(spark, path, "avro")
    Scenario("flat_avro", relativePath(filesRoot, path), 2, 0, "contacts.avro")
  }

  private def generateFlatXlsx(filesRoot: Path): Scenario = {
    val path = filesRoot.resolve("flat/xlsx/contacts.xlsx")
    createSpreadsheetFile(path)
    Scenario("flat_xlsx", relativePath(filesRoot, path), 2, 0, "contacts.xlsx#Contacts")
  }

  private def generateFlatTextFallback(filesRoot: Path): Scenario = {
    val path = filesRoot.resolve("flat/text-extension/notes.log")
    writeText(path, TextFallbackContent)
    Scenario("flat_text_extension", relativePath(filesRoot, path), 2, 0, "notes.log")
  }

  private def generateFlatExtensionlessText(filesRoot: Path): Scenario = {
    val path = filesRoot.resolve("flat/text-no-extension/notes")
    writeText(path, TextFallbackContent)
    Scenario("flat_text_no_extension", relativePath(filesRoot, path), 2, 0, "notes")
  }

  private def generateFlatExtensionlessParquet(spark: SparkSession, filesRoot: Path): Scenario = {
    val path = filesRoot.resolve("flat/extensionless-parquet/payload")
    createColumnarDataFile(spark, path, "parquet")
    Scenario("flat_extensionless_parquet", relativePath(filesRoot, path), 2, 0, "payload")
  }

  private def generateFlatExtensionlessOrc(spark: SparkSession, filesRoot: Path): Scenario = {
    val path = filesRoot.resolve("flat/extensionless-orc/payload")
    createColumnarDataFile(spark, path, "orc")
    Scenario("flat_extensionless_orc", relativePath(filesRoot, path), 2, 0, "payload")
  }

  private def generateFlatParquetAlias(spark: SparkSession, filesRoot: Path): Scenario = {
    val path = filesRoot.resolve("flat/parquet-alias/contacts.parq")
    createColumnarDataFile(spark, path, "parquet")
    Scenario("flat_parq_alias", relativePath(filesRoot, path), 2, 0, "contacts.parq")
  }

  private def generateArchiveZipMixed(spark: SparkSession, filesRoot: Path): Scenario = {
    val temporaryParquet = createTemporaryColumnarFile(spark, filesRoot, "zip-parquet", "parquet")
    val parquetPayload = Files.readAllBytes(temporaryParquet)
    deleteRecursively(temporaryParquet.getParent)
    val path = filesRoot.resolve("archive/mixed.zip")
    createArchiveFileWithBytes(
      path,
      Seq(
        "nested/customers.csv" -> CsvContent.getBytes(StandardCharsets.UTF_8),
        "nested/payload" -> parquetPayload,
        "nested/notes.log" -> TextFallbackContent.getBytes(StandardCharsets.UTF_8),
        "nested/empty.csv" -> Array.emptyByteArray
      )
    )
    Scenario("archive_zip_mixed", relativePath(filesRoot, path), 6, 0, "mixed.zip!")
  }

  private def generateArchiveJar(filesRoot: Path): Scenario = {
    val path = filesRoot.resolve("archive/mixed.jar")
    createArchiveFileWithBytes(
      path,
      Seq(
        "data/events.jsonl" -> JsonLineRecords.mkString("", "\n", "\n").getBytes(StandardCharsets.UTF_8)
      )
    )
    Scenario("archive_jar_jsonl", relativePath(filesRoot, path), 2, 0, "mixed.jar!")
  }

  private def generateArchiveZeroByteEntry(filesRoot: Path): Scenario = {
    val path = filesRoot.resolve("archive/zero-byte-entry.zip")
    createArchiveFileWithBytes(
      path,
      Seq(
        "data/customers.csv" -> CsvContent.getBytes(StandardCharsets.UTF_8),
        "data/ignored.csv" -> Array.emptyByteArray,
        "data/_SUCCESS" -> Array.emptyByteArray
      )
    )
    Scenario("archive_zero_byte_entry", relativePath(filesRoot, path), 2, 0, "zero-byte-entry.zip!")
  }

  private def generateZeroByteSiblingDirectory(filesRoot: Path): Scenario = {
    val dir = filesRoot.resolve("edge/zero-byte-sibling")
    writeText(dir.resolve("contacts.csv"), CsvContent)
    writeBytes(dir.resolve("_SUCCESS"), Array.emptyByteArray)
    Scenario("edge_zero_byte_sibling", relativePath(filesRoot, dir), 2, 0, "contacts.csv")
  }

  private def generateUnsupportedBinary(filesRoot: Path): Scenario = {
    val path = filesRoot.resolve("edge/unsupported-binary/Bytecode.class")
    writeBytes(path, Array(0xca.toByte, 0xfe.toByte, 0xba.toByte, 0xbe.toByte, 0x00.toByte, 0x00.toByte))
    Scenario("edge_unsupported_binary", relativePath(filesRoot, path), 0, 1, "Bytecode.class", "Unsupported file format")
  }

  private def generateBrokenWorkbook(filesRoot: Path): Scenario = {
    val path = filesRoot.resolve("edge/broken-workbook/broken.xlsx")
    writeText(path, "not a real workbook")
    Scenario("edge_broken_workbook", relativePath(filesRoot, path), 0, 1, "broken.xlsx", "Workbook read failed")
  }

  private def generateNestedArchive(filesRoot: Path): Scenario = {
    val path = filesRoot.resolve("edge/nested-archive/nested.zip")
    val innerArchive = createArchiveBytes(
      Seq("inner/customers.csv" -> CsvContent.getBytes(StandardCharsets.UTF_8))
    )
    createArchiveFileWithBytes(path, Seq("inner.zip" -> innerArchive))
    Scenario("edge_nested_archive", relativePath(filesRoot, path), 0, 1, "nested.zip!inner.zip", "Nested archive expansion is not supported")
  }

  private def generateUnsafeArchive(filesRoot: Path): Scenario = {
    val path = filesRoot.resolve("edge/unsafe-archive/unsafe.zip")
    createArchiveFileWithBytes(path, Seq("../escape.csv" -> CsvContent.getBytes(StandardCharsets.UTF_8)))
    Scenario("edge_unsafe_archive", relativePath(filesRoot, path), 0, 1, "unsafe.zip!..", "Unsafe archive entry path")
  }

  private def createColumnarDataFile(spark: SparkSession, finalPath: Path, format: String): Unit = {
    import spark.implicits._

    val targetDir = finalPath.getParent.resolve(s".tmp-${finalPath.getFileName.toString}-$format")
    recreateDirectory(targetDir)

    val sourceDf = Seq(
      ("alpha@example.com", "010-1111-2222", "ok"),
      ("invalid-email", "not-phone", "skip"),
      ("beta@example.com", "031-555-7777", "ok")
    ).toDF("email", "phone", "message")

    format match {
      case "parquet" => sourceDf.coalesce(1).write.mode("overwrite").parquet(targetDir.toString)
      case "orc" => sourceDf.coalesce(1).write.mode("overwrite").orc(targetDir.toString)
      case "avro" => sourceDf.coalesce(1).write.mode("overwrite").format("avro").save(targetDir.toString)
      case _ => throw new IllegalArgumentException(s"Unsupported sample format: $format")
    }

    Files.createDirectories(finalPath.getParent)
    val dataFile = findDataFile(targetDir, s".$format")
      .getOrElse(throw new IllegalStateException(s"Failed to locate generated $format file under $targetDir"))
    Files.move(dataFile, finalPath, StandardCopyOption.REPLACE_EXISTING)
    deleteRecursively(targetDir)
  }

  private def createTemporaryColumnarFile(spark: SparkSession, filesRoot: Path, name: String, format: String): Path = {
    val tempDir = filesRoot.resolve(".generated-temp")
    Files.createDirectories(tempDir)
    val tempPath = tempDir.resolve(s"$name.$format")
    createColumnarDataFile(spark, tempPath, format)
    tempPath
  }

  private def createSpreadsheetFile(path: Path): Unit = {
    Files.createDirectories(path.getParent)
    val workbook = new XSSFWorkbook()
    try {
      val sheet = workbook.createSheet("Contacts")
      val header = sheet.createRow(0)
      header.createCell(0).setCellValue("email")
      header.createCell(1).setCellValue("phone")

      val row1 = sheet.createRow(1)
      row1.createCell(0).setCellValue("alpha@example.com")
      row1.createCell(1).setCellValue("010-1111-2222")

      val row2 = sheet.createRow(2)
      row2.createCell(0).setCellValue("invalid-email")
      row2.createCell(1).setCellValue("not-phone")

      val row3 = sheet.createRow(3)
      row3.createCell(0).setCellValue("beta@example.com")
      row3.createCell(1).setCellValue("031-555-7777")

      workbook.createSheet("IgnoredEmpty")

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

  private def createArchiveFileWithBytes(path: Path, entries: Seq[(String, Array[Byte])]): Unit = {
    Files.createDirectories(path.getParent)
    val outputStream = new ZipOutputStream(Files.newOutputStream(path))
    try {
      entries.foreach {
        case (entryName, content) =>
          outputStream.putNextEntry(new ZipEntry(entryName))
          outputStream.write(content)
          outputStream.closeEntry()
      }
    } finally {
      outputStream.close()
    }
  }

  private def createArchiveBytes(entries: Seq[(String, Array[Byte])]): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    val zipOutputStream = new ZipOutputStream(output)
    try {
      entries.foreach {
        case (entryName, content) =>
          zipOutputStream.putNextEntry(new ZipEntry(entryName))
          zipOutputStream.write(content)
          zipOutputStream.closeEntry()
      }
    } finally {
      zipOutputStream.close()
    }
    output.toByteArray
  }

  private def recreateDirectory(path: Path): Unit = {
    deleteRecursively(path)
    Files.createDirectories(path)
  }

  private def deleteRecursively(path: Path): Unit = {
    if (!Files.exists(path)) {
      return
    }

    val walk = Files.walk(path)
    try {
      walk.sorted(Comparator.reverseOrder()).forEach(p => Files.deleteIfExists(p))
    } finally {
      walk.close()
    }
  }

  private def writeText(path: Path, content: String): Unit = {
    Files.createDirectories(path.getParent)
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
  }

  private def writeBytes(path: Path, content: Array[Byte]): Unit = {
    Files.createDirectories(path.getParent)
    Files.write(path, content)
  }

  private def relativePath(filesRoot: Path, path: Path): String = {
    FilesDirectoryName + "/" + filesRoot.relativize(path).toString.replace('\\', '/')
  }

  private def writeManifest(path: Path, scenarios: Seq[Scenario]): Unit = {
    val lines = Seq("case_id\trelative_path\texpected_result_rows\texpected_error_rows\texpected_identifier_fragment\texpected_error_fragment") ++
      scenarios.map { scenario =>
        Seq(
          scenario.caseId,
          scenario.relativePath,
          scenario.expectedResultRows.toString,
          scenario.expectedErrorRows.toString,
          scenario.expectedIdentifierFragment,
          scenario.expectedErrorFragment
        ).mkString("\t")
      }
    writeText(path, lines.mkString("", "\n", "\n"))
  }

  private def findDataFile(dir: Path, suffix: String): Option[Path] = {
    if (!Files.exists(dir)) {
      return None
    }

    val walk = Files.walk(dir)
    try {
      val iterator = walk.iterator()
      var found: Option[Path] = None
      while (iterator.hasNext && found.isEmpty) {
        val current = iterator.next()
        if (Files.isRegularFile(current) && current.getFileName.toString.toLowerCase.endsWith(suffix.toLowerCase)) {
          found = Some(current)
        }
      }
      found
    } finally {
      walk.close()
    }
  }
}
