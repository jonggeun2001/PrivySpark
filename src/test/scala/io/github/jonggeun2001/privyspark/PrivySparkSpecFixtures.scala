package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.model.{PiiRule, ScanError, ScanGroup, ScanResult}
import io.github.jonggeun2001.privyspark.scan.{DirectoryScanner, GroupScanCoordinator}
import io.github.jonggeun2001.privyspark.util.DriverLogger
import org.apache.commons.compress.archivers.sevenz.SevenZOutputFile
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream
import org.apache.poi.ss.usermodel.{DataFormatter, WorkbookFactory}
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted}
import org.apache.spark.sql.SparkSession
import org.scalatest.{Assertions, BeforeAndAfterAll, Suite}

import java.io.{BufferedWriter, ByteArrayOutputStream, OutputStreamWriter, PrintStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.Comparator
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.mutable.ArrayBuffer
import scala.util.control.ControlThrowable

trait PrivySparkSpecFixtures extends BeforeAndAfterAll { this: Suite =>
  @volatile private var sparkStarted = false

  protected lazy val spark: SparkSession = {
    sparkStarted = true
    SparkSession.builder()
      .appName(getClass.getSimpleName)
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    if (sparkStarted) {
      spark.stop()
    }
    super.afterAll()
  }

  protected def capturePersistedRddNames[T](block: => T): (T, Seq[String]) = {
    val persistedRdds = new ConcurrentLinkedQueue[String]()
    val listener = new SparkListener {
      override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
        stageSubmitted.stageInfo.rddInfos.foreach { rddInfo =>
          val storageLevel = rddInfo.storageLevel
          if (storageLevel.useMemory || storageLevel.useDisk || storageLevel.useOffHeap) {
            persistedRdds.add(s"${rddInfo.name}:${storageLevel.description}")
          }
        }
      }
    }

    spark.sparkContext.addSparkListener(listener)
    try {
      (block, persistedRdds.toArray(new Array[String](0)).toSeq.distinct)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  protected def writeText(path: Path, content: String): Unit = {
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
  }

  protected def writeTextViaHadoop(path: Path, content: String): Unit = {
    val hadoopPath = new org.apache.hadoop.fs.Path(path.toString)
    val fs = hadoopPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val writer = new BufferedWriter(new OutputStreamWriter(fs.create(hadoopPath, true), StandardCharsets.UTF_8))
    try {
      writer.write(content)
    } finally {
      writer.close()
    }
  }

  protected def writeBytes(path: Path, content: Array[Byte]): Unit = {
    Files.write(path, content)
  }

  protected def captureStderr[A](block: => A): String = {
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

  protected def withDebugLoggingEnabled[A](block: => A): A = {
    withDriverLogLevel("debug")(block)
  }

  protected def withDriverLogLevel[A](level: String)(block: => A): A = {
    val previous = sys.props.get("privyspark.debug")
    PrivySparkApp.resetDebugCache()
    DriverLogger.resetCache()
    System.setProperty("privyspark.debug", level)
    try {
      block
    } finally {
      previous match {
        case Some(value) => System.setProperty("privyspark.debug", value)
        case None => System.clearProperty("privyspark.debug")
      }
      PrivySparkApp.resetDebugCache()
      DriverLogger.resetCache()
    }
  }

  protected def scanWithRules(
    inputPath: String,
    datasetPath: String,
    rules: Seq[PiiRule],
    timestamp: String
  ): (Seq[ScanResult], Seq[ScanError]) = {
    val plan = DirectoryScanner.scanDirectoryStructure(
      spark,
      inputPath,
      datasetPath,
      timestamp
    )

    val results = ArrayBuffer.empty[ScanResult]
    val errors = ArrayBuffer.empty[ScanError] ++ plan.errors

    plan.groups.foreach { group =>
      val (groupResults, groupErrors) = GroupScanCoordinator.scanGroup(
        spark,
        datasetPath,
        group,
        rules,
        sampleRatio = 1.0,
        timestamp = timestamp
      )
      results ++= groupResults
      errors ++= groupErrors
    }

    (results.toSeq, errors.toSeq)
  }

  protected case class ExitCalled(code: Int) extends ControlThrowable

  protected def createColumnarDataFile(outputDir: Path, format: String): String = {
    import spark.implicits._

    val sourceDf = Seq(
      ("alpha@example.com", "010-1111-2222", "ok"),
      ("invalid-email", "not-phone", "skip"),
      ("beta@example.com", "031-555-7777", "ok")
    ).toDF("email", "phone", "message")

    val targetDir = outputDir.resolve(s"fixture-$format")
    format match {
      case "parquet" => sourceDf.coalesce(1).write.mode("overwrite").parquet(targetDir.toString)
      case "orc" => sourceDf.coalesce(1).write.mode("overwrite").orc(targetDir.toString)
      case "avro" => sourceDf.coalesce(1).write.mode("overwrite").format("avro").save(targetDir.toString)
      case _ => Assertions.fail(s"Unsupported columnar fixture format: $format")
    }

    findDataFile(targetDir, s".$format")
      .map(_.toString)
      .getOrElse(Assertions.fail(s"Failed to locate generated $format data file under $targetDir"))
  }

  protected def createSpreadsheetFile(outputDir: Path): String = {
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

      val workbookPath = outputDir.resolve("contacts.xlsx")
      val outputStream = Files.newOutputStream(workbookPath)
      try {
        workbook.write(outputStream)
      } finally {
        outputStream.close()
      }
      workbookPath.toString
    } finally {
      workbook.close()
    }
  }

  protected def createArchiveFile(path: Path, entries: Seq[(String, String)]): String = {
    createArchiveFileWithBytes(
      path,
      entries.map { case (entryName, content) => entryName -> content.getBytes(StandardCharsets.UTF_8) }
    )
  }

  protected def createArchiveFileWithBytes(path: Path, entries: Seq[(String, Array[Byte])]): String = {
    val outputStream = new java.util.zip.ZipOutputStream(Files.newOutputStream(path))
    try {
      entries.foreach {
        case (entryName, content) =>
          outputStream.putNextEntry(new java.util.zip.ZipEntry(entryName))
          outputStream.write(content)
          outputStream.closeEntry()
      }
    } finally {
      outputStream.close()
    }
    path.toString
  }

  protected def createArchiveBytes(entries: Seq[(String, Array[Byte])]): Array[Byte] = {
    val outputStream = new ByteArrayOutputStream()
    val zipOutputStream = new java.util.zip.ZipOutputStream(outputStream)
    try {
      entries.foreach {
        case (entryName, content) =>
          zipOutputStream.putNextEntry(new java.util.zip.ZipEntry(entryName))
          zipOutputStream.write(content)
          zipOutputStream.closeEntry()
      }
    } finally {
      zipOutputStream.close()
    }
    outputStream.toByteArray
  }

  protected def createGzipFile(path: Path, content: String): String = {
    val outputStream = new GzipCompressorOutputStream(Files.newOutputStream(path))
    try {
      outputStream.write(content.getBytes(StandardCharsets.UTF_8))
    } finally {
      outputStream.close()
    }
    path.toString
  }

  protected def createTarArchiveFile(path: Path, entries: Seq[(String, String)], codec: Option[String] = None): String = {
    val rawOutputStream = Files.newOutputStream(path)
    val compressedOutputStream = codec match {
      case Some("gz") => new GzipCompressorOutputStream(rawOutputStream)
      case Some("bz2") => new BZip2CompressorOutputStream(rawOutputStream)
      case Some("xz") => new XZCompressorOutputStream(rawOutputStream)
      case Some("zst") => new ZstdCompressorOutputStream(rawOutputStream)
      case None => rawOutputStream
      case Some(other) => throw new IllegalArgumentException(s"Unsupported tar test codec: $other")
    }
    val tarOutputStream = new TarArchiveOutputStream(compressedOutputStream)
    tarOutputStream.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX)
    try {
      entries.foreach {
        case (entryName, content) =>
          val bytes = content.getBytes(StandardCharsets.UTF_8)
          val entry = new TarArchiveEntry(entryName)
          entry.setSize(bytes.length.toLong)
          tarOutputStream.putArchiveEntry(entry)
          tarOutputStream.write(bytes)
          tarOutputStream.closeArchiveEntry()
      }
      tarOutputStream.finish()
    } finally {
      tarOutputStream.close()
    }
    path.toString
  }

  protected def createSevenZArchiveFile(path: Path, entries: Seq[(String, String)]): String = {
    val outputFile = new SevenZOutputFile(path.toFile)
    try {
      entries.foreach {
        case (entryName, content) =>
          val bytes = content.getBytes(StandardCharsets.UTF_8)
          val entry = outputFile.createArchiveEntry(path.toFile, entryName)
          entry.setSize(bytes.length.toLong)
          outputFile.putArchiveEntry(entry)
          outputFile.write(bytes)
          outputFile.closeArchiveEntry()
      }
      outputFile.finish()
    } finally {
      outputFile.close()
    }
    path.toString
  }

  protected def copyClasspathResource(resourcePath: String, destination: Path): String = {
    val inputStream = Option(getClass.getResourceAsStream(resourcePath)).getOrElse {
      Assertions.fail(s"Missing classpath resource: $resourcePath")
    }
    try {
      Files.copy(inputStream, destination)
    } finally {
      inputStream.close()
    }
    destination.toString
  }

  protected def readWorkbookRows(path: Path, sheetName: String): Seq[Seq[String]] = {
    val inputStream = Files.newInputStream(path)
    val formatter = new DataFormatter()
    try {
      val workbook = WorkbookFactory.create(inputStream)
      try {
        val sheet = workbook.getSheet(sheetName)
        assert(sheet != null, s"expected workbook $path to contain sheet $sheetName")
        (0 to sheet.getLastRowNum).flatMap { rowIndex =>
          Option(sheet.getRow(rowIndex)).map { row =>
            (0 until row.getLastCellNum).map { cellIndex =>
              formatter.formatCellValue(row.getCell(cellIndex))
            }
          }
        }
      } finally {
        workbook.close()
      }
    } finally {
      inputStream.close()
    }
  }

  protected def findDataFile(root: Path, extension: String): Option[Path] = {
    val stream = Files.walk(root)
    try {
      val iter = stream.iterator()
      var found: Option[Path] = None
      while (iter.hasNext && found.isEmpty) {
        val candidate = iter.next()
        if (Files.isRegularFile(candidate) && candidate.getFileName.toString.toLowerCase.endsWith(extension)) {
          found = Some(candidate)
        }
      }
      found
    } finally {
      stream.close()
    }
  }

  protected def countPartFiles(root: Path): Long = {
    if (!Files.exists(root)) {
      0L
    } else {
      val stream = Files.walk(root)
      try {
        val iter = stream.iterator()
        var count = 0L
        while (iter.hasNext) {
          val candidate = iter.next()
          if (Files.isRegularFile(candidate) && candidate.getFileName.toString.startsWith("part-")) {
            count += 1L
          }
        }
        count
      } finally {
        stream.close()
      }
    }
  }

  protected def countFilesWithExtension(root: Path, extension: String): Long = {
    if (!Files.exists(root)) {
      0L
    } else {
      val stream = Files.walk(root)
      try {
        val iter = stream.iterator()
        var count = 0L
        while (iter.hasNext) {
          val candidate = iter.next()
          if (Files.isRegularFile(candidate) && candidate.getFileName.toString.endsWith(extension)) {
            count += 1L
          }
        }
        count
      } finally {
        stream.close()
      }
    }
  }

  protected def waitForCondition(timeoutMillis: Long, pollMillis: Long)(condition: => Boolean): Boolean = {
    val deadline = System.nanoTime() + timeoutMillis * 1000000L
    while (System.nanoTime() < deadline) {
      if (condition) {
        return true
      }
      Thread.sleep(pollMillis)
    }
    condition
  }

  protected def normalizeResults(results: Seq[ScanResult]): Seq[(String, String, String, Long, Long, Double, Double, Double)] = {
    results
      .map(result =>
        (
          result.file_identifier,
          result.column_name,
          result.pii_type,
          result.match_count,
          result.sampled_row_count,
          result.match_ratio,
          result.non_empty_match_ratio,
          result.confidence
        )
      )
      .sortBy(identity)
  }

  protected def normalizeErrors(errors: Seq[ScanError]): Seq[(String, String)] = {
    errors
      .map(error => (error.file_identifier, error.error_message))
      .sortBy(identity)
  }

  protected def normalizeOutcomeResults(
    outcomes: Seq[(ScanGroup, Seq[ScanResult], Seq[ScanError])]
  ): Seq[(String, String, String, Long, Long, Double, Double, Double)] = {
    normalizeResults(outcomes.flatMap(_._2))
  }

  protected def normalizeOutcomeErrors(
    outcomes: Seq[(ScanGroup, Seq[ScanResult], Seq[ScanError])]
  ): Seq[(String, String)] = {
    normalizeErrors(outcomes.flatMap(_._3))
  }

  protected def normalizePlanGroups(
    groups: Seq[ScanGroup]
  ): Seq[(String, String, String, Seq[String], Boolean, Boolean, Boolean)] = {
    groups
      .map(group =>
        (
          group.directoryPath,
          group.format,
          group.schemaSignature,
          group.logicalIdentifiersByKey.values.toSeq.sorted,
          group.schemaSampled,
          group.csvHasHeader,
          group.allowDirectoryIdentifier
        )
      )
      .sortBy { case (directoryPath, format, schemaSignature, logicalIdentifiers, schemaSampled, csvHasHeader, allowDirectoryIdentifier) =>
        (directoryPath, format, schemaSignature, logicalIdentifiers.mkString("|"), schemaSampled, csvHasHeader, allowDirectoryIdentifier)
      }
  }

  protected def resolveResourcePath(resource: String): Path = {
    val resourceUrl = Option(getClass.getClassLoader.getResource(resource))
      .getOrElse(Assertions.fail(s"Missing test resource: $resource"))
    Paths.get(resourceUrl.toURI)
  }

  protected def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      val stream = Files.walk(path)
      try {
        stream.sorted(Comparator.reverseOrder()).forEach(pathToDelete => Files.deleteIfExists(pathToDelete))
      } finally {
        stream.close()
      }
    }
  }
}
