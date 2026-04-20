package io.github.jonggeun2001.privyspark.review

import org.apache.hadoop.conf.Configuration
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.FileOutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

@RunWith(classOf[JUnitRunner])
class FileIdentifierResolverSpec extends AnyFunSuite {
  private val conf = new Configuration()

  test("resolveFingerprints returns crc32 metadata for a flat file") {
    val inputRoot = Files.createTempDirectory("privyspark-review-flat-")

    try {
      val csvFile = inputRoot.resolve("users.csv")
      Files.write(csvFile, "name,email\nalice,alice@example.com\n".getBytes(StandardCharsets.UTF_8))

      val fingerprints = FileIdentifierResolver.resolveFingerprints(conf, inputRoot.toString, "users.csv")

      assert(fingerprints.isRight)
      assert(fingerprints.exists(_.size == 1))

      val fingerprint = fingerprints.toOption.get.head
      assert(fingerprint.fileIdentifier == "users.csv")
      assert(fingerprint.fileSize > 0L)
      assert(fingerprint.fileMtimeEpochMs > 0L)
      assert(fingerprint.fileChecksumAlgo == "CRC32")
      assert(fingerprint.fileChecksum.nonEmpty)
    } finally {
      deleteRecursively(inputRoot)
    }
  }

  test("resolveFingerprints resolves basename identifiers when inputRoot points to a single flat file") {
    val tempDir = Files.createTempDirectory("privyspark-review-single-flat-")

    try {
      val csvFile = tempDir.resolve("users.csv")
      Files.write(csvFile, "name,email\nalice,alice@example.com\n".getBytes(StandardCharsets.UTF_8))

      val fingerprints = FileIdentifierResolver.resolveFingerprints(conf, csvFile.toString, "users.csv")

      assert(fingerprints.isRight)
      assert(fingerprints.exists(_.size == 1))

      val fingerprint = fingerprints.toOption.get.head
      assert(fingerprint.fileIdentifier == "users.csv")
      assert(fingerprint.physicalPath == csvFile.toString)
      assert(fingerprint.fileSize == Files.size(csvFile))
    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("resolveFingerprints resolves workbook sheet identifiers to workbook metadata") {
    val inputRoot = Files.createTempDirectory("privyspark-review-xlsx-")

    try {
      val workbookPath = inputRoot.resolve("review.xlsx")
      writeWorkbook(workbookPath, "scan_results")

      val fingerprints = FileIdentifierResolver.resolveFingerprints(conf, inputRoot.toString, "review.xlsx#scan_results")

      assert(fingerprints.isRight)
      assert(fingerprints.exists(_.size == 1))

      val fingerprint = fingerprints.toOption.get.head
      assert(fingerprint.fileIdentifier == "review.xlsx#scan_results")
      assert(fingerprint.fileSize == Files.size(workbookPath))
      assert(fingerprint.fileMtimeEpochMs > 0L)
      assert(fingerprint.fileChecksum.nonEmpty)
    } finally {
      deleteRecursively(inputRoot)
    }
  }

  test("resolveFingerprints resolves workbook sheet identifiers when inputRoot points to a single workbook") {
    val tempDir = Files.createTempDirectory("privyspark-review-single-xlsx-")

    try {
      val workbookPath = tempDir.resolve("review.xlsx")
      writeWorkbook(workbookPath, "scan_results")

      val fingerprints = FileIdentifierResolver.resolveFingerprints(conf, workbookPath.toString, "review.xlsx#scan_results")

      assert(fingerprints.isRight)
      assert(fingerprints.exists(_.size == 1))

      val fingerprint = fingerprints.toOption.get.head
      assert(fingerprint.fileIdentifier == "review.xlsx#scan_results")
      assert(fingerprint.physicalPath == workbookPath.toString)
      assert(fingerprint.fileSize == Files.size(workbookPath))
    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("resolveFingerprints resolves zip entry identifiers without materializing a staged file") {
    val inputRoot = Files.createTempDirectory("privyspark-review-zip-")

    try {
      val zipPath = inputRoot.resolve("bundle.zip")
      writeZip(zipPath, "nested/customers.csv", "name,email\nalice,alice@example.com\n")

      val fingerprints =
        FileIdentifierResolver.resolveFingerprints(conf, inputRoot.toString, "bundle.zip!nested/customers.csv")

      assert(fingerprints.isRight)
      assert(fingerprints.exists(_.size == 1))

      val fingerprint = fingerprints.toOption.get.head
      assert(fingerprint.fileIdentifier == "bundle.zip!nested/customers.csv")
      assert(fingerprint.fileSize > 0L)
      assert(fingerprint.fileMtimeEpochMs > 0L)
      assert(fingerprint.fileChecksum.nonEmpty)
    } finally {
      deleteRecursively(inputRoot)
    }
  }

  test("resolveFingerprints resolves archive entry identifiers when inputRoot points to a single archive") {
    val tempDir = Files.createTempDirectory("privyspark-review-single-zip-")

    try {
      val zipPath = tempDir.resolve("bundle.zip")
      writeZip(zipPath, "nested/customers.csv", "name,email\nalice,alice@example.com\n")

      val fingerprints =
        FileIdentifierResolver.resolveFingerprints(conf, zipPath.toString, "bundle.zip!nested/customers.csv")

      assert(fingerprints.isRight)
      assert(fingerprints.exists(_.size == 1))

      val fingerprint = fingerprints.toOption.get.head
      assert(fingerprint.fileIdentifier == "bundle.zip!nested/customers.csv")
      assert(fingerprint.physicalPath == zipPath.toString)
      assert(fingerprint.fileSize > 0L)
    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("resolveFingerprints expands directory identifiers to direct child files") {
    val inputRoot = Files.createTempDirectory("privyspark-review-dir-")

    try {
      val reviewsDir = Files.createDirectories(inputRoot.resolve("reviews"))
      Files.write(reviewsDir.resolve("a.csv"), "id\n1\n".getBytes(StandardCharsets.UTF_8))
      Files.write(reviewsDir.resolve("b.csv"), "id\n2\n".getBytes(StandardCharsets.UTF_8))
      Files.createDirectories(reviewsDir.resolve("nested"))
      Files.write(reviewsDir.resolve("nested").resolve("ignored.csv"), "id\n3\n".getBytes(StandardCharsets.UTF_8))

      val fingerprints = FileIdentifierResolver.resolveFingerprints(conf, inputRoot.toString, "reviews")

      assert(fingerprints.isRight)
      assert(fingerprints.toOption.get.map(_.fileIdentifier) == Seq("reviews/a.csv", "reviews/b.csv"))
    } finally {
      deleteRecursively(inputRoot)
    }
  }

  private def writeWorkbook(path: Path, sheetName: String): Unit = {
    val workbook = new XSSFWorkbook()
    val sheet = workbook.createSheet(sheetName)
    val header = sheet.createRow(0)
    header.createCell(0).setCellValue("name")
    header.createCell(1).setCellValue("email")
    val row = sheet.createRow(1)
    row.createCell(0).setCellValue("alice")
    row.createCell(1).setCellValue("alice@example.com")

    val outputStream = new FileOutputStream(path.toFile)
    try {
      workbook.write(outputStream)
    } finally {
      outputStream.close()
      workbook.close()
    }
  }

  private def writeZip(path: Path, entryName: String, contents: String): Unit = {
    val outputStream = new ZipOutputStream(new FileOutputStream(path.toFile))
    try {
      outputStream.putNextEntry(new ZipEntry(entryName))
      outputStream.write(contents.getBytes(StandardCharsets.UTF_8))
      outputStream.closeEntry()
    } finally {
      outputStream.close()
    }
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      Files.walk(path)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.deleteIfExists)
    }
  }
}
