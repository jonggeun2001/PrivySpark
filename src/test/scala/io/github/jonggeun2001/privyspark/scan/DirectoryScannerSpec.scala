package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.PrivySparkSpecFixtures
import io.github.jonggeun2001.privyspark.config.IgnoreMatcher
import io.github.jonggeun2001.privyspark.fsio.ManagedPaths
import io.github.jonggeun2001.privyspark.model.ScanGroup
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.File
import java.nio.file.Files

@RunWith(classOf[JUnitRunner])
class DirectoryScannerSpec extends AnyFunSuite with PrivySparkSpecFixtures {
  private val Timestamp = "2026-04-30T00:00:00Z"

  test("scanDirectoryStructure returns an empty plan for an empty directory") {
    val inputDir = Files.createTempDirectory("privyspark-dir-empty-")

    try {
      val plan = DirectoryScanner.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        Timestamp
      )

      assert(plan.groups.isEmpty)
      assert(plan.errors.isEmpty)
      assert(plan.totalFiles == 0)
      assert(plan.directoryCount == 0)
      assert(plan.ignoredFiles == 0)
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanDirectoryStructure plans a single input file") {
    val inputDir = Files.createTempDirectory("privyspark-dir-single-file-")

    try {
      val inputFile = inputDir.resolve("customers.csv")
      writeText(inputFile,
        "name,email\n" +
          "alice,alice@example.com\n")

      val plan = DirectoryScanner.scanDirectoryStructure(
        spark,
        inputFile.toString,
        inputFile.toString,
        Timestamp
      )

      assert(plan.errors.isEmpty)
      assert(plan.totalFiles == 1)
      assert(plan.groups.size == 1)
      assert(plan.groups.head.format == "csv")
      assert(plan.groups.head.filePaths.map(path => new File(path).getName) == Seq("customers.csv"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanDirectoryStructure applies ignore matcher before pre-scan") {
    val inputDir = Files.createTempDirectory("privyspark-dir-ignore-")
    val backupDir = Files.createDirectories(inputDir.resolve("backup"))

    try {
      writeText(inputDir.resolve("customers.csv"),
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(inputDir.resolve("_SUCCESS"), "done\n")
      writeText(backupDir.resolve("old.csv"),
        "name,email\n" +
          "stale,stale@example.com\n")

      val plan = DirectoryScanner.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        Timestamp,
        ignoreMatcher = IgnoreMatcher.fromSources(Seq("_SUCCESS", "backup/**"), None)
      )

      assert(plan.errors.isEmpty)
      assert(plan.totalFiles == 1)
      assert(plan.ignoredFiles == 2)
      assert(plan.groups.size == 1)
      assert(plan.groups.head.filePaths.map(path => new File(path).getName) == Seq("customers.csv"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("scanDirectoryStructure expands archives and preserves nested logical identifiers") {
    val inputDir = Files.createTempDirectory("privyspark-dir-archive-")
    var stagingPaths = Seq.empty[String]

    try {
      createArchiveFile(
        inputDir.resolve("bundle.zip"),
        Seq(
          "nested/customers.csv" ->
            ("name,email\n" +
              "alice,alice@example.com\n")
        )
      )

      val plan = DirectoryScanner.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        Timestamp
      )
      stagingPaths = plan.stagingPaths

      val logicalIdentifiers = plan.groups.flatMap(_.logicalIdentifiersByKey.values).toSet
      assert(plan.errors.isEmpty)
      assert(plan.totalFiles == 1)
      assert(plan.groups.size == 1)
      assert(logicalIdentifiers == Set("bundle.zip!nested/customers.csv"))
      assert(plan.stagingPaths.nonEmpty)
    } finally {
      ManagedPaths.cleanupStagingPaths(spark.sparkContext.hadoopConfiguration, stagingPaths)
      deleteRecursively(inputDir)
    }
  }

  test("scanDirectoryStructure skips zero-byte files without creating errors") {
    val inputDir = Files.createTempDirectory("privyspark-dir-zero-byte-")

    try {
      writeText(inputDir.resolve("customers.csv"),
        "name,email\n" +
          "alice,alice@example.com\n")
      writeBytes(inputDir.resolve("_SUCCESS"), Array.emptyByteArray)

      val plan = DirectoryScanner.scanDirectoryStructure(
        spark,
        inputDir.toString,
        inputDir.toString,
        Timestamp
      )

      assert(plan.errors.isEmpty)
      assert(plan.totalFiles == 1)
      assert(plan.groups.size == 1)
      assert(plan.groups.head.filePaths.map(path => new File(path).getName) == Seq("customers.csv"))
    } finally {
      deleteRecursively(inputDir)
    }
  }

  test("splitGroupBySchema exact mode separates files with different schemas") {
    val inputDir = Files.createTempDirectory("privyspark-dir-schema-split-")

    try {
      val emailFile = inputDir.resolve("users_email.csv")
      val phoneFile = inputDir.resolve("users_phone.csv")
      writeText(emailFile,
        "name,email\n" +
          "alice,alice@example.com\n")
      writeText(phoneFile,
        "name,phone\n" +
          "bob,010-1234-5678\n")

      val group = ScanGroup(
        directoryPath = inputDir.toString,
        format = "csv",
        schemaSignature = "",
        filePaths = Seq(emailFile.toString, phoneFile.toString)
      )

      val (splitGroups, splitErrors) = DirectoryScanner.splitGroupBySchema(
        spark,
        inputDir.toString,
        Timestamp,
        group,
        new CsvHeadCache(),
        new SchemaSignatureCache()
      )

      assert(splitErrors.isEmpty)
      assert(splitGroups.size == 2)
      assert(splitGroups.forall(_.filePaths.size == 1))
      assert(splitGroups.forall(!_.schemaSampled))
      assert(splitGroups.map(_.filePaths.head).toSet == Set(emailFile.toString, phoneFile.toString))
    } finally {
      deleteRecursively(inputDir)
    }
  }
}
