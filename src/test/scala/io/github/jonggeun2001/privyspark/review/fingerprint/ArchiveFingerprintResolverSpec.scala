package io.github.jonggeun2001.privyspark.review.fingerprint

import io.github.jonggeun2001.privyspark.review.FileIdentifierResolver
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import org.apache.hadoop.conf.Configuration
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.FileOutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

@RunWith(classOf[JUnitRunner])
class ArchiveFingerprintResolverSpec extends AnyFunSuite {
  private val conf = new Configuration()

  test("resolveFingerprints resolves tar entry identifiers") {
    val inputRoot = Files.createTempDirectory("privyspark-review-tar-")

    try {
      val tarPath = inputRoot.resolve("bundle.tar")
      writeTar(tarPath, "nested/customers.csv", "name,email\nalice,alice@example.com\n")

      val fingerprints =
        FileIdentifierResolver.resolveFingerprints(conf, inputRoot.toString, "bundle.tar!nested/customers.csv")

      assert(fingerprints.isRight)
      assert(fingerprints.exists(_.size == 1))

      val fingerprint = fingerprints.toOption.get.head
      assert(fingerprint.fileIdentifier == "bundle.tar!nested/customers.csv")
      assert(fingerprint.physicalPath == tarPath.toString)
      assert(fingerprint.fileChecksumAlgo == FileIdentifierResolver.DefaultChecksumAlgo)
      assert(fingerprint.fileChecksum.nonEmpty)
      assert(fingerprint.fileSize > 0L)
    } finally {
      deleteRecursively(inputRoot)
    }
  }

  private def writeTar(path: Path, entryName: String, contents: String): Unit = {
    val bytes = contents.getBytes(StandardCharsets.UTF_8)
    val outputStream = new TarArchiveOutputStream(new FileOutputStream(path.toFile))
    try {
      val entry = new TarArchiveEntry(entryName)
      entry.setSize(bytes.length.toLong)
      outputStream.putArchiveEntry(entry)
      outputStream.write(bytes)
      outputStream.closeArchiveEntry()
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
