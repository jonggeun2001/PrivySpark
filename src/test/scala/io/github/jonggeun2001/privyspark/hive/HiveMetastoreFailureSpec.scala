package io.github.jonggeun2001.privyspark.hive

import io.github.jonggeun2001.privyspark.PrivySparkSpecFixtures
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.file.{Files, Path}
import java.sql.DriverManager
import java.util.UUID

@RunWith(classOf[JUnitRunner])
class HiveMetastoreFailureSpec extends AnyFunSuite with PrivySparkSpecFixtures {
  private def withPassword(contents: String)(body: Path => Unit): Unit = {
    val path = Files.createTempFile("privyspark-metastore-password-", ".txt")
    try {
      writeText(path, contents)
      body(path)
    } finally Files.deleteIfExists(path)
  }

  test("password files use the trimmed first line only") {
    withPassword("  test-password  \r\nignored line\n") { path =>
      assert(HiveTableLookup.readPasswordFile(spark, path.toString) == "test-password")
    }
  }

  Seq("", "  \t\n", "\npassword on second line").zipWithIndex.foreach { case (contents, index) =>
    test(s"empty password case $index fails explicitly") {
      withPassword(contents) { path =>
        val error = intercept[IllegalArgumentException](HiveTableLookup.readPasswordFile(spark, path.toString))
        assert(error.getMessage.contains("file is empty"))
      }
    }
  }

  test("missing password files and directories fail before any JDBC connection") {
    val directory = Files.createTempDirectory("privyspark-metastore-missing-")
    try {
      intercept[java.io.FileNotFoundException] {
        HiveTableLookup.readPasswordFile(spark, directory.resolve("missing").toString)
      }
      val error = intercept[IllegalArgumentException](HiveTableLookup.readPasswordFile(spark, directory.toString))
      assert(error.getMessage.contains("not a file"))
      val index = HiveTableLookup.buildLookupIndex(spark, Some(
        HiveMetastoreJdbcConfig("jdbc:unused", "user", directory.resolve("missing").toString, "unused.Driver")))
      assert(index == HiveTableLookupIndex.Empty)
    } finally deleteRecursively(directory)
  }

  test("password file size limit includes exactly one MiB and rejects one extra byte") {
    val maximum = "x" * (1024 * 1024)
    withPassword(maximum) { path =>
      assert(HiveTableLookup.readPasswordFile(spark, path.toString) == maximum)
      writeText(path, maximum + "x")
      val error = intercept[IllegalArgumentException](HiveTableLookup.readPasswordFile(spark, path.toString))
      assert(error.getMessage.contains("larger than 1048576 bytes"))
    }
  }

  test("absent configuration and missing JDBC driver disable optional Hive lookup") {
    assert(HiveTableLookup.buildLookupIndex(spark, None) == HiveTableLookupIndex.Empty)
    withPassword("secret") { path =>
      val index = HiveTableLookup.buildLookupIndex(spark, Some(
        HiveMetastoreJdbcConfig("jdbc:unused", "user", path.toString, "privyspark.test.MissingDriver")))
      assert(index == HiveTableLookupIndex.Empty)
    }
  }

  test("JDBC authentication failure disables lookup without failing the scan") {
    Class.forName("org.h2.Driver")
    val url = s"jdbc:h2:mem:authentication_${UUID.randomUUID().toString.replace('-', '_')}"
    val connection = DriverManager.getConnection(url, "sa", "correct-password")
    try withPassword("incorrect-password") { path =>
      val index = HiveTableLookup.buildLookupIndex(spark, Some(
        HiveMetastoreJdbcConfig(url, "sa", path.toString, "org.h2.Driver")))
      assert(index == HiveTableLookupIndex.Empty)
      assert(!connection.isClosed)
    } finally connection.close()
  }

  test("metastore query failure disables lookup when required tables are absent") {
    Class.forName("org.h2.Driver")
    val url = s"jdbc:h2:mem:missing_schema_${UUID.randomUUID().toString.replace('-', '_')}"
    val connection = DriverManager.getConnection(url, "sa", "secret")
    try withPassword("secret") { path =>
      val index = HiveTableLookup.buildLookupIndex(spark, Some(
        HiveMetastoreJdbcConfig(url, "sa", path.toString, "org.h2.Driver")))
      assert(index == HiveTableLookupIndex.Empty)
      assert(!connection.isClosed)
    } finally connection.close()
  }
}
