package io.github.jonggeun2001.privyspark.config

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.charset.StandardCharsets
import java.nio.file.Files

@RunWith(classOf[JUnitRunner])
class IgnoreMatcherSpec extends AnyFunSuite {
  test("fromSources merges inline and file patterns while ignoring comments and blank lines") {
    val ignoreFile = Files.createTempFile("privyspark-ignore-", ".txt")

    try {
      Files.write(
        ignoreFile,
        "# comment\n\nbackup/**\nlogs/\n".getBytes(StandardCharsets.UTF_8)
      )

      val matcher = IgnoreMatcher.fromSources(Seq("_SUCCESS"), Some(ignoreFile.toString))

      assert(matcher.matched("/data/input/_SUCCESS", "/data/input").contains("_SUCCESS"))
      assert(matcher.matched("/data/input/backup/old.csv", "/data/input").contains("backup/**"))
      assert(matcher.matched("/data/input/logs", "/data/input", isDirectory = true).contains("logs/"))
      assert(matcher.matched("/data/input/data.csv", "/data/input").isEmpty)
    } finally {
      Files.deleteIfExists(ignoreFile)
    }
  }

  test("matches windows separators and archive entry relative paths") {
    val matcher = IgnoreMatcher.fromSources(Seq("backup/**", "__MACOSX/**"), None)

    assert(matcher.matched("""C:\data\input\backup\old.csv""", """C:\data\input""").contains("backup/**"))
    assert(matcher.matched("bundle.zip!__MACOSX/metadata.txt", "/data/input").contains("__MACOSX/**"))
  }

  test("directory patterns only match directories") {
    val matcher = IgnoreMatcher.fromSources(Seq("logs/"), None)

    assert(matcher.matched("/data/input/logs", "/data/input").isEmpty)
    assert(matcher.matched("/data/input/logs", "/data/input", isDirectory = true).contains("logs/"))
  }

  test("supports root-anchored patterns with a leading slash") {
    val matcher = IgnoreMatcher.fromSources(Seq("/backup/**", "/logs/"), None)

    assert(matcher.matched("/data/input/backup/old.csv", "/data/input").contains("/backup/**"))
    assert(matcher.matched("/data/input/logs", "/data/input", isDirectory = true).contains("/logs/"))
  }
}
