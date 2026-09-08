package io.github.jonggeun2001.privyspark.review.collect

import io.github.jonggeun2001.privyspark.PrivySparkSpecFixtures
import io.github.jonggeun2001.privyspark.fsio.FaultInjectingLocalFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.AccessControlException
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.file.Files

@RunWith(classOf[JUnitRunner])
class ResponseEnvelopeReaderSpec extends AnyFunSuite with PrivySparkSpecFixtures {
  private val json = """{"schema_version":1,"scan_path":"/input","responder":"owner1","responded_at":"2026-01-01T00:00:00Z","responses":[{"finding_key":"key","hive_table_fqn":"catalog.db.table","sample_row_count":10,"match_count":2,"non_empty_match_ratio":0.2}]}"""

  test("parses BOM-prefixed envelopes and derives Hive names and absent item defaults") {
    val result = ResponseEnvelopeReader.parseEnvelope("source.json", "\uFEFF" + json)
    assert(result.isRight)
    val envelope = result.right.get
    assert(envelope.sourcePath == "source.json")
    assert(envelope.scanPath == "/input")
    assert(envelope.responder == "owner1")
    val item = envelope.responses.head
    assert(item.hiveDatabase == "catalog.db")
    assert(item.hiveTable == "table")
    assert(item.sampleRowCount == 10L)
    assert(item.matchCount == 2L)
    assert(item.nonEmptyMatchRatio == 0.2)
    assert(item.decision == "")
    assert(item.fileIdentifier == "")
  }

  test("accepts a string schema version and explicit Hive fields override derived names") {
    val input = """{"schema_version":"1","responses":[{"hive_table_fqn":"db.table","hive_database":"explicit_db","hive_table":"explicit_table"}]}"""
    val item = ResponseEnvelopeReader.parseEnvelope("source.json", input).right.get.responses.head
    assert(item.hiveDatabase == "explicit_db")
    assert(item.hiveTable == "explicit_table")
    assert(item.sampleRowCount == 0L)
    assert(item.matchCount == 0L)
  }

  test("rejects missing or unsupported schema versions") {
    Seq("", "not-json", "{}", "{\"schema_version\":2}", "{\"schema_version\":\"invalid\"}").foreach { input =>
      assert(ResponseEnvelopeReader.parseEnvelope("source.json", input).isLeft, input)
    }
  }

  test("missing response arrays are parsed as empty and rejected by envelope validation") {
    val envelope = ResponseEnvelopeReader.parseEnvelope("source.json",
      """{"schema_version":1,"scan_path":"/input","responder":"owner1","responded_at":"2026-01-01T00:00:00Z"}""").right.get
    assert(envelope.responses.isEmpty)
    assert(ResponseValidator.validateEnvelope(envelope).contains("responses must not be empty"))
  }

  test("inbox reads sort JSON files, ignore directories and retain malformed response failures") {
    val root = Files.createTempDirectory("privyspark-inbox-")
    try {
      writeText(root.resolve("b.json"), json)
      writeText(root.resolve("a.json"), "{}")
      writeText(root.resolve("notes.txt"), "ignored")
      Files.createDirectory(root.resolve("nested.json"))
      val responses = ResponseEnvelopeReader.readResponseEnvelopes(new Configuration(), root.toString)
      assert(responses.size == 2)
      assert(responses.head.left.get.sourcePath.endsWith("/a.json"))
      assert(responses(1).right.get.sourcePath.endsWith("/b.json"))
      assert(ResponseEnvelopeReader.readResponseEnvelopes(new Configuration(), root.resolve("missing").toString).isEmpty)
    } finally deleteRecursively(root)
  }

  test("inbox read permission errors propagate instead of returning an empty inbox") {
    val root = Files.createTempDirectory("privyspark-inbox-denied-")
    try {
      writeText(root.resolve("blocked.json"), json)
      val conf = FaultInjectingLocalFileSystem.configuration("open", "blocked.json")
      intercept[AccessControlException] {
        ResponseEnvelopeReader.readResponseEnvelopes(conf, root.toString)
      }
      assert(Files.exists(root.resolve("blocked.json")))
    } finally deleteRecursively(root)
  }
}
