package io.github.jonggeun2001.privyspark.report

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JsonCodecSpec extends AnyFunSuite {
  test("extractJsonStringField ignores field-like tokens inside string literals") {
    val json =
      """{"reason":"contains escaped token: \"file_checksum\":\"deadbeef\"","file_checksum":"cafebabe"}"""

    assert(JsonCodec.extractJsonStringField(json, "file_checksum").contains("cafebabe"))
  }

  test("extractJsonLongField ignores numeric field-like tokens inside string literals") {
    val json =
      """{"reason":"contains escaped token: \"file_size\":123","file_size":456}"""

    assert(JsonCodec.extractJsonLongField(json, "file_size").contains(456L))
  }
}
