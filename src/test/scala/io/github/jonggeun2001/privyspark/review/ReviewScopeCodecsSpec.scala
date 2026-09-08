package io.github.jonggeun2001.privyspark.review

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReviewScopeCodecsSpec extends AnyFunSuite {
  test("identifiers preserve Unicode, separators, leading spaces and duplicate order") {
    val identifiers = Seq(" 고객|a+b%:c.csv", "archive.zip!folder/a.csv", "book.xlsx#시트", " 고객|a+b%:c.csv")
    assert(ReviewScopeIdentifierCodec.decode(ReviewScopeIdentifierCodec.encode(identifiers)) == Right(identifiers))
    assert(ReviewScopeIdentifierCodec.decode("a%7Cb|%20c%2Bd") == Right(Seq("a|b", " c+d")))
  }

  test("null and whitespace scope strings decode to an empty scope") {
    Seq(null, "", " \t ").foreach { raw =>
      assert(ReviewScopeIdentifierCodec.decode(raw) == Right(Seq.empty))
      assert(ReviewScopeFingerprintCodec.decode(raw) == Right(Seq.empty))
    }
    assert(ReviewScopeIdentifierCodec.encode(Seq.empty) == "")
    assert(ReviewScopeFingerprintCodec.encode(Seq.empty) == "")
  }

  test("identifier empty entries are rejected instead of silently dropping files") {
    Seq("|a", "a|", "a||b").foreach { raw => assert(ReviewScopeIdentifierCodec.decode(raw).isLeft) }
  }

  test("malformed identifier percent escapes retain the decoder failure") {
    Seq("%", "%ZZ").foreach { raw =>
      assertThrows[IllegalArgumentException](ReviewScopeIdentifierCodec.decode(raw))
    }
  }

  test("fingerprints round-trip reserved characters in deterministic file order") {
    val a = RecordedFileFingerprint(" a|b.csv", 0L, 123L, "CRC32", "ab:cd%")
    val b = RecordedFileFingerprint("z.xlsx#시트", Long.MaxValue, 456L, "CRC32", "1234")
    assert(ReviewScopeFingerprintCodec.decode(ReviewScopeFingerprintCodec.encode(Seq(b, a))) == Right(Seq(a, b)))
    assert(ReviewScopeFingerprintCodec.decode("file.csv:12:34:CRC32:abcd") == Right(Seq(
      RecordedFileFingerprint("file.csv", 12L, 34L, "CRC32", "abcd")
    )))
  }

  test("fingerprint field counts and malformed or overflowing numeric values are rejected") {
    Seq("a:1:2:CRC32", "a:1:2:CRC32:hash:extra", "a:no:2:CRC32:hash", "a:1:no:CRC32:hash", "a:9223372036854775808:2:CRC32:hash")
      .foreach(raw => assert(ReviewScopeFingerprintCodec.decode(raw).isLeft, raw))
    assert(ReviewScopeFingerprintCodec.decode("a:1:2:CRC32:hash|invalid").left.get.contains("index 1"))
  }
}
