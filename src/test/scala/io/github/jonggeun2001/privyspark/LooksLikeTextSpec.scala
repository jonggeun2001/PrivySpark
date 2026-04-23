package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.format.ByteProbe
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatestplus.junit.JUnitRunner

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

@RunWith(classOf[JUnitRunner])
class LooksLikeTextSpec extends AnyFunSuite {
  private val EucKr = Charset.forName("EUC-KR")
  private val cases = Table(
    ("label", "bytes", "expected"),
    ("plain ascii text", "alice@example.com\nbob@example.com\n".getBytes(StandardCharsets.UTF_8), true),
    ("euc-kr korean text", "이름=홍길동\n".getBytes(EucKr), true),
    ("content with null byte", Array[Byte](0x61.toByte, 0x00.toByte, 0x62.toByte), false),
    ("record separator delimited csv", "a\u001eb\u001ec\u001ed\n".getBytes(StandardCharsets.UTF_8), true),
    ("file separator delimited text", "a\u001cb\u001cc\u001cd\n".getBytes(StandardCharsets.UTF_8), true),
    ("group separator delimited text", "a\u001db\u001dc\u001dd\n".getBytes(StandardCharsets.UTF_8), true),
    ("unit separator delimited text", "a\u001fb\u001fc\u001fd\n".getBytes(StandardCharsets.UTF_8), true),
    ("unsupported escape control density", Array.fill[Byte](12)(0x1b.toByte) ++ "ok".getBytes(StandardCharsets.UTF_8), false),
    ("invalid utf8 sequence", Array[Byte](0xc3.toByte, 0x28.toByte), false),
  )

  test("looksLikeText classifies utf-8 text and suspicious control bytes") {
    forAll(cases) { (label: String, bytes: Array[Byte], expected: Boolean) =>
      assert(
        ByteProbe.looksLikeText(bytes, false) == expected,
        label
      )
    }
  }
}
