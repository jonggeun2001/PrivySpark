package io.github.jonggeun2001.privyspark.format

import io.github.jonggeun2001.privyspark.model.{ProbeSample, ScanReadOptions}
import org.apache.hadoop.fs.Path

import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.nio.charset.{CharacterCodingException, CodingErrorAction}

private[privyspark] object ByteProbe {
  val TextFormat = "text"
  val Utf8Encoding = "UTF-8"
  val EucKrEncoding = "EUC-KR"
  val MagicProbeByteLimit = 4
  val TextProbeByteLimit = 512
  val ParquetMagicBytes = Array[Byte]('P'.toByte, 'A'.toByte, 'R'.toByte, '1'.toByte)
  val OrcMagicBytes = Array[Byte]('O'.toByte, 'R'.toByte, 'C'.toByte)
  private val EucKrCharset = Charset.forName(EucKrEncoding)

  def probe(conf: org.apache.hadoop.conf.Configuration, path: String, maxBytes: Int): ProbeSample = {
    val sourcePath = new Path(path)
    val fs = sourcePath.getFileSystem(conf)
    val inputStream = fs.open(sourcePath)
    try {
      val buffer = new Array[Byte](maxBytes)
      var totalBytesRead = 0
      var continueReading = true

      while (continueReading && totalBytesRead < maxBytes) {
        val bytesRead = inputStream.read(buffer, totalBytesRead, maxBytes - totalBytesRead)
        if (bytesRead < 0) {
          continueReading = false
        } else if (bytesRead == 0) {
          val singleByte = inputStream.read()
          if (singleByte < 0) {
            continueReading = false
          } else {
            buffer(totalBytesRead) = singleByte.toByte
            totalBytesRead += 1
          }
        } else {
          totalBytesRead += bytesRead
        }
      }

      val truncated =
        totalBytesRead >= maxBytes && inputStream.read() >= 0
      val bytes =
        if (totalBytesRead <= 0) Array.emptyByteArray else java.util.Arrays.copyOf(buffer, totalBytesRead)
      ProbeSample(bytes, truncated)
    } finally {
      inputStream.close()
    }
  }

  def inferMagicByteFormat(bytes: Array[Byte]): Option[String] = {
    if (bytes.length >= ParquetMagicBytes.length && ParquetMagicBytes.indices.forall(index => bytes(index) == ParquetMagicBytes(index))) {
      Some("parquet")
    } else if (bytes.length >= OrcMagicBytes.length && OrcMagicBytes.indices.forall(index => bytes(index) == OrcMagicBytes(index))) {
      Some("orc")
    } else {
      None
    }
  }

  def inferTextFormat(
    bytes: Array[Byte],
    allowIncompleteTrailingSequence: Boolean = false
  ): Option[String] = {
    if (looksLikeText(bytes, allowIncompleteTrailingSequence)) Some(TextFormat) else None
  }

  def inferTextReadOptions(
    bytes: Array[Byte],
    allowIncompleteTrailingSequence: Boolean = false
  ): Option[ScanReadOptions] = {
    if (looksLikeText(bytes, allowIncompleteTrailingSequence)) {
      detectTextEncoding(bytes, allowIncompleteTrailingSequence).map {
        case EucKrEncoding => ScanReadOptions(textEncoding = Some(EucKrEncoding))
        case _ => ScanReadOptions()
      }
    } else {
      None
    }
  }

  def looksLikeText(bytes: Array[Byte], allowIncompleteTrailingSequence: Boolean): Boolean = {
    if (bytes.isEmpty) {
      true
    } else if (bytes.contains(0.toByte)) {
      false
    } else if (detectTextEncoding(bytes, allowIncompleteTrailingSequence).isEmpty) {
      false
    } else {
      val suspiciousControlBytes = bytes.count { rawByte =>
        val byte = rawByte & 0xff
        byte < 0x20 &&
          byte != 0x09 &&
          byte != 0x0A &&
          byte != 0x0D &&
          byte != 0x1C &&
          byte != 0x1D &&
          byte != 0x1E &&
          byte != 0x1F
      }
      suspiciousControlBytes * 10 <= bytes.length
    }
  }

  private def detectTextEncoding(bytes: Array[Byte], allowIncompleteTrailingSequence: Boolean): Option[String] = {
    if (isValidUtf8(bytes, allowIncompleteTrailingSequence)) {
      Some(Utf8Encoding)
    } else if (isValidEucKr(bytes, allowIncompleteTrailingSequence)) {
      Some(EucKrEncoding)
    } else {
      None
    }
  }

  private def isValidUtf8(bytes: Array[Byte], allowIncompleteTrailingSequence: Boolean): Boolean = {
    val trailingTrimBytes =
      if (allowIncompleteTrailingSequence) incompleteTrailingUtf8Bytes(bytes) else 0
    val candidateBytes =
      if (trailingTrimBytes <= 0) bytes
      else java.util.Arrays.copyOf(bytes, bytes.length - trailingTrimBytes)
    val decoder = StandardCharsets.UTF_8
      .newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)

    try {
      decoder.decode(ByteBuffer.wrap(candidateBytes))
      true
    } catch {
      case _: CharacterCodingException => false
    }
  }

  private def isValidEucKr(bytes: Array[Byte], allowIncompleteTrailingSequence: Boolean): Boolean = {
    isValidCharset(bytes, EucKrCharset) || {
      allowIncompleteTrailingSequence &&
        bytes.nonEmpty &&
        isPotentialEucKrLeadByte(bytes.last) &&
        isValidCharset(java.util.Arrays.copyOf(bytes, bytes.length - 1), EucKrCharset)
    }
  }

  private def isValidCharset(bytes: Array[Byte], charset: Charset): Boolean = {
    val decoder = charset
      .newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)

    try {
      decoder.decode(ByteBuffer.wrap(bytes))
      true
    } catch {
      case _: CharacterCodingException => false
    }
  }

  private def isPotentialEucKrLeadByte(rawByte: Byte): Boolean = {
    val byte = rawByte & 0xff
    byte >= 0x81 && byte <= 0xFE
  }

  private def incompleteTrailingUtf8Bytes(bytes: Array[Byte]): Int = {
    if (bytes.isEmpty) {
      0
    } else {
      var index = bytes.length - 1
      var continuationBytes = 0

      while (index >= 0 && isUtf8ContinuationByte(bytes(index))) {
        continuationBytes += 1
        index -= 1
      }

      if (index < 0) {
        0
      } else {
        expectedUtf8SequenceLength(bytes(index) & 0xff) match {
          case Some(expectedLength) =>
            val observedLength = continuationBytes + 1
            if (observedLength < expectedLength && isValidIncompleteUtf8Prefix(bytes, index, observedLength, expectedLength)) {
              observedLength
            } else {
              0
            }
          case None =>
            0
        }
      }
    }
  }

  private def isUtf8ContinuationByte(rawByte: Byte): Boolean = {
    ((rawByte & 0xff) & 0xC0) == 0x80
  }

  private def expectedUtf8SequenceLength(leadByte: Int): Option[Int] = {
    if (leadByte <= 0x7F) {
      Some(1)
    } else if (leadByte >= 0xC2 && leadByte <= 0xDF) {
      Some(2)
    } else if (leadByte >= 0xE0 && leadByte <= 0xEF) {
      Some(3)
    } else if (leadByte >= 0xF0 && leadByte <= 0xF4) {
      Some(4)
    } else {
      None
    }
  }

  private def isValidIncompleteUtf8Prefix(
    bytes: Array[Byte],
    leadIndex: Int,
    observedLength: Int,
    expectedLength: Int
  ): Boolean = {
    val leadByte = bytes(leadIndex) & 0xff

    if (observedLength <= 0 || observedLength >= expectedLength) {
      false
    } else if (observedLength == 1) {
      true
    } else if (!isValidUtf8FirstContinuation(leadByte, bytes(leadIndex + 1) & 0xff)) {
      false
    } else {
      var offset = 2
      var valid = true

      while (offset < observedLength && valid) {
        valid = isUtf8ContinuationByte(bytes(leadIndex + offset))
        offset += 1
      }

      valid
    }
  }

  private def isValidUtf8FirstContinuation(leadByte: Int, continuationByte: Int): Boolean = {
    if (leadByte >= 0xC2 && leadByte <= 0xDF) {
      continuationByte >= 0x80 && continuationByte <= 0xBF
    } else if (leadByte == 0xE0) {
      continuationByte >= 0xA0 && continuationByte <= 0xBF
    } else if ((leadByte >= 0xE1 && leadByte <= 0xEC) || (leadByte >= 0xEE && leadByte <= 0xEF)) {
      continuationByte >= 0x80 && continuationByte <= 0xBF
    } else if (leadByte == 0xED) {
      continuationByte >= 0x80 && continuationByte <= 0x9F
    } else if (leadByte == 0xF0) {
      continuationByte >= 0x90 && continuationByte <= 0xBF
    } else if (leadByte >= 0xF1 && leadByte <= 0xF3) {
      continuationByte >= 0x80 && continuationByte <= 0xBF
    } else if (leadByte == 0xF4) {
      continuationByte >= 0x80 && continuationByte <= 0x8F
    } else {
      false
    }
  }

  def detectPhysicalFormat(
    conf: org.apache.hadoop.conf.Configuration,
    filePath: String
  ): Option[String] = {
    detectPhysicalFormatWithReadOptions(conf, filePath).map(_._1)
  }

  def detectPhysicalFormatWithReadOptions(
    conf: org.apache.hadoop.conf.Configuration,
    filePath: String
  ): Option[(String, ScanReadOptions)] = {
    val extensionFormat = FormatDetector.infer(filePath)
    if (extensionFormat.isDefined) {
      extensionFormat.map(format => (format, ScanReadOptions()))
    } else if (FormatDetector.shouldSkipProbe(filePath)) {
      None
    } else {
      val probeSample = probe(conf, filePath, TextProbeByteLimit)
      val magicProbeBytes = java.util.Arrays.copyOf(probeSample.bytes, math.min(probeSample.bytes.length, MagicProbeByteLimit))
      inferMagicByteFormat(magicProbeBytes)
        .map(format => (format, ScanReadOptions()))
        .orElse {
          inferTextReadOptions(
            probeSample.bytes,
            allowIncompleteTrailingSequence = probeSample.truncated
          ).map(readOptions => (TextFormat, readOptions))
        }
    }
  }

  def shouldProbeForFormat(filePath: String, pathInferredFormat: Option[String]): Boolean =
    pathInferredFormat.isEmpty && !FormatDetector.shouldSkipProbe(filePath)

  def isZeroBytePhysicalFile(conf: org.apache.hadoop.conf.Configuration, filePath: String): Boolean = {
    val sourcePath = new Path(filePath)
    val fs = sourcePath.getFileSystem(conf)
    fs.getFileStatus(sourcePath).getLen == 0L
  }
}
