package io.github.jonggeun2001.privyspark.review

import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets
import scala.util.Try

object ReviewScopeFingerprintCodec {
  private val EntrySeparator = "\\|"
  private val EncodedEntrySeparator = "|"
  private val FieldSeparator = ":"
  private val ExpectedFieldCount = 5

  def encode(fingerprints: Seq[RecordedFileFingerprint]): String = {
    fingerprints
      .sortBy(_.fileIdentifier)
      .map { fingerprint =>
        Seq(
          encodeToken(fingerprint.fileIdentifier),
          fingerprint.fileSize.toString,
          fingerprint.fileMtimeEpochMs.toString,
          encodeToken(fingerprint.fileChecksumAlgo),
          encodeToken(fingerprint.fileChecksum)
        ).mkString(FieldSeparator)
      }
      .mkString(EncodedEntrySeparator)
  }

  def decode(rawValue: String): Either[String, Seq[RecordedFileFingerprint]] = {
    val normalized = Option(rawValue).map(_.trim).getOrElse("")
    if (normalized.isEmpty) {
      Right(Seq.empty)
    } else {
      normalized
        .split(EntrySeparator, -1)
        .toSeq
        .zipWithIndex
        .foldLeft[Either[String, Vector[RecordedFileFingerprint]]](Right(Vector.empty)) {
          case (Right(entries), (encodedEntry, index)) =>
            decodeEntry(encodedEntry, index).map(entries :+ _)
          case (left, _) =>
            left
        }
        .map(_.toSeq)
    }
  }

  private def decodeEntry(
    encodedEntry: String,
    index: Int
  ): Either[String, RecordedFileFingerprint] = {
    val parts = encodedEntry.split(FieldSeparator, -1)
    if (parts.length != ExpectedFieldCount) {
      Left(s"Malformed review_scope_file_fingerprints entry at index $index")
    } else {
      for {
        fileSize <- parseLong(parts(1), s"file_size at index $index")
        fileMtimeEpochMs <- parseLong(parts(2), s"file_mtime_epoch_ms at index $index")
      } yield RecordedFileFingerprint(
        fileIdentifier = decodeToken(parts(0)),
        fileSize = fileSize,
        fileMtimeEpochMs = fileMtimeEpochMs,
        fileChecksumAlgo = decodeToken(parts(3)),
        fileChecksum = decodeToken(parts(4))
      )
    }
  }

  private def parseLong(rawValue: String, fieldName: String): Either[String, Long] =
    Try(rawValue.toLong).toOption.toRight(s"Malformed $fieldName in review_scope_file_fingerprints")

  private def encodeToken(value: String): String =
    URLEncoder.encode(Option(value).getOrElse(""), StandardCharsets.UTF_8.name())

  private def decodeToken(value: String): String =
    URLDecoder.decode(Option(value).getOrElse(""), StandardCharsets.UTF_8.name())
}
