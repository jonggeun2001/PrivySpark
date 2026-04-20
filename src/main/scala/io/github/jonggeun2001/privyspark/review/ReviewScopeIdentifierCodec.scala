package io.github.jonggeun2001.privyspark.review

import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets

object ReviewScopeIdentifierCodec {
  private val EntrySeparator = "\\|"
  private val EncodedEntrySeparator = "|"

  def encode(identifiers: Seq[String]): String = {
    identifiers
      .map(identifier => URLEncoder.encode(Option(identifier).getOrElse(""), StandardCharsets.UTF_8.name()))
      .mkString(EncodedEntrySeparator)
  }

  def decode(rawValue: String): Either[String, Seq[String]] = {
    val normalized = Option(rawValue).map(_.trim).getOrElse("")
    if (normalized.isEmpty) {
      Right(Seq.empty)
    } else {
      val decoded = normalized
        .split(EntrySeparator, -1)
        .toSeq
        .map(token => URLDecoder.decode(token, StandardCharsets.UTF_8.name()))

      if (decoded.exists(_.isEmpty)) {
        Left("Malformed review_scope_file_identifiers entry")
      } else {
        Right(decoded)
      }
    }
  }
}
