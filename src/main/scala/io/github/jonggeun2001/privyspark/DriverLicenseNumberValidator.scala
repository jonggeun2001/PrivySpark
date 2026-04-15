package io.github.jonggeun2001.privyspark

import java.util.regex.Pattern

object DriverLicenseNumberValidator {
  final case class CandidateMatch(candidate: String, start: Int, end: Int)

  private val KoreanRegionNames = Seq(
    "서울",
    "부산",
    "경기",
    "강원",
    "충북",
    "충남",
    "전북",
    "전남",
    "경북",
    "경남",
    "제주",
    "대구",
    "인천",
    "광주",
    "대전",
    "울산"
  )
  private val KoreanRegionAlternation = KoreanRegionNames.mkString("(?:", "|", ")")
  private val CandidatePattern = Pattern.compile(
    s"(?:(?<![0-9])(?:[0-9]{10}|[0-9]{12}|[0-9]{2}-[0-9]{6}-[0-9]{2}|[0-9]{2}-[0-9]{2}-[0-9]{6}-[0-9]{2})(?![0-9])|(?<![가-힣A-Za-z0-9])$KoreanRegionAlternation\\s*(?:[0-9]{10}|[0-9]{2}\\s*-\\s*[0-9]{6}\\s*-\\s*[0-9]{2})(?![가-힣A-Za-z0-9]))"
  )
  private val CurrentRegionCodes: Set[String] =
    ((11 to 26).map(code => f"$code%02d") :+ "28").toSet

  private val SupportedNumericFormats = Seq(
    "^[0-9]{10}$",
    "^[0-9]{12}$",
    "^[0-9]{2}-[0-9]{6}-[0-9]{2}$",
    "^[0-9]{2}-[0-9]{2}-[0-9]{6}-[0-9]{2}$"
  )

  def isValid(raw: String): Boolean = {
    Option(raw).exists { value =>
      val trimmed = value.trim
      if (isValidNamedRegionFormat(trimmed)) {
        true
      } else if (!SupportedNumericFormats.exists(trimmed.matches)) {
        false
      } else {
        val normalized = trimmed.replace("-", "")
        normalized.length match {
          case 10 => normalized.forall(_.isDigit)
          case 12 => normalized.forall(_.isDigit) && CurrentRegionCodes.contains(normalized.substring(0, 2))
          case _ => false
        }
      }
    }
  }

  private def isValidNamedRegionFormat(raw: String): Boolean = {
    val normalized = raw.replaceAll("\\s+", "")
    KoreanRegionNames.find(normalized.startsWith).exists { regionName =>
      val remainder = normalized.substring(regionName.length)
      remainder.matches("^[0-9]{10}$") || remainder.matches("^[0-9]{2}-[0-9]{6}-[0-9]{2}$")
    }
  }

  def findFirstValidCandidate(raw: String): Option[CandidateMatch] = {
    Option(raw).flatMap { value =>
      val matcher = CandidatePattern.matcher(value)
      while (matcher.find()) {
        val candidate = matcher.group()
        if (isValid(candidate)) {
          return Some(CandidateMatch(candidate, matcher.start(), matcher.end()))
        }
      }
      None
    }
  }

  def containsValidCandidate(raw: String): Boolean = {
    findFirstValidCandidate(raw).nonEmpty
  }
}
