package io.github.jonggeun2001.privyspark

object DriverLicenseNumberValidator {
  private val CandidatePattern =
    "(?<![0-9])(?:[0-9]{10}|[0-9]{12}|[0-9]{2}-[0-9]{6}-[0-9]{2}|[0-9]{2}-[0-9]{2}-[0-9]{6}-[0-9]{2})(?![0-9])".r
  private val CurrentRegionCodes: Set[String] =
    ((11 to 26).map(code => f"$code%02d") :+ "28").toSet

  private val SupportedFormats = Seq(
    "^[0-9]{10}$",
    "^[0-9]{12}$",
    "^[0-9]{2}-[0-9]{6}-[0-9]{2}$",
    "^[0-9]{2}-[0-9]{2}-[0-9]{6}-[0-9]{2}$"
  )

  def isValid(raw: String): Boolean = {
    Option(raw).exists { value =>
      val trimmed = value.trim
      if (!SupportedFormats.exists(trimmed.matches)) {
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

  def containsValidCandidate(raw: String): Boolean = {
    Option(raw).exists(value => CandidatePattern.findAllIn(value).exists(isValid))
  }
}
