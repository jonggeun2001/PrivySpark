package io.github.jonggeun2001.privyspark.util

import java.time.{OffsetDateTime, ZoneId}
import java.time.format.DateTimeFormatter

private[privyspark] sealed abstract class DriverLogLevel(val priority: Int, val label: String)

private[privyspark] object DriverLogLevel {
  case object Off extends DriverLogLevel(-1, "OFF")
  case object Error extends DriverLogLevel(0, "ERROR")
  case object Warn extends DriverLogLevel(1, "WARN")
  case object Info extends DriverLogLevel(2, "INFO")
  case object Debug extends DriverLogLevel(3, "DEBUG")

  val Default: DriverLogLevel = Warn

  def parse(rawValue: String): Option[DriverLogLevel] = {
    Option(rawValue).map(_.trim.toLowerCase).filter(_.nonEmpty).flatMap {
      case "debug" | "1" | "true" | "yes" | "on" => Some(Debug)
      case "info" => Some(Info)
      case "warn" | "warning" | "0" | "false" | "no" => Some(Warn)
      case "error" => Some(Error)
      case "off" | "none" | "silent" => Some(Off)
      case _ => None
    }
  }
}

private[privyspark] object DriverLogger {
  val PropertyName = "privyspark.debug"
  val EnvName = "PRIVYSPARK_DEBUG"

  @volatile private var currentLogLevelCache: DriverLogLevel = _

  def currentLogLevel: DriverLogLevel = {
    val cached = currentLogLevelCache
    if (cached != null) {
      cached
    } else {
      val resolved = resolveLogLevel(sys.props.get(PropertyName), sys.env.get(EnvName))
      currentLogLevelCache = resolved
      resolved
    }
  }

  private[privyspark] def resolveLogLevel(propertyValue: Option[String], envValue: Option[String]): DriverLogLevel = {
    propertyValue
      .flatMap(DriverLogLevel.parse)
      .orElse(envValue.flatMap(DriverLogLevel.parse))
      .getOrElse(DriverLogLevel.Default)
  }

  def resetCache(): Unit = {
    currentLogLevelCache = null
  }

  def debug(event: String, fields: (String, Any)*): Unit = {
    log(DriverLogLevel.Debug, event, fields: _*)
  }

  def info(event: String, fields: (String, Any)*): Unit = {
    log(DriverLogLevel.Info, event, fields: _*)
  }

  def warn(event: String, fields: (String, Any)*): Unit = {
    log(DriverLogLevel.Warn, event, fields: _*)
  }

  def error(event: String, fields: (String, Any)*): Unit = {
    log(DriverLogLevel.Error, event, fields: _*)
  }

  def emitAlways(level: DriverLogLevel, event: String, fields: (String, Any)*): Unit = {
    System.err.println(render(level, event, fields: _*))
  }

  private def log(level: DriverLogLevel, event: String, fields: (String, Any)*): Unit = {
    if (!shouldLog(level)) {
      return
    }

    System.err.println(render(level, event, fields: _*))
  }

  private def render(level: DriverLogLevel, event: String, fields: (String, Any)*): String = {
    val suffix = if (fields.isEmpty) {
      ""
    } else {
      fields.map {
        case (key, value) =>
          val renderedValue = renderValue(value)
          s"$key=$renderedValue"
      }.mkString(" ", " ", "")
    }

    s"[PrivySpark][${level.label}][${currentTimestamp}] $event$suffix"
  }

  private def currentTimestamp: String = {
    OffsetDateTime.now(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
  }

  private def renderValue(value: Any): String = {
    val raw = if (value == null) "null" else value.toString
    if (raw.nonEmpty && raw.forall(isSafeUnquotedCharacter)) {
      raw
    } else {
      "\"" + escapeValue(raw) + "\""
    }
  }

  private def isSafeUnquotedCharacter(char: Char): Boolean = {
    char.isLetterOrDigit || "-._:/@%+".contains(char)
  }

  private def escapeValue(raw: String): String = {
    raw.flatMap {
      case '\\' => "\\\\"
      case '"' => "\\\""
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case char if Character.isISOControl(char) => f"\\u${char.toInt}%04x"
      case char => char.toString
    }
  }

  private def shouldLog(level: DriverLogLevel): Boolean = {
    currentLogLevel.priority >= level.priority
  }
}
