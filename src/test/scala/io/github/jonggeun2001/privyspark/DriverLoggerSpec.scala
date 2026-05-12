package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.util.{DriverLogLevel, DriverLogger}
import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.time.OffsetDateTime
import java.util.TimeZone

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DriverLoggerSpec extends AnyFunSuite {
  private val LogLinePattern = """^\[PrivySpark\]\[[A-Z]+\]\[([^\]]+)\] .*$""".r

  test("resolves driver log levels from property values with compatibility aliases") {
    assert(withDriverLogLevel("debug") { DriverLogger.currentLogLevel } == DriverLogLevel.Debug)
    assert(withDriverLogLevel("true") { DriverLogger.currentLogLevel } == DriverLogLevel.Debug)
    assert(withDriverLogLevel("info") { DriverLogger.currentLogLevel } == DriverLogLevel.Info)
    assert(withDriverLogLevel("warn") { DriverLogger.currentLogLevel } == DriverLogLevel.Warn)
    assert(withDriverLogLevel("error") { DriverLogger.currentLogLevel } == DriverLogLevel.Error)
    assert(withDriverLogLevel("false") { DriverLogger.currentLogLevel } == DriverLogLevel.Warn)
    assert(withDriverLogLevel("off") { DriverLogger.currentLogLevel } == DriverLogLevel.Off)
  }

  test("falls back to environment log level when property value is invalid") {
    assert(DriverLogger.resolveLogLevel(Some("bogus"), Some("info")) == DriverLogLevel.Info)
    assert(DriverLogger.resolveLogLevel(Some("debug"), Some("warn")) == DriverLogLevel.Debug)
    assert(DriverLogger.resolveLogLevel(Some(""), Some("error")) == DriverLogLevel.Error)
  }

  test("formats driver log timestamps with fixed millisecond precision by default") {
    assert(DriverLogger.formatTimestamp(OffsetDateTime.parse("2026-05-12T11:22:33.100+09:00")) == "2026-05-12T11:22:33.100+09:00")
    assert(DriverLogger.formatTimestamp(OffsetDateTime.parse("2026-05-12T11:22:33.120+09:00")) == "2026-05-12T11:22:33.120+09:00")
    assert(DriverLogger.formatTimestamp(OffsetDateTime.parse("2026-05-12T11:22:33.000+09:00")) == "2026-05-12T11:22:33.000+09:00")
  }

  test("allows overriding driver log timestamp pattern with system property") {
    val timestamp = OffsetDateTime.parse("2026-05-12T11:22:33.120+09:00")

    assert(withDriverTimestampPattern("uuuu-MM-dd HH:mm:ss XXX") {
      DriverLogger.formatTimestamp(timestamp)
    } == "2026-05-12 11:22:33 +09:00")
  }

  test("falls back to default driver log timestamp pattern when override is invalid") {
    val timestamp = OffsetDateTime.parse("2026-05-12T11:22:33.100+09:00")

    assert(withDriverTimestampPattern("not a valid [ pattern") {
      DriverLogger.formatTimestamp(timestamp)
    } == "2026-05-12T11:22:33.100+09:00")
  }

  test("filters structured driver logs by configured level") {
    val warnLogs = captureStderr {
      withDriverLogLevel("warn") {
        DriverLogger.info("scan_start", "input_path" -> "/data/input")
        DriverLogger.warn("group_scan_fallback", "directory" -> "/data/input/users", "reason" -> "schema_mismatch")
        DriverLogger.error("scan_group_failed", "directory" -> "/data/input/users", "reason" -> "broken_file")
      }
    }

    assert(!warnLogs.contains("scan_start"))
    assert(warnLogs.linesIterator.exists(_.matches("""\[PrivySpark\]\[WARN\]\[\d{4}-\d{2}-\d{2}T[^\]]+(?:Z|[+-]\d{2}:\d{2})\] group_scan_fallback.*directory=/data/input/users.*""")))
    assert(warnLogs.linesIterator.exists(_.matches("""\[PrivySpark\]\[ERROR\]\[\d{4}-\d{2}-\d{2}T[^\]]+(?:Z|[+-]\d{2}:\d{2})\] scan_group_failed.*reason=broken_file.*""")))

    val infoLogs = captureStderr {
      withDriverLogLevel("info") {
        DriverLogger.info("scan_start", "input_path" -> "/data/input")
      }
    }

    assert(infoLogs.linesIterator.exists(_.matches("""\[PrivySpark\]\[INFO\]\[\d{4}-\d{2}-\d{2}T[^\]]+(?:Z|[+-]\d{2}:\d{2})\] scan_start.*input_path=/data/input.*""")))
  }

  test("emits forced error logs even when driver log level is off") {
    val logs = captureStderr {
      withDriverLogLevel("off") {
        DriverLogger.emitAlways(DriverLogLevel.Error, "scan_failed", "reason" -> "boom")
      }
    }

    assert(logs.linesIterator.exists(_.matches("""\[PrivySpark\]\[ERROR\]\[\d{4}-\d{2}-\d{2}T[^\]]+(?:Z|[+-]\d{2}:\d{2})\] scan_failed.*reason=boom.*""")))
  }

  test("renders driver log timestamps in the JVM local time zone") {
    val logs = withDefaultTimeZone("Asia/Seoul") {
      captureStderr {
        DriverLogger.emitAlways(DriverLogLevel.Info, "local_time_check")
      }
    }

    val timestamp = extractTimestamp(logs)
    val parsed = OffsetDateTime.parse(timestamp)
    assert(parsed.getOffset.getId == "+09:00")
    assert(!timestamp.endsWith("Z"))
  }

  test("quotes structured field values when they contain unsafe characters") {
    val logs = captureStderr {
      withDriverLogLevel("warn") {
        DriverLogger.warn("staging_cleanup_failed", "reason" -> "delete returned=false\nretry")
      }
    }

    assert(logs.linesIterator.size == 1)
    assert(logs.contains("""reason="delete returned=false\nretry""""))
  }

  private def withDriverLogLevel[A](level: String)(block: => A): A = {
    val previous = sys.props.get("privyspark.debug")
    DriverLogger.resetCache()
    System.setProperty("privyspark.debug", level)
    try {
      block
    } finally {
      previous match {
        case Some(value) => System.setProperty("privyspark.debug", value)
        case None => System.clearProperty("privyspark.debug")
      }
      DriverLogger.resetCache()
    }
  }

  private def withDriverTimestampPattern[A](pattern: String)(block: => A): A = {
    val previous = sys.props.get("privyspark.debug.timestampPattern")
    DriverLogger.resetCache()
    System.setProperty("privyspark.debug.timestampPattern", pattern)
    try {
      block
    } finally {
      previous match {
        case Some(value) => System.setProperty("privyspark.debug.timestampPattern", value)
        case None => System.clearProperty("privyspark.debug.timestampPattern")
      }
      DriverLogger.resetCache()
    }
  }

  private def withDefaultTimeZone[A](zoneId: String)(block: => A): A = {
    val previous = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone(zoneId))
    try {
      block
    } finally {
      TimeZone.setDefault(previous)
    }
  }

  private def extractTimestamp(logs: String): String = {
    logs.linesIterator.collectFirst {
      case LogLinePattern(timestamp) => timestamp
    }.getOrElse(fail(s"expected one structured driver log line, got: $logs"))
  }

  private def captureStderr[A](block: => A): String = {
    val output = new ByteArrayOutputStream()
    val originalErr = System.err
    val captureErr = new PrintStream(output, true, StandardCharsets.UTF_8.name())
    try {
      System.setErr(captureErr)
      block
    } finally {
      captureErr.flush()
      System.setErr(originalErr)
    }
    output.toString(StandardCharsets.UTF_8.name())
  }
}
