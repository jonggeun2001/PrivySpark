package io.github.jonggeun2001.privyspark.progress

import io.github.jonggeun2001.privyspark.report.JsonCodec.jsonString
import io.github.jonggeun2001.privyspark.util.DriverLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.UUID
import scala.util.control.NonFatal

private[privyspark] object InFlightMarker {
  private val FilenamePrefixMaxLength = 64
  private val FilenameHashLength = 16

  def run[A](
    conf: Configuration,
    inFlightDir: String,
    scope: String,
    identifier: String,
    extra: Map[String, String] = Map.empty,
    preserveOnFailure: Boolean = false
  )(body: => A): A = {
    val markerPath = s"${inFlightDir.stripSuffix("/")}/${markerFileName(identifier)}"
    val markerJson = renderJson(
      runId = resolveRunId(inFlightDir),
      scope = scope,
      identifier = identifier,
      extra = extra,
      threadName = Thread.currentThread().getName,
      startedAtEpochMs = System.currentTimeMillis()
    )

    ProgressIO.writeJsonFile(conf, markerPath, markerJson, overwrite = false)
    var completed = false
    try {
      val result = body
      completed = true
      result
    } finally {
      if (!completed && preserveOnFailure) {
        DriverLogger.warn(
          "in_flight_marker_preserved_after_failure",
          "marker_path" -> markerPath,
          "scope" -> scope,
          "identifier" -> identifier
        )
      } else {
        deleteMarker(conf, markerPath)
      }
    }
  }

  private def sanitize(identifier: String): String =
    Option(identifier).getOrElse("").replaceAll("[^A-Za-z0-9._-]", "_")

  private def markerFileName(identifier: String): String = {
    val safeIdentifier = sanitize(identifier)
    val prefix = if (safeIdentifier.isEmpty) "marker" else safeIdentifier.take(FilenamePrefixMaxLength)
    val hash = sha256Hex(Option(identifier).getOrElse("")).take(FilenameHashLength)
    s"$prefix-$hash-${UUID.randomUUID().toString}.json"
  }

  private def sha256Hex(value: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"${byte & 0xff}%02x")
      .mkString

  private def resolveRunId(inFlightDir: String): String = {
    val inFlightPath = new Path(inFlightDir.stripSuffix("/"))
    Option(inFlightPath.getParent).map(_.getName).getOrElse("")
  }

  private def renderJson(
    runId: String,
    scope: String,
    identifier: String,
    extra: Map[String, String],
    threadName: String,
    startedAtEpochMs: Long
  ): String = {
    val stringFields = Seq(
      "runId" -> runId,
      "scope" -> scope,
      "identifier" -> identifier
    ) ++ extra.toSeq.sortBy(_._1) ++ Seq("threadName" -> threadName)

    val renderedStringFields = stringFields.map {
      case (name, value) => s""""$name":${jsonString(value)}"""
    }
    (renderedStringFields :+ s""""startedAtEpochMs":$startedAtEpochMs""").mkString("{", ",", "}")
  }

  private def deleteMarker(conf: Configuration, markerPath: String): Unit = {
    val path = new Path(markerPath)
    try {
      val deleted = path.getFileSystem(conf).delete(path, false)
      if (!deleted) {
        DriverLogger.warn("in_flight_marker_delete_missing", "marker_path" -> markerPath)
      }
    } catch {
      case NonFatal(e) =>
        DriverLogger.warn(
          "in_flight_marker_delete_failed",
          "marker_path" -> markerPath,
          "error" -> Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        )
    }
  }
}
