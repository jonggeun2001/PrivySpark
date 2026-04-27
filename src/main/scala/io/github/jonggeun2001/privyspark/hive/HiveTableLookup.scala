package io.github.jonggeun2001.privyspark.hive

import io.github.jonggeun2001.privyspark.util.DriverLogger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier

import java.net.{URI, URLDecoder}
import java.nio.charset.StandardCharsets
import scala.util.Try
import scala.util.control.NonFatal

final case class HiveTableLookupIndex(entries: Vector[(String, String)]) extends Serializable {
  @transient private lazy val normalizedEntries: Vector[(String, String)] =
    HiveTableLookupIndex.normalizeEntries(entries)

  def size: Int = normalizedEntries.size

  def lookup(rawPath: String): String = {
    HiveTableLookup.normalizePathForLookup(rawPath).flatMap { normalizedPath =>
      normalizedEntries.collectFirst {
        case (prefix, tableFqn) if HiveTableLookupIndex.matchesPrefix(normalizedPath, prefix) => tableFqn
      }
    }.getOrElse("")
  }
}

object HiveTableLookupIndex {
  val Empty: HiveTableLookupIndex = HiveTableLookupIndex(Vector.empty)

  private[hive] def normalizeEntries(entries: Vector[(String, String)]): Vector[(String, String)] =
    entries.flatMap {
      case (location, tableFqn) =>
        val normalizedTableFqn = Option(tableFqn).map(_.trim).getOrElse("")
        HiveTableLookup.normalizeLocation(location)
          .filter(_ => normalizedTableFqn.nonEmpty)
          .toVector
          .flatMap(locationVariants)
          .map(_ -> normalizedTableFqn)
    }.distinct.sortBy {
      case (prefix, tableFqn) => (-prefix.length, prefix, tableFqn)
    }

  private def locationVariants(normalizedLocation: String): Vector[String] = {
    val filePathVariant =
      if (normalizedLocation.startsWith("file:/")) Some(normalizedLocation.stripPrefix("file:")) else None
    (Vector(normalizedLocation) ++ filePathVariant).distinct
  }

  private[hive] def matchesPrefix(path: String, prefix: String): Boolean = {
    path == prefix ||
      (prefix == "/" && path.startsWith("/")) ||
      (prefix.endsWith("/") && path.startsWith(prefix)) ||
      path.startsWith(prefix + "/")
  }
}

object HiveTableLookup {
  def buildAndBroadcast(spark: SparkSession): Broadcast[HiveTableLookupIndex] =
    spark.sparkContext.broadcast(buildIndex(spark))

  private[hive] def buildIndex(spark: SparkSession): HiveTableLookupIndex = {
    try {
      val entries = spark.catalog.listDatabases().collect().toVector.flatMap { database =>
        val databaseName = database.name
        try {
          spark.catalog.listTables(databaseName).collect().toVector.flatMap { table =>
            if (Option(table.tableType).exists(_.equalsIgnoreCase("VIEW"))) {
              None
            } else {
              tableLocation(spark, databaseName, table.name).flatMap { location =>
                normalizeLocation(location).map(_ -> s"$databaseName.${table.name}")
              }
            }
          }
        } catch {
          case NonFatal(e) =>
            DriverLogger.warn(
              "hive_database_lookup_failed",
              "database" -> databaseName,
              "exception" -> e.getClass.getSimpleName,
              "reason" -> Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
            )
            Vector.empty
        }
      }
      val index = HiveTableLookupIndex(entries)
      DriverLogger.info("hive_lookup_ready", "size" -> index.size)
      index
    } catch {
      case NonFatal(e) =>
        DriverLogger.warn(
          "hive_disabled_metastore_init_failed",
          "exception" -> e.getClass.getSimpleName,
          "reason" -> Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        )
        HiveTableLookupIndex.Empty
    }
  }

  def stripCompositeIdentifier(path: String): String = {
    val rawPath = Option(path).getOrElse("")
    val archiveSeparatorIndex = rawPath.indexOf('!')
    if (archiveSeparatorIndex > 0) {
      rawPath.substring(0, archiveSeparatorIndex)
    } else {
      val workbookSeparatorIndex = rawPath.lastIndexOf('#')
      if (workbookSeparatorIndex > 0) rawPath.substring(0, workbookSeparatorIndex) else rawPath
    }
  }

  private[hive] def normalizeLocation(rawLocation: String): Option[String] =
    normalizeUriString(rawLocation)

  private[hive] def normalizePathForLookup(rawPath: String): Option[String] =
    normalizeUriString(stripCompositeIdentifier(rawPath))

  private def tableLocation(spark: SparkSession, databaseName: String, tableName: String): Option[String] = {
    try {
      spark.sessionState.catalog
        .getTableMetadata(TableIdentifier(tableName, Some(databaseName)))
        .storage
        .locationUri
        .map(_.toString)
    } catch {
      case NonFatal(e) =>
        DriverLogger.warn(
          "hive_table_lookup_failed",
          "table" -> s"$databaseName.$tableName",
          "exception" -> e.getClass.getSimpleName,
          "reason" -> Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        )
        None
    }
  }

  private def normalizeUriString(rawValue: String): Option[String] = {
    val raw = Option(rawValue).map(_.trim).getOrElse("")
    if (raw.isEmpty) {
      None
    } else {
      parseUri(raw).flatMap(renderNormalizedUri)
    }
  }

  private def parseUri(raw: String): Option[URI] =
    Try(new URI(raw)).orElse(Try(new URI(raw.replace(" ", "%20")))).toOption.map(_.normalize())

  private def renderNormalizedUri(uri: URI): Option[String] = {
    val rawPath = Option(uri.getRawPath).filter(_.nonEmpty).getOrElse(uri.getPath)
    val decodedPath = Option(rawPath)
      .map(path => URLDecoder.decode(path, StandardCharsets.UTF_8.name()))
      .map(removeTrailingSlash)
      .filter(_.nonEmpty)
      .getOrElse("/")
    val scheme = Option(uri.getScheme).map(_.toLowerCase)
    val authority = normalizedAuthority(uri)

    scheme match {
      case Some(value) if authority.nonEmpty =>
        Some(s"$value://${authority.get}$decodedPath")
      case Some(value) =>
        Some(s"$value:$decodedPath")
      case None =>
        Some(decodedPath)
    }
  }

  private def normalizedAuthority(uri: URI): Option[String] = {
    val host = Option(uri.getHost).map(_.toLowerCase)
    host.orElse(Option(uri.getAuthority).map(_.toLowerCase)).map { authority =>
      val port = uri.getPort
      if (host.nonEmpty && port >= 0 && !authority.endsWith(s":$port")) {
        s"$authority:$port"
      } else {
        authority
      }
    }
  }

  private def removeTrailingSlash(path: String): String = {
    val normalized = Option(path).getOrElse("").replace('\\', '/')
    if (normalized == "/") normalized else normalized.replaceAll("/+$", "")
  }
}
