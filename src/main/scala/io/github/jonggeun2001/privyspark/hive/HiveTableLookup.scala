package io.github.jonggeun2001.privyspark.hive

import io.github.jonggeun2001.privyspark.util.DriverLogger
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.{URI, URLDecoder}
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager}
import java.util.Properties
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.NonFatal

final case class HiveMetastoreJdbcConfig(
  jdbcUrl: String,
  user: String,
  passwordFile: String
)

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
  val Empty: HiveTableLookupIndex = HiveTableLookupIndex.Empty

  private val MariaDbDriverClass = "org.mariadb.jdbc.Driver"
  private val MaxPasswordFileBytes = 1024L * 1024L
  private val TableLocationSql =
    """SELECT D.NAME, T.TBL_NAME, S.LOCATION
      |FROM TBLS T
      |JOIN DBS D ON T.DB_ID = D.DB_ID
      |JOIN SDS S ON T.SD_ID = S.SD_ID
      |WHERE T.TBL_TYPE IN ('MANAGED_TABLE', 'EXTERNAL_TABLE')
      |  AND S.LOCATION IS NOT NULL
      |  AND S.LOCATION <> ''
      |""".stripMargin

  def buildAndBroadcast(
    spark: SparkSession,
    config: Option[HiveMetastoreJdbcConfig]
  ): Broadcast[HiveTableLookupIndex] =
    spark.sparkContext.broadcast(buildLookupIndex(spark, config))

  private[hive] def buildLookupIndex(
    spark: SparkSession,
    config: Option[HiveMetastoreJdbcConfig]
  ): HiveTableLookupIndex = {
    config match {
      case None =>
        DriverLogger.info("hive_lookup_inactive")
        HiveTableLookupIndex.Empty
      case Some(jdbcConfig) =>
        try {
          val password = readPasswordFile(spark, jdbcConfig.passwordFile)
          Class.forName(MariaDbDriverClass)
          val connection = DriverManager.getConnection(jdbcConfig.jdbcUrl, connectionProperties(jdbcConfig, password))
          try {
            val index = buildIndex(queryLocations(connection))
            DriverLogger.info("hive_lookup_ready", "size" -> index.size)
            index
          } finally {
            connection.close()
          }
        } catch {
          case NonFatal(e) =>
            DriverLogger.warn(
              "hive_lookup_disabled",
              "exception" -> e.getClass.getSimpleName,
              "reason" -> Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
            )
            HiveTableLookupIndex.Empty
        }
    }
  }

  def readPasswordFile(spark: SparkSession, path: String): String = {
    val hadoopPath = new Path(path)
    val fs = FileSystem.get(new URI(path), spark.sparkContext.hadoopConfiguration)
    val status = fs.getFileStatus(hadoopPath)
    if (!status.isFile) {
      throw new IllegalArgumentException(s"Hive metastore password path is not a file: $path")
    }
    if (status.getLen > MaxPasswordFileBytes) {
      throw new IllegalArgumentException(s"Hive metastore password file is larger than $MaxPasswordFileBytes bytes: $path")
    }

    val input = fs.open(hadoopPath)
    try {
      val reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))
      val password = Option(reader.readLine()).map(_.trim).getOrElse("")
      if (password.isEmpty) {
        throw new IllegalArgumentException(s"Hive metastore password file is empty: $path")
      }
      password
    } finally {
      input.close()
    }
  }

  private[hive] def queryLocations(conn: Connection): Vector[(String, String, String)] = {
    val rows = ArrayBuffer.empty[(String, String, String)]
    val statement = conn.createStatement()
    try {
      val resultSet = statement.executeQuery(TableLocationSql)
      try {
        while (resultSet.next()) {
          rows += ((
            resultSet.getString(1),
            resultSet.getString(2),
            resultSet.getString(3)
          ))
        }
      } finally {
        resultSet.close()
      }
    } finally {
      statement.close()
    }
    rows.toVector
  }

  private[hive] def buildIndex(rows: Vector[(String, String, String)]): HiveTableLookupIndex = {
    HiveTableLookupIndex(rows.flatMap {
      case (dbName, tableName, location) =>
        val db = Option(dbName).map(_.trim).getOrElse("")
        val table = Option(tableName).map(_.trim).getOrElse("")
        if (db.nonEmpty && table.nonEmpty) Some(location -> s"$db.$table") else None
    })
  }

  def stripCompositeIdentifier(path: String): String = {
    val rawPath = Option(path).getOrElse("")
    val separatorIndexes = Seq(rawPath.indexOf('!'), rawPath.indexOf('#')).filter(_ > 0)
    if (separatorIndexes.isEmpty) rawPath else rawPath.substring(0, separatorIndexes.min)
  }

  private[hive] def normalizeLocation(rawLocation: String): Option[String] =
    normalizeUriString(rawLocation)

  private[hive] def normalizePathForLookup(rawPath: String): Option[String] =
    normalizeUriString(stripCompositeIdentifier(rawPath))

  private def connectionProperties(config: HiveMetastoreJdbcConfig, password: String): Properties = {
    val props = new Properties()
    props.setProperty("user", config.user)
    props.setProperty("password", password)
    if (!urlHasProperty(config.jdbcUrl, "connectTimeout")) {
      props.setProperty("connectTimeout", "5000")
    }
    if (!urlHasProperty(config.jdbcUrl, "socketTimeout")) {
      props.setProperty("socketTimeout", "30000")
    }
    props
  }

  private def urlHasProperty(jdbcUrl: String, propertyName: String): Boolean = {
    val lowerUrl = Option(jdbcUrl).getOrElse("").toLowerCase
    val lowerName = propertyName.toLowerCase
    lowerUrl.contains(s"?$lowerName=") || lowerUrl.contains(s"&$lowerName=")
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
      case None if rawValueLooksLikeMalformedUri(uri) =>
        None
      case None =>
        Some(decodedPath)
    }
  }

  private def rawValueLooksLikeMalformedUri(uri: URI): Boolean = {
    val raw = Option(uri.toString).getOrElse("")
    raw.startsWith("://")
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
