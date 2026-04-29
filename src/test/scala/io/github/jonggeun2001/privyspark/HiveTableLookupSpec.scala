package io.github.jonggeun2001.privyspark.hive

import org.junit.runner.RunWith
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.sql.{Connection, DriverManager}
import java.util.Properties

@RunWith(classOf[JUnitRunner])
class HiveTableLookupSpec extends AnyFunSuite {
  test("resolveDriverClass defaults to MariaDB when unset") {
    val conf = new SparkConf(false)

    val driverClass = HiveMetastoreJdbcConfig.resolveDriverClass(conf, None)

    assert(driverClass == HiveMetastoreJdbcConfig.DefaultDriverClass)
  }

  test("resolveDriverClass prefers Spark conf when CLI value is unset") {
    val conf = new SparkConf(false)
      .set(HiveMetastoreJdbcConfig.DriverClassConfKey, "com.mysql.cj.jdbc.Driver")

    val driverClass = HiveMetastoreJdbcConfig.resolveDriverClass(conf, None)

    assert(driverClass == "com.mysql.cj.jdbc.Driver")
  }

  test("resolveDriverClass prefers CLI value over Spark conf") {
    val conf = new SparkConf(false)
      .set(HiveMetastoreJdbcConfig.DriverClassConfKey, "com.mysql.cj.jdbc.Driver")

    val driverClass = HiveMetastoreJdbcConfig.resolveDriverClass(conf, Some("org.h2.Driver"))

    assert(driverClass == "org.h2.Driver")
  }

  test("resolveDriverClass rejects blank Spark conf values") {
    val conf = new SparkConf(false)
      .set(HiveMetastoreJdbcConfig.DriverClassConfKey, "   ")

    val error = intercept[IllegalArgumentException] {
      HiveMetastoreJdbcConfig.resolveDriverClass(conf, None)
    }

    assert(error.getMessage.contains(HiveMetastoreJdbcConfig.DriverClassConfKey))
    assert(error.getMessage.contains("must not be blank"))
  }

  test("connectionProperties applies default timeouts only to MariaDB or MySQL compatible drivers") {
    val mariaDbProps = HiveTableLookup.connectionProperties(
      HiveMetastoreJdbcConfig("jdbc:mariadb://hms-db.internal:3306/metastore", "metastore_ro", "/pw"),
      "secret"
    )
    val mysqlProps = HiveTableLookup.connectionProperties(
      HiveMetastoreJdbcConfig("jdbc:mysql://hms-db.internal:3306/metastore", "metastore_ro", "/pw", "com.mysql.cj.jdbc.Driver"),
      "secret"
    )
    val postgresProps = HiveTableLookup.connectionProperties(
      HiveMetastoreJdbcConfig("jdbc:postgresql://hms-db.internal:5432/metastore", "metastore_ro", "/pw", "org.postgresql.Driver"),
      "secret"
    )

    assert(mariaDbProps.getProperty("connectTimeout") == "5000")
    assert(mariaDbProps.getProperty("socketTimeout") == "30000")
    assert(mysqlProps.getProperty("connectTimeout") == "5000")
    assert(mysqlProps.getProperty("socketTimeout") == "30000")
    assert(postgresProps.getProperty("connectTimeout") == null)
    assert(postgresProps.getProperty("socketTimeout") == null)
  }

  test("connectionProperties keeps URL timeout values authoritative") {
    val props = HiveTableLookup.connectionProperties(
      HiveMetastoreJdbcConfig(
        "jdbc:mariadb://hms-db.internal:3306/metastore?connectTimeout=111&socketTimeout=222",
        "metastore_ro",
        "/pw"
      ),
      "secret"
    )

    assert(props.getProperty("connectTimeout") == null)
    assert(props.getProperty("socketTimeout") == null)
  }

  test("empty index returns blank fqn without throwing") {
    assert(HiveTableLookupIndex.Empty.lookup("/warehouse/sales/orders.parquet") == "")
    assert(HiveTableLookupIndex.Empty.lookup("") == "")
    assert(HiveTableLookupIndex.Empty.lookup(null) == "")
    assert(HiveTableLookupIndex(Vector("/warehouse" -> "mart.table")).lookup("://not a valid uri") == "")
  }

  test("lookup uses longest prefix and keeps directory boundaries") {
    val index = HiveTableLookupIndex(
      Vector(
        "/warehouse/sale" -> "mart.sale",
        "/warehouse/sales" -> "mart.sales",
        "/warehouse/sales/orders" -> "mart.orders"
      )
    )

    assert(index.lookup("/warehouse/sales/orders/part-00000.parquet") == "mart.orders")
    assert(index.lookup("/warehouse/sales/customers/part-00000.parquet") == "mart.sales")
    assert(index.lookup("/warehouse/sale/part-00000.parquet") == "mart.sale")
    assert(index.lookup("/warehouse/salesforce/part-00000.parquet") == "")
  }

  test("lookup normalizes uri case, trailing slash, and percent encoded paths") {
    val index = HiveTableLookupIndex(
      Vector("hdfs://namenode.example.com/warehouse/sales data" -> "mart.sales_data")
    )

    assert(index.lookup("HDFS://NameNode.Example.Com/warehouse/sales%20data/part-00000.parquet") == "mart.sales_data")
  }

  test("stripCompositeIdentifier keeps archive or workbook host path") {
    assert(
      HiveTableLookup.stripCompositeIdentifier("hdfs://nn/data/archive.zip!nested/customers.csv") ==
        "hdfs://nn/data/archive.zip"
    )
    assert(
      HiveTableLookup.stripCompositeIdentifier("hdfs://nn/data/workbook.xlsx#Contacts") ==
        "hdfs://nn/data/workbook.xlsx"
    )
    assert(HiveTableLookup.stripCompositeIdentifier("hdfs://nn/data/plain.csv") == "hdfs://nn/data/plain.csv")
  }

  test("stripCompositeIdentifier uses the earliest composite separator") {
    assert(
      HiveTableLookup.stripCompositeIdentifier("hdfs://nn/data/workbook.xlsx#Sheet!cell-note") ==
        "hdfs://nn/data/workbook.xlsx"
    )
  }

  test("duplicate prefixes choose a deterministic table fqn") {
    val index = HiveTableLookupIndex(
      Vector(
        "/warehouse/sales" -> "z_db.sales",
        "/warehouse/sales" -> "a_db.sales"
      )
    )

    assert(index.lookup("/warehouse/sales/part-00000.parquet") == "a_db.sales")
  }

  test("buildIndex converts metastore rows into lookup entries") {
    val index = HiveTableLookup.buildIndex(
      Vector(
        ("finance", "cards", "hdfs://NameNode/warehouse/finance.db/cards/"),
        ("finance", "cards_pii", "hdfs://namenode/warehouse/finance.db/cards/pii")
      )
    )

    assert(index.lookup("hdfs://namenode/warehouse/finance.db/cards/pii/part-00000.parquet") == "finance.cards_pii")
    assert(index.lookup("hdfs://namenode/warehouse/finance.db/cards/year=2024/part-00000.parquet") == "finance.cards")
  }

  test("queryLocations reads managed and external table locations from metastore schema") {
    Class.forName("org.h2.Driver")
    val connection = DriverManager.getConnection(s"jdbc:h2:mem:privyspark_hive_lookup_${System.nanoTime()}")
    try {
      createMetastoreSubset(connection)
      execute(connection, "INSERT INTO DBS (DB_ID, NAME) VALUES (1, 'finance'), (2, 'default')")
      execute(connection, "INSERT INTO SDS (SD_ID, LOCATION) VALUES (10, 'hdfs://nn/warehouse/finance.db/cards'), (11, 'hdfs://nn/warehouse/default.db/audit'), (12, ''), (13, 'hdfs://nn/view')")
      execute(connection, "INSERT INTO TBLS (TBL_ID, DB_ID, SD_ID, TBL_NAME, TBL_TYPE) VALUES (100, 1, 10, 'cards', 'EXTERNAL_TABLE'), (101, 2, 11, 'audit', 'MANAGED_TABLE'), (102, 1, 12, 'empty_loc', 'EXTERNAL_TABLE'), (103, 1, 13, 'view_like', 'VIRTUAL_VIEW')")

      val rows = HiveTableLookup.queryLocations(connection)

      assert(rows.toSet == Set(
        ("finance", "cards", "hdfs://nn/warehouse/finance.db/cards"),
        ("default", "audit", "hdfs://nn/warehouse/default.db/audit")
      ))
    } finally {
      connection.close()
    }
  }

  test("buildLookupIndex loads the configured JDBC driver class") {
    Class.forName("org.h2.Driver")
    val spark = SparkSession.builder()
      .appName("HiveTableLookupSpec")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.allowMultipleContexts", "true")
      .getOrCreate()
    val passwordPath = Files.createTempFile("privyspark-hive-password-", ".txt")
    Files.write(passwordPath, "secret\n".getBytes(StandardCharsets.UTF_8))
    val jdbcUrl = s"jdbc:h2:mem:privyspark_hive_lookup_configured_driver_${System.nanoTime()};DB_CLOSE_DELAY=-1"
    val properties = new Properties()
    properties.setProperty("user", "sa")
    properties.setProperty("password", "secret")
    val connection = DriverManager.getConnection(jdbcUrl, properties)
    try {
      createMetastoreSubset(connection)
      execute(connection, "INSERT INTO DBS (DB_ID, NAME) VALUES (1, 'finance')")
      execute(connection, "INSERT INTO SDS (SD_ID, LOCATION) VALUES (10, 'hdfs://nn/warehouse/finance.db/cards')")
      execute(connection, "INSERT INTO TBLS (TBL_ID, DB_ID, SD_ID, TBL_NAME, TBL_TYPE) VALUES (100, 1, 10, 'cards', 'EXTERNAL_TABLE')")

      val index = HiveTableLookup.buildLookupIndex(
        spark,
        Some(HiveMetastoreJdbcConfig(jdbcUrl, "sa", passwordPath.toString, "org.h2.Driver"))
      )

      assert(index.lookup("hdfs://nn/warehouse/finance.db/cards/part-00000.parquet") == "finance.cards")
    } finally {
      connection.close()
      spark.stop()
      Files.deleteIfExists(passwordPath)
    }
  }

  private def createMetastoreSubset(connection: Connection): Unit = {
    execute(connection, "CREATE TABLE DBS (DB_ID BIGINT PRIMARY KEY, NAME VARCHAR(255) NOT NULL)")
    execute(connection, "CREATE TABLE SDS (SD_ID BIGINT PRIMARY KEY, LOCATION VARCHAR(1024))")
    execute(connection, "CREATE TABLE TBLS (TBL_ID BIGINT PRIMARY KEY, DB_ID BIGINT NOT NULL, SD_ID BIGINT NOT NULL, TBL_NAME VARCHAR(255) NOT NULL, TBL_TYPE VARCHAR(255) NOT NULL)")
  }

  private def execute(connection: Connection, sql: String): Unit = {
    val statement = connection.createStatement()
    try {
      statement.execute(sql)
    } finally {
      statement.close()
    }
  }
}
