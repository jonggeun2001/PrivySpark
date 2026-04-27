package io.github.jonggeun2001.privyspark.hive

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.sql.{Connection, DriverManager}

@RunWith(classOf[JUnitRunner])
class HiveTableLookupSpec extends AnyFunSuite {
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
