package io.github.jonggeun2001.privyspark.hive

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

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

  test("duplicate prefixes choose a deterministic table fqn") {
    val index = HiveTableLookupIndex(
      Vector(
        "/warehouse/sales" -> "z_db.sales",
        "/warehouse/sales" -> "a_db.sales"
      )
    )

    assert(index.lookup("/warehouse/sales/part-00000.parquet") == "a_db.sales")
  }
}
