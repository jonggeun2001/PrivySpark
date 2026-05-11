package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.model.{ScanGroup, ScanReadOptions}
import io.github.jonggeun2001.privyspark.scan.GroupScanRoute.{BatchScan, FileScan, SampledExact}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GroupScanRouterSpec extends AnyFunSuite {
  test("routeOf sends sampled batch-capable non-json groups to batch scan") {
    val group = csvGroup(filePaths = Seq("/data/a.csv", "/data/b.csv"), schemaSampled = true)

    assert(GroupScanRouter.routeOf(group) == BatchScan)
  }

  test("routeOf sends sampled multi-file json groups to exact split") {
    val group = csvGroup(filePaths = Seq("/data/a.json", "/data/b.json"), format = "json", schemaSampled = true)

    assert(GroupScanRouter.routeOf(group) == SampledExact)
  }

  test("routeOf sends directory identifier groups to file scan") {
    val group = csvGroup(filePaths = Seq("/data/a.csv", "/data/b.csv"), useDirectoryIdentifier = true)

    assert(GroupScanRouter.routeOf(group) == FileScan)
  }

  test("routeOf sends unsupported batch formats to file scan") {
    val group = csvGroup(filePaths = Seq("/data/users.xlsx"), format = "xlsx")

    assert(GroupScanRouter.routeOf(group) == FileScan)
  }

  test("routeOf sends sampled unsupported batch formats to file scan") {
    val group = csvGroup(filePaths = Seq("/data/a.xlsx", "/data/b.xlsx"), format = "xlsx", schemaSampled = true)

    assert(GroupScanRouter.routeOf(group) == FileScan)
  }

  test("routeOf sends groups with per-file read options to file scan") {
    val group = csvGroup(
      filePaths = Seq("/data/a.csv"),
      readOptionsByKey = Map("/data/a.csv" -> ScanReadOptions(textEncoding = Some("EUC-KR")))
    )

    assert(GroupScanRouter.routeOf(group) == FileScan)
  }

  test("routeOf sends exact batch-capable groups to batch scan") {
    val group = csvGroup(filePaths = Seq("/data/a.csv", "/data/b.csv"))

    assert(GroupScanRouter.routeOf(group) == BatchScan)
  }

  private def csvGroup(
    filePaths: Seq[String],
    format: String = "csv",
    schemaSampled: Boolean = false,
    useDirectoryIdentifier: Boolean = false,
    readOptionsByKey: Map[String, ScanReadOptions] = Map.empty
  ): ScanGroup =
    ScanGroup(
      directoryPath = "/data",
      format = format,
      schemaSignature = "name|email",
      filePaths = filePaths,
      schemaSampled = schemaSampled,
      useDirectoryIdentifier = useDirectoryIdentifier,
      readOptionsByKey = readOptionsByKey
    )
}
