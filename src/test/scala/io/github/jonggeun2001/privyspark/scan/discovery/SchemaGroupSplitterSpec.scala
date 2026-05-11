package io.github.jonggeun2001.privyspark.scan.discovery

import io.github.jonggeun2001.privyspark.model.ScanGroup
import io.github.jonggeun2001.privyspark.util.RpcGate

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.util.concurrent.atomic.AtomicInteger

@RunWith(classOf[JUnitRunner])
class SchemaGroupSplitterSpec extends AnyFunSuite {
  test("schema split tasks are capped by the pre-scan RpcGate") {
    val groups = (1 to 8).map { index =>
      ScanGroup(
        directoryPath = s"/input/d$index",
        format = "csv",
        schemaSignature = "",
        filePaths = Seq(s"/input/d$index/file.csv")
      )
    }
    val activeTasks = new AtomicInteger(0)
    val peakActiveTasks = new AtomicInteger(0)

    val result = SchemaGroupSplitter.executeSchemaSplitTasks(
      parallelism = 8,
      groups = groups,
      rpcGate = Some(new RpcGate(2))
    ) { group =>
      val active = activeTasks.incrementAndGet()
      updatePeak(peakActiveTasks, active)
      try {
        Thread.sleep(20L)
        group.directoryPath
      } finally {
        activeTasks.decrementAndGet()
      }
    }

    assert(result.sorted == groups.map(_.directoryPath).sorted)
    assert(peakActiveTasks.get() <= 2)
  }

  test("per-file schema split tasks are capped by the pre-scan RpcGate") {
    val sourceKeys = (1 to 8).map(index => s"/input/d/file-$index.csv")
    val activeTasks = new AtomicInteger(0)
    val peakActiveTasks = new AtomicInteger(0)

    val result = SchemaGroupSplitter.executeFileSchemaTasks(
      parallelism = 8,
      sourceKeys = sourceKeys,
      rpcGate = Some(new RpcGate(2))
    ) { sourceKey =>
      val active = activeTasks.incrementAndGet()
      updatePeak(peakActiveTasks, active)
      try {
        Thread.sleep(20L)
        sourceKey
      } finally {
        activeTasks.decrementAndGet()
      }
    }

    assert(result.sorted == sourceKeys.sorted)
    assert(peakActiveTasks.get() <= 2)
  }

  private def updatePeak(peak: AtomicInteger, candidate: Int): Unit = {
    var updated = false
    while (!updated) {
      val current = peak.get()
      updated = candidate <= current || peak.compareAndSet(current, candidate)
    }
  }
}
