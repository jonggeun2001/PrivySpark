package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.util.{ParallelismConfig, RpcGate}

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.util.concurrent.atomic.AtomicInteger

@RunWith(classOf[JUnitRunner])
class RpcGateSpec extends AnyFunSuite {
  test("executeInParallel caps concurrent tasks with an RpcGate") {
    val gate = new RpcGate(permits = 2)
    val activeTasks = new AtomicInteger(0)
    val peakActiveTasks = new AtomicInteger(0)

    val results = ParallelismConfig.executeInParallel(
      parallelism = 8,
      tasks = (1 to 8).map { value =>
        () => {
          val active = activeTasks.incrementAndGet()
          updatePeak(peakActiveTasks, active)
          try {
            Thread.sleep(20L)
            value
          } finally {
            activeTasks.decrementAndGet()
          }
        }
      },
      gate = Some(gate)
    )

    assert(results.sorted == (1 to 8))
    assert(peakActiveTasks.get() <= 2)
  }

  test("zero configured permits disables the RpcGate") {
    assert(RpcGate.fromConfiguredPermits(0).isEmpty)
  }

  test("negative configured permits are rejected") {
    intercept[IllegalArgumentException] {
      RpcGate.fromConfiguredPermits(-1)
    }
  }

  test("driver gates are shared for the same configured permits") {
    val first = RpcGate.cachedGateForPermits(3, isPreScan = false)
    val second = RpcGate.cachedGateForPermits(3, isPreScan = false)

    assert(first.nonEmpty)
    assert(first.get eq second.get)
  }

  private def updatePeak(peak: AtomicInteger, candidate: Int): Unit = {
    var updated = false
    while (!updated) {
      val current = peak.get()
      updated = candidate <= current || peak.compareAndSet(current, candidate)
    }
  }
}
