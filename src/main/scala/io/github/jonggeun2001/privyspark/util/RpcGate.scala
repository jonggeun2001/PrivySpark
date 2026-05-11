package io.github.jonggeun2001.privyspark.util

import org.apache.spark.sql.SparkSession

import java.util.concurrent.Semaphore

private[privyspark] final class RpcGate(val permits: Int) {
  require(permits > 0, "RpcGate permits must be > 0")

  private val semaphore = new Semaphore(permits, false)

  def withPermit[A](block: => A): A = {
    semaphore.acquire()
    try {
      block
    } finally {
      semaphore.release()
    }
  }
}

private[privyspark] object RpcGate {
  val DriverRpcConcurrencyConfKey = "spark.privyspark.driverRpcConcurrency"
  val DefaultDriverRpcConcurrency = 48
  val DefaultPreScanRpcConcurrency = 64

  @volatile private var driverGateCache: (Int, Option[RpcGate]) = _
  @volatile private var preScanGateCache: (Int, Option[RpcGate]) = _

  def driverGate(spark: SparkSession): Option[RpcGate] =
    cachedGateForPermits(
      spark.sparkContext.getConf.getInt(DriverRpcConcurrencyConfKey, DefaultDriverRpcConcurrency),
      isPreScan = false
    )

  def preScanGate(spark: SparkSession): Option[RpcGate] =
    cachedGateForPermits(
      spark.sparkContext.getConf.getInt(DriverRpcConcurrencyConfKey, DefaultPreScanRpcConcurrency),
      isPreScan = true
    )

  def fromConfiguredPermits(permits: Int): Option[RpcGate] = {
    if (permits == 0) {
      None
    } else if (permits < 0) {
      throw new IllegalArgumentException(s"$DriverRpcConcurrencyConfKey must be >= 0")
    } else {
      Some(new RpcGate(permits))
    }
  }

  private[privyspark] def cachedGateForPermits(permits: Int, isPreScan: Boolean): Option[RpcGate] = synchronized {
    val current = if (isPreScan) preScanGateCache else driverGateCache
    if (current != null && current._1 == permits) {
      current._2
    } else {
      val resolved = fromConfiguredPermits(permits)
      val updated = permits -> resolved
      if (isPreScan) {
        preScanGateCache = updated
      } else {
        driverGateCache = updated
      }
      resolved
    }
  }
}
