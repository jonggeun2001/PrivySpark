package io.github.jonggeun2001.privyspark.util

import org.apache.spark.sql.SparkSession

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import java.util.concurrent.Executors

private[privyspark] object ParallelismConfig {
  val PreScanParallelismConfKey = "spark.privyspark.preScanParallelism"
  val DefaultPreScanParallelism = 32
  // Allow higher-than-core I/O fan-out without letting a single scan create an unbounded number of driver threads.
  val MaxSafePreScanParallelism = 64
  val GroupParallelismConfKey = "spark.privyspark.groupParallelism"
  val DefaultGroupParallelism = 16
  val FileParallelismConfKey = "spark.privyspark.fileParallelism"
  val DefaultFileParallelism = 8

  def resolveParallelism(itemCount: Int, configured: Int): Int = {
    if (itemCount <= 1) 1 else math.max(1, math.min(itemCount, configured))
  }

  def defaultPreScanParallelism: Int = {
    DefaultPreScanParallelism
  }

  def defaultGroupParallelism: Int = {
    DefaultGroupParallelism
  }

  def defaultFileParallelism: Int = {
    DefaultFileParallelism
  }

  def maxSafePreScanParallelism: Int = {
    MaxSafePreScanParallelism
  }

  def resolveConfiguredPreScanParallelism(fileCount: Int, configured: Int, source: String): Int = {
    resolveParallelism(fileCount, resolvePreScanPoolSize(configured, source))
  }

  private def resolvePreScanPoolSize(configured: Int, source: String): Int = {
    if (configured <= 0) {
      throw new IllegalArgumentException(s"$source must be > 0")
    }

    math.min(configured, maxSafePreScanParallelism)
  }

  def resolvePreScanParallelism(spark: SparkSession, fileCount: Int): Int = {
    spark.sparkContext.getConf.getOption(PreScanParallelismConfKey) match {
      case Some(_) =>
        resolveConfiguredPreScanParallelism(
          fileCount,
          spark.sparkContext.getConf.getInt(PreScanParallelismConfKey, defaultPreScanParallelism),
          PreScanParallelismConfKey
        )
      case None =>
        resolveParallelism(fileCount, defaultPreScanParallelism)
    }
  }

  def resolveDiscoveryParallelism(spark: SparkSession, configured: Int): Int = {
    if (configured > 0) {
      resolvePreScanPoolSize(configured, "--pre-scan-parallelism")
    } else {
      spark.sparkContext.getConf.getOption(PreScanParallelismConfKey) match {
        case Some(_) =>
          resolvePreScanPoolSize(
            spark.sparkContext.getConf.getInt(PreScanParallelismConfKey, defaultPreScanParallelism),
            PreScanParallelismConfKey
          )
        case None =>
          math.min(defaultPreScanParallelism, maxSafePreScanParallelism)
      }
    }
  }

  def resolveGroupParallelism(spark: SparkSession, groupCount: Int): Int = {
    resolveParallelism(groupCount, spark.sparkContext.getConf.getInt(GroupParallelismConfKey, DefaultGroupParallelism))
  }

  def resolveFileParallelism(spark: SparkSession, fileCount: Int): Int = {
    resolveParallelism(fileCount, spark.sparkContext.getConf.getInt(FileParallelismConfKey, DefaultFileParallelism))
  }

  def resolveCliParallelism(
    preScan: Option[Int],
    group: Option[Int],
    file: Option[Int]
  ): (Int, Int, Int) = {
    (
      preScan.getOrElse(-1),
      group.getOrElse(-1),
      file.getOrElse(-1)
    )
  }

  def renderConfiguredParallelism(configured: Option[Int]): String = {
    configured.map(_.toString).getOrElse("spark_conf_or_default")
  }

  def executeInParallel[A](parallelism: Int, tasks: Seq[() => A]): Seq[A] = {
    if (tasks.isEmpty) {
      Seq.empty
    } else if (parallelism <= 1 || tasks.size <= 1) {
      tasks.map(task => task())
    } else {
      val workerCount = math.max(1, math.min(parallelism, tasks.size))
      val pool = Executors.newFixedThreadPool(workerCount)
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
      try {
        Await.result(Future.sequence(tasks.map(task => Future(task()))), Duration.Inf)
      } finally {
        pool.shutdown()
      }
    }
  }
}
