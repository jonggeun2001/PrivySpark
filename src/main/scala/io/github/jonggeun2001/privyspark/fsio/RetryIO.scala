package io.github.jonggeun2001.privyspark.fsio

import io.github.jonggeun2001.privyspark.util.DriverLogger
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import java.nio.file.NoSuchFileException
import scala.annotation.tailrec
import scala.util.Random
import scala.util.control.NonFatal

private[privyspark] object RetryIO {
  val RetryRefreshParentConfKey = "spark.privyspark.retry.refreshParent"
  val MaxFileReadAttempts = 3
  val FileReadRetryDelayMillis = 200L
  val RetryJitterRatio = 0.25
  val RetriableFileReadErrorSnippets = Seq(
    "path does not exist",
    "file does not exist",
    "no such file",
    "underlying files have been updated",
    "failed_read_file",
    "encountered error while reading file"
  )

  @tailrec
  private def collectThrowableChain(current: Throwable, acc: Vector[Throwable] = Vector.empty): Vector[Throwable] = {
    if (current == null || acc.contains(current)) {
      acc
    } else {
      collectThrowableChain(current.getCause, acc :+ current)
    }
  }

  def formatThrowableSummary(error: Throwable): String = {
    collectThrowableChain(error)
      .flatMap { cause =>
        Option(cause.getMessage)
          .filter(_.trim.nonEmpty)
          .map(message => s"${cause.getClass.getSimpleName}: ${message.trim}")
      }
      .headOption
      .getOrElse(error.getClass.getSimpleName)
  }

  private def isRetriableFileReadFailure(error: Throwable): Boolean = {
    collectThrowableChain(error).exists {
      case _: java.io.FileNotFoundException => true
      case _: NoSuchFileException => true
      case cause =>
        val normalizedMessage = Option(cause.getMessage).map(_.toLowerCase).getOrElse("")
        RetriableFileReadErrorSnippets.exists(normalizedMessage.contains)
    }
  }

  private[privyspark] def refreshTargetsForRetry(filePaths: Seq[String], refreshParent: Boolean): Seq[String] = {
    filePaths.distinct.flatMap { path =>
      if (refreshParent) {
        Seq(Some(path), Option(new Path(path).getParent).map(_.toString)).flatten
      } else {
        Seq(path)
      }
    }.distinct
  }

  private def refreshReadPaths(spark: SparkSession, filePaths: Seq[String], refreshParent: Boolean): Unit = {
    val refreshTargets = refreshTargetsForRetry(filePaths, refreshParent)

    refreshTargets.foreach { path =>
      try {
        spark.catalog.refreshByPath(path)
      } catch {
        case NonFatal(_) => ()
      }
    }
  }

  private[privyspark] def retryDelayMillis(
    baseDelayMs: Long,
    nextAttempt: Int,
    jitterRatio: Double,
    randomFraction: Double
  ): Long = {
    val retryIndex = math.max(0, nextAttempt - 2)
    val exponentialDelay = baseDelayMs * (1L << math.min(retryIndex, 20))
    val boundedRandomFraction = math.max(0.0, math.min(1.0, randomFraction))
    val jitter = (exponentialDelay * math.max(0.0, jitterRatio) * boundedRandomFraction).toLong
    exponentialDelay + jitter
  }

  private def pauseBeforeRetry(delayMs: Long): Unit = {
    if (delayMs > 0L) {
      try {
        Thread.sleep(delayMs)
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          throw new IllegalStateException("File read retry interrupted")
      }
    }
  }

  def withFileReadRetry[A](
    spark: SparkSession,
    filePaths: Seq[String],
    operationName: String,
    maxAttempts: Int = MaxFileReadAttempts,
    retryDelayMs: Long = FileReadRetryDelayMillis
  )(block: => A): A = {
    require(maxAttempts >= 1, "maxAttempts must be >= 1")
    val refreshParent = spark.sparkContext.getConf.getBoolean(RetryRefreshParentConfKey, false)

    def attempt(attemptNumber: Int): A = {
      try {
        block
      } catch {
        case NonFatal(error) if attemptNumber < maxAttempts && isRetriableFileReadFailure(error) =>
          val nextAttempt = attemptNumber + 1
          val reason = formatThrowableSummary(error)
          DriverLogger.warn(
            "file_read_retry",
            "operation" -> operationName,
            "attempt" -> nextAttempt,
            "max_attempts" -> maxAttempts,
            "files" -> filePaths.size,
            "refresh_parent" -> refreshParent,
            "reason" -> reason
          )
          DriverLogger.debug(
            "file_read_retry",
            "operation" -> operationName,
            "attempt" -> nextAttempt,
            "max_attempts" -> maxAttempts,
            "files" -> filePaths.size,
            "refresh_parent" -> refreshParent,
            "reason" -> reason
          )
          refreshReadPaths(spark, filePaths, refreshParent)
          pauseBeforeRetry(retryDelayMillis(retryDelayMs, nextAttempt, RetryJitterRatio, Random.nextDouble()))
          attempt(nextAttempt)
      }
    }

    attempt(1)
  }
}
