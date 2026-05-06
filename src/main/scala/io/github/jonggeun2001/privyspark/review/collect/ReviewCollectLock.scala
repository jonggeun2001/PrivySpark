package io.github.jonggeun2001.privyspark.review.collect

import io.github.jonggeun2001.privyspark.util.DriverLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileAlreadyExistsException, FileSystem, Path}

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID
import scala.util.control.NonFatal

private[privyspark] object ReviewCollectLock {
  val LockFileName = ".collect.lock"

  def withLock[T](conf: Configuration, reviewStateRoot: String)(body: => T): T = {
    val lock = new Path(lockPath(reviewStateRoot))
    val fs = lock.getFileSystem(conf)
    val parent = lock.getParent
    if (parent != null) {
      fs.mkdirs(parent)
    }

    acquire(fs, lock)
    try {
      body
    } finally {
      release(fs, lock)
    }
  }

  private[privyspark] def lockPath(reviewStateRoot: String): String =
    s"${reviewStateRoot.stripSuffix("/")}/$LockFileName"

  private def acquire(fs: FileSystem, lock: Path): Unit = {
    var created = false
    try {
      val output = fs.create(lock, false)
      created = true
      try {
        output.write(lockMetadata().getBytes(StandardCharsets.UTF_8))
      } finally {
        output.close()
      }
    } catch {
      case e: FileAlreadyExistsException =>
        throw new IllegalStateException(s"Review collect lock already exists: ${lock.toString}", e)
      case NonFatal(e) =>
        if (created) {
          deleteQuietly(fs, lock)
        }
        throw e
    }
  }

  private def release(fs: FileSystem, lock: Path): Unit = {
    try {
      if (!deleteQuietly(fs, lock)) {
        DriverLogger.warn("review_collect_lock_release_failed", "lock" -> lock.toString)
      }
    } catch {
      case NonFatal(e) =>
        DriverLogger.warn(
          "review_collect_lock_release_failed",
          "lock" -> lock.toString,
          "reason" -> Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        )
    }
  }

  private def deleteQuietly(fs: FileSystem, lock: Path): Boolean =
    !fs.exists(lock) || fs.delete(lock, false)

  private def lockMetadata(): String =
    s"run_id=${UUID.randomUUID().toString}\ncreated_at=${Instant.now().toString}\n"
}
