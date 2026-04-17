package io.github.jonggeun2001.privyspark.fsio

import io.github.jonggeun2001.privyspark.util.DriverLogger
import org.apache.hadoop.fs.Path

import scala.util.control.NonFatal

private[privyspark] object ManagedPaths {
  def pathExists(conf: org.apache.hadoop.conf.Configuration, path: String): Boolean = {
    val targetPath = new Path(path)
    targetPath.getFileSystem(conf).exists(targetPath)
  }

  def deleteManagedPath(conf: org.apache.hadoop.conf.Configuration, path: String): Unit = {
    val targetPath = new Path(path)
    val fs = targetPath.getFileSystem(conf)
    if (fs.exists(targetPath) && !fs.delete(targetPath, true)) {
      throw new IllegalStateException(s"Failed to delete report output: $path")
    }
  }

  def renameManagedPath(conf: org.apache.hadoop.conf.Configuration, sourcePath: String, targetPath: String): Unit = {
    val source = new Path(sourcePath)
    val target = new Path(targetPath)
    val fs = source.getFileSystem(conf)
    val targetFs = target.getFileSystem(conf)
    require(fs.getUri == targetFs.getUri, s"Cannot rename across filesystems: $sourcePath -> $targetPath")
    Option(target.getParent).foreach { parent =>
      if (!fs.exists(parent) && !fs.mkdirs(parent)) {
        throw new IllegalStateException(s"Failed to create parent path for report output: ${parent.toString}")
      }
    }
    if (!fs.rename(source, target)) {
      throw new IllegalStateException(s"Failed to move report output: $sourcePath -> $targetPath")
    }
  }

  def cleanupStagingPaths(conf: org.apache.hadoop.conf.Configuration, stagingPaths: Seq[String]): Unit = {
    stagingPaths.foreach(path => deleteStagingPath(conf, path))
  }

  def deleteStagingPath(conf: org.apache.hadoop.conf.Configuration, path: String): Unit = {
    try {
      val stagingPath = new Path(path)
      val fs = stagingPath.getFileSystem(conf)
      if (fs.exists(stagingPath) && !fs.delete(stagingPath, true)) {
        DriverLogger.warn("staging_cleanup_failed", "path" -> path, "reason" -> "delete returned false")
      }
    } catch {
      case NonFatal(e) =>
        DriverLogger.warn(
          "staging_cleanup_failed",
          "path" -> path,
          "reason" -> Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        )
    }
  }
}
