package io.github.jonggeun2001.privyspark.scan.discovery

import io.github.jonggeun2001.privyspark.config.IgnoreMatcher
import io.github.jonggeun2001.privyspark.util.RpcGate
import io.github.jonggeun2001.privyspark.util.ParallelismConfig.executeInParallel
import org.apache.hadoop.fs.{FileStatus, Path}

import scala.collection.mutable.ArrayBuffer

private[privyspark] object DirectoryDiscovery {
  private val PreScanProgressLogInterval = 10000

  def resolvePreScanProgressInterval(fileCount: Int): Int = {
    if (fileCount <= 0) 1 else math.min(fileCount, PreScanProgressLogInterval)
  }

  def discover(
    fs: org.apache.hadoop.fs.FileSystem,
    rootPath: Path,
    inputPath: String,
    ignoreMatcher: IgnoreMatcher,
    parallelism: Int,
    rpcGate: Option[RpcGate] = None,
    listStatusOverride: Option[Path => Array[FileStatus]] = None
  ): (Seq[DiscoveredFile], Seq[(String, String)]) = {
    val discoveredFiles = ArrayBuffer.empty[DiscoveredFile]
    val ignoredPaths = ArrayBuffer.empty[(String, String)]
    val listStatus = listStatusOverride.getOrElse { directory: Path =>
      Option(fs.listStatus(directory)).getOrElse(Array.empty)
    }

    var currentLevelDirectories = Seq(rootPath)
    while (currentLevelDirectories.nonEmpty) {
      val nextLevelDirectories = ArrayBuffer.empty[Path]

      currentLevelDirectories
        .sortBy(_.toString)
        .grouped(math.max(1, parallelism))
        .foreach { directoryBatch =>
          val listedDirectories = executeInParallel(
            parallelism,
            directoryBatch.map { directory =>
              () => listStatus(directory).sortBy(_.getPath.toString)
            },
            gate = rpcGate
          )

          listedDirectories.foreach { children =>
            children.foreach { status =>
              val childPath = status.getPath.toString
              if (status.isDirectory) {
                ignoreMatcher.matched(childPath, inputPath, isDirectory = true) match {
                  case Some(pattern) =>
                    ignoredPaths += ((childPath, pattern))
                  case None =>
                    nextLevelDirectories += status.getPath
                }
              } else if (status.isFile) {
                ignoreMatcher.matched(childPath, inputPath) match {
                  case Some(pattern) =>
                    ignoredPaths += ((childPath, pattern))
                  case None =>
                    discoveredFiles += DiscoveredFile(
                      path = childPath,
                      size = status.getLen,
                      mtimeEpochMs = status.getModificationTime
                    )
                }
              }
            }
          }
        }

      currentLevelDirectories = nextLevelDirectories.toSeq
    }

    (discoveredFiles.toSeq.sortBy(_.path), ignoredPaths.toSeq)
  }
}
