package io.github.jonggeun2001.privyspark.config

import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkEnv, SparkFiles}

import java.nio.file.{Files, Paths}

private[config] object LocalConfigFileResolver {
  def resolve(path: String): Option[java.nio.file.Path] = {
    val uri = new Path(path).toUri
    if (uri.getScheme != null || uri.getAuthority != null) {
      None
    } else {
      val sparkFilesCandidate = Option(SparkEnv.get).map(_ => Paths.get(SparkFiles.get(path)))
      val workingDirectoryCandidate = Paths.get(path)

      Seq(sparkFilesCandidate, Some(workingDirectoryCandidate)).flatten.collectFirst {
        case candidate if Files.exists(candidate) => candidate.toAbsolutePath.normalize()
      }
    }
  }
}
