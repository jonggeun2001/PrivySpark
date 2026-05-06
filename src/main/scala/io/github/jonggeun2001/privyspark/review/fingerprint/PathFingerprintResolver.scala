package io.github.jonggeun2001.privyspark.review.fingerprint

import io.github.jonggeun2001.privyspark.review.ResolvedFileFingerprint
import io.github.jonggeun2001.privyspark.util.PathIdentifiers.resolveRelativeIdentifier
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.collection.mutable.ArrayBuffer

private[review] object PathFingerprintResolver {
  def resolve(
    conf: Configuration,
    inputRoot: String,
    fileIdentifier: String
  ): Either[String, Seq[ResolvedFileFingerprint]] = {
    val resolvedPath = resolveInputPath(conf, inputRoot, fileIdentifier)
    val path = new Path(resolvedPath)
    val fs = path.getFileSystem(conf)
    if (!fs.exists(path)) {
      Left(s"Resolved path not found: $fileIdentifier")
    } else {
      val status = fs.getFileStatus(path)
      if (status.isDirectory) {
        resolveDirectoryFingerprints(conf, inputRoot, path)
      } else {
        resolveFlatFileFingerprint(conf, fileIdentifier, resolvedPath, status).map(Seq(_))
      }
    }
  }

  private[fingerprint] def resolveInputPath(
    conf: Configuration,
    inputRoot: String,
    relativeIdentifier: String
  ): String = {
    val inputPath = new Path(inputRoot)
    val fs = inputPath.getFileSystem(conf)
    val inputIsFile = fs.exists(inputPath) && fs.getFileStatus(inputPath).isFile
    val normalizedIdentifier = Option(relativeIdentifier).getOrElse("")

    if (normalizedIdentifier == "." || normalizedIdentifier.isEmpty) {
      inputRoot
    } else if (inputIsFile && normalizedIdentifier == inputPath.getName) {
      inputRoot
    } else if (inputIsFile) {
      Option(inputPath.getParent).map(parent => new Path(parent, normalizedIdentifier).toString).getOrElse(inputRoot)
    } else {
      new Path(inputPath, normalizedIdentifier).toString
    }
  }

  private def resolveDirectoryFingerprints(
    conf: Configuration,
    inputRoot: String,
    directoryPath: Path
  ): Either[String, Seq[ResolvedFileFingerprint]] = {
    val fs = directoryPath.getFileSystem(conf)
    val statuses = Option(fs.listStatus(directoryPath)).getOrElse(Array.empty)
      .filter(status => status.isFile)
      .sortBy(_.getPath.toString)

    val fingerprints = ArrayBuffer.empty[ResolvedFileFingerprint]
    statuses.foreach { status =>
      val childPath = status.getPath.toString
      val childIdentifier = resolveRelativeIdentifier(inputRoot, childPath)
      resolveFlatFileFingerprint(conf, childIdentifier, childPath, status) match {
        case Right(fingerprint) =>
          fingerprints += fingerprint
        case Left(errorMessage) =>
          return Left(errorMessage)
      }
    }

    Right(fingerprints.toSeq)
  }

  private def resolveFlatFileFingerprint(
    conf: Configuration,
    fileIdentifier: String,
    physicalPath: String,
    status: org.apache.hadoop.fs.FileStatus
  ): Either[String, ResolvedFileFingerprint] = {
    Crc32Stream.crc32ForFile(conf, physicalPath).map { checksum =>
      ResolvedFileFingerprint(
        fileIdentifier = fileIdentifier,
        physicalPath = physicalPath,
        fileSize = status.getLen,
        fileMtimeEpochMs = status.getModificationTime,
        fileChecksumAlgo = Crc32Stream.DefaultChecksumAlgo,
        fileChecksum = checksum
      )
    }
  }
}
