package io.github.jonggeun2001.privyspark.scan

import scala.util.hashing.MurmurHash3

private[privyspark] object FileSampling {
  def selectSampledFileKeys(fileKeys: Seq[String], fileSampleRatio: Double, samplingContext: String = ""): Seq[String] = {
    require(fileKeys.nonEmpty, "fileKeys must not be empty")
    require(fileSampleRatio > 0.0 && fileSampleRatio <= 1.0, "fileSampleRatio must be > 0.0 and <= 1.0")

    val sampleSize = math.max(1, math.min(fileKeys.size, math.ceil(fileKeys.size * fileSampleRatio).toInt))
    val selectedKeySet = fileKeys
      .sortBy(fileKey => (stableSampleHash(samplingContext, fileKey), fileKey))
      .take(sampleSize)
      .toSet
    fileKeys.filter(selectedKeySet.contains)
  }

  private def stableSampleHash(samplingContext: String, fileKey: String): Long =
    java.lang.Integer.toUnsignedLong(MurmurHash3.stringHash(s"${Option(samplingContext).getOrElse("")}\u0000$fileKey"))
}
