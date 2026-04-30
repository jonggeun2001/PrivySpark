package io.github.jonggeun2001.privyspark.scan

@deprecated("use GroupScanCoordinator and FileSampling", "1.5.0")
private[privyspark] object GroupScanner {
  @deprecated("use FileSampling.selectSampledFileKeys", "1.5.0")
  def selectSampledFileKeys(fileKeys: Seq[String], fileSampleRatio: Double, samplingContext: String = ""): Seq[String] = {
    FileSampling.selectSampledFileKeys(fileKeys, fileSampleRatio, samplingContext)
  }
}
