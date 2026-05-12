package io.github.jonggeun2001.privyspark.scan

import java.io.FileNotFoundException
import java.util.Locale

private[privyspark] object DeletedFileDetection {
  def isDeletedFile(error: FileNotFoundException): Boolean = {
    val message = Option(error.getMessage).getOrElse("").toLowerCase(Locale.ROOT)
    !message.contains("permission denied") &&
      !message.contains("operation not permitted") &&
      !message.contains("access denied") &&
      !message.contains("access is denied")
  }
}
