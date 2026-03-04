package io.github.jonggeun2001.privyspark

import java.nio.file.Paths

object PathValidator {
  private val UriWithScheme = "^[a-zA-Z][a-zA-Z0-9+.-]*://.+$".r

  def isAbsolute(path: String): Boolean = {
    if (path == null || path.trim.isEmpty) {
      false
    } else {
      path match {
        case UriWithScheme() => true
        case _ =>
          try {
            Paths.get(path).isAbsolute
          } catch {
            case _: Exception => false
          }
      }
    }
  }
}
