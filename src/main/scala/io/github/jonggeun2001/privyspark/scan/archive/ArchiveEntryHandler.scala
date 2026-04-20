package io.github.jonggeun2001.privyspark.scan.archive

import java.io.Closeable
import java.io.InputStream

private[privyspark] trait ArchiveEntryHandler extends Closeable {
  def nextEntry(): Boolean
  def entryName: String
  def entrySize: Long
  def isDirectory: Boolean
  def withEntryInputStream(process: InputStream => Unit): Unit
}
