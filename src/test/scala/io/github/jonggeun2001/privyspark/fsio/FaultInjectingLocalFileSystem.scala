package io.github.jonggeun2001.privyspark.fsio

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path, RawLocalFileSystem}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.security.AccessControlException

import java.io.{FilterOutputStream, IOException, OutputStream}
import java.net.URI

// Keep real local IO except for a single operation on a named test path.
class FaultInjectingLocalFileSystem extends RawLocalFileSystem {
  private def mode(operation: String, path: Path): String = {
    if (getConf.get("test.fs.operation", "") == operation && getConf.get("test.fs.name", "") == path.getName) {
      val result = getConf.get("test.fs.mode", "deny")
      if (result == "deny") throw new AccessControlException(s"Denied $operation: $path")
      result
    } else ""
  }

  override def open(path: Path, bufferSize: Int): FSDataInputStream = {
    mode("open", path)
    super.open(path, bufferSize)
  }

  override protected def createOutputStreamWithMode(path: Path, append: Boolean, permission: FsPermission): OutputStream = {
    val failure = mode("create", path)
    val output = super.createOutputStreamWithMode(path, append, permission)
    if (failure == "write") {
      new FilterOutputStream(output) {
        override def write(value: Int): Unit = throw new IOException("injected write failure")
        override def write(bytes: Array[Byte], offset: Int, length: Int): Unit =
          throw new IOException("injected write failure")
      }
    } else output
  }

  override def rename(source: Path, target: Path): Boolean =
    if (mode("rename", source) == "false") false else super.rename(source, target)

  override def delete(path: Path, recursive: Boolean): Boolean =
    if (mode("delete", path) == "false") false else super.delete(path, recursive)

  override def mkdirs(path: Path): Boolean =
    if (mode("mkdirs", path) == "false") false else super.mkdirs(path)
}

class OtherLocalFileSystem extends RawLocalFileSystem {
  override def getUri: URI = URI.create("otherfile:///")
}

object FaultInjectingLocalFileSystem {
  def configuration(operation: String, name: String, mode: String = "deny"): Configuration = {
    val conf = new Configuration(false)
    conf.set("fs.file.impl", classOf[FaultInjectingLocalFileSystem].getName)
    conf.setBoolean("fs.file.impl.disable.cache", true)
    conf.set("test.fs.operation", operation)
    conf.set("test.fs.name", name)
    conf.set("test.fs.mode", mode)
    conf
  }
}
