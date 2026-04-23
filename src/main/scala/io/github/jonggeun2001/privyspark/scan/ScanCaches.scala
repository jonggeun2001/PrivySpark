package io.github.jonggeun2001.privyspark.scan

import io.github.jonggeun2001.privyspark.model.{CachedSchemaSignature, ScanReadOptions}

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import java.util.concurrent.atomic.AtomicInteger

private[privyspark] final class CsvHeadCache {
  private val cachedLinesByFile = new ConcurrentHashMap[String, Seq[String]]()
  private val cachedCharactersByFile = new ConcurrentHashMap[String, Int]()
  private val insertionOrder = new ConcurrentLinkedQueue[String]()
  private val totalCachedCharacters = new AtomicInteger(0)

  def getOrRead(filePath: String)(loader: => Seq[String]): Seq[String] = {
    Option(cachedLinesByFile.get(filePath)).getOrElse {
      val loaded = loader
      cacheLoaded(filePath, loaded)
      Option(cachedLinesByFile.get(filePath)).getOrElse(loaded)
    }
  }

  def clear(): Unit = this.synchronized {
    cachedLinesByFile.clear()
    cachedCharactersByFile.clear()
    insertionOrder.clear()
    totalCachedCharacters.set(0)
  }

  private def cacheLoaded(filePath: String, lines: Seq[String]): Unit = this.synchronized {
    if (cachedLinesByFile.containsKey(filePath)) {
      return
    }

    val characterCount = lines.iterator.map(_.length).sum
    if (characterCount > CsvHeadCache.MaxCharacters) {
      return
    }

    cachedLinesByFile.put(filePath, lines)
    cachedCharactersByFile.put(filePath, characterCount)
    insertionOrder.add(filePath)
    totalCachedCharacters.addAndGet(characterCount)
    evictToBounds()
  }

  private def evictToBounds(): Unit = {
    while (cachedLinesByFile.size() > CsvHeadCache.MaxEntries || totalCachedCharacters.get() > CsvHeadCache.MaxCharacters) {
      val oldestPath = insertionOrder.poll()
      if (oldestPath == null) {
        return
      }

      val removed = cachedLinesByFile.remove(oldestPath)
      if (removed != null) {
        val removedCharacterCount = Option(cachedCharactersByFile.remove(oldestPath)).map(_.toInt).getOrElse(removed.iterator.map(_.length).sum)
        totalCachedCharacters.addAndGet(-removedCharacterCount)
      }
    }
  }
}

private[privyspark] object CsvHeadCache {
  val CachedLineLimit = 16
  val MaxEntries = 4096
  val MaxCharacters = 1024 * 1024
}

private[privyspark] final class SchemaSignatureCache {
  private val cache = new ConcurrentHashMap[String, CachedSchemaSignature]()
  private val insertionOrder = new ConcurrentLinkedQueue[String]()

  def getOrCompute(
    filePath: String,
    format: String,
    readOptions: ScanReadOptions = ScanReadOptions()
  )(loader: => CachedSchemaSignature): CachedSchemaSignature = {
    val key = cacheKey(filePath, format, readOptions)
    Option(cache.get(key)).getOrElse {
      val loaded = loader
      val existing = cache.putIfAbsent(key, loaded)
      if (existing == null) {
        insertionOrder.offer(key)
        evictIfNeeded()
        loaded
      } else {
        existing
      }
    }
  }

  def clear(): Unit = this.synchronized {
    cache.clear()
    insertionOrder.clear()
  }

  private def cacheKey(filePath: String, format: String, readOptions: ScanReadOptions): String =
    s"$format::$filePath::$readOptions"

  private def evictIfNeeded(): Unit = this.synchronized {
    while (cache.size() > SchemaSignatureCache.MaxEntries) {
      val oldestKey = insertionOrder.poll()
      if (oldestKey == null) {
        return
      }
      cache.remove(oldestKey)
    }
  }
}

private object SchemaSignatureCache {
  val MaxEntries = 4096
}

private[privyspark] final class ParseOkCache {
  private val okPaths = ConcurrentHashMap.newKeySet[String]()

  def markOk(path: String): Unit = okPaths.add(path)
  def isOk(path: String): Boolean = okPaths.contains(path)
  def clear(): Unit = okPaths.clear()
}
