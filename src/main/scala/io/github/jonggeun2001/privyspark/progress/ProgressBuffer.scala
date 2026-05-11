package io.github.jonggeun2001.privyspark.progress

import io.github.jonggeun2001.privyspark.model.{ProgressRun, ScanError, ScanResult}
import io.github.jonggeun2001.privyspark.report.JsonCodec._
import io.github.jonggeun2001.privyspark.util.DriverLogger

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.collection.mutable.ArrayBuffer

private[privyspark] final class ProgressBuffer(
  conf: org.apache.hadoop.conf.Configuration,
  progressRun: ProgressRun,
  scope: String,
  identifier: String
) {
  private val resultLines = new ConcurrentLinkedQueue[String]()
  private val errorLines = new ConcurrentLinkedQueue[String]()
  private val resultCount = new AtomicInteger(0)
  private val errorCount = new AtomicInteger(0)
  private val flushed = new AtomicBoolean(false)

  def enqueue(results: Seq[ScanResult], errors: Seq[ScanError]): Unit = {
    results.foreach(result => resultLines.add(scanResultToJson(result)))
    errors.foreach(error => errorLines.add(scanErrorToJson(error)))
    resultCount.addAndGet(results.size)
    errorCount.addAndGet(errors.size)
  }

  def flush(): Unit = {
    if (!flushed.compareAndSet(false, true)) {
      return
    }

    val results = drain(resultLines)
    val errors = drain(errorLines)
    ProgressIO.writeProgressLines(conf, progressRun.resultsPath, scope, results)
    ProgressIO.writeProgressLines(conf, progressRun.errorsPath, scope, errors)
    ProgressIO.writeProgressLines(
      conf,
      progressRun.completionsPath,
      scope,
      Seq(progressCompletionToJson(scope, identifier, resultCount.get(), errorCount.get()))
    )
    DriverLogger.debug(
      "progress_buffer_flushed",
      "run_id" -> progressRun.runId,
      "scope" -> scope,
      "identifier" -> identifier,
      "results" -> resultCount.get(),
      "errors" -> errorCount.get()
    )
  }

  private def drain(queue: ConcurrentLinkedQueue[String]): Seq[String] = {
    val lines = ArrayBuffer.empty[String]
    var line = queue.poll()
    while (line != null) {
      lines += line
      line = queue.poll()
    }
    lines.toSeq
  }
}
