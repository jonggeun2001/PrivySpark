package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.cli.{Cli, CliArgumentValidator, CliCommand, CliConfig, ReviewApplyCliConfig, ReviewCollectCliConfig}
import io.github.jonggeun2001.privyspark.review.{ReviewApplyCommand, ReviewCollectCommand, ReviewHtmlWriter}
import io.github.jonggeun2001.privyspark.scan.ScanPipeline
import io.github.jonggeun2001.privyspark.util.{DriverLogLevel, DriverLogger}
import org.apache.spark.sql.SparkSession

import scala.util.control.ControlThrowable
import scala.util.control.NonFatal

object PrivySparkApp {
  def main(args: Array[String]): Unit = {
    runMain(args)
  }

  private[privyspark] def runMain(
    args: Array[String],
    createSparkSession: () => SparkSession = () => buildDefaultSparkSession(),
    exitWith: Int => Unit = code => System.exit(code),
    runScanCommand: (SparkSession, CliConfig) => Unit = runScan,
    runReviewApplyCommand: (SparkSession, ReviewApplyCliConfig) => Unit = ReviewApplyCommand.run,
    runReviewCollectCommand: (SparkSession, ReviewCollectCliConfig) => Unit = ReviewCollectCommand.run
  ): Unit = {
    val parseResult = Cli.parseWithErrors(args)
    val command = parseResult.command.getOrElse {
      DriverLogger.emitAlways(
        DriverLogLevel.Error,
        "cli_argument_invalid",
        "errors" -> parseResult.errors.mkString(" | "),
        "args" -> args.mkString(" ")
      )
      exitWith(2)
      return
    }

    CliArgumentValidator.validate(command).foreach { code =>
      exitWith(code)
      return
    }

    var spark: Option[SparkSession] = None

    try {
      val session = createSparkSession()
      spark = Some(session)
      session.sparkContext.setLogLevel("WARN")
      command match {
        case CliCommand.Scan(config) =>
          config.reviewStateRoot.foreach { root =>
            runReviewCollectCommand(session, ReviewCollectCliConfig(reviewStateRoot = root))
          }
          runScanCommand(session, config)
        case CliCommand.ReviewApply(config) =>
          runReviewApplyCommand(session, config)
        case CliCommand.ReviewCollect(config) =>
          runReviewCollectCommand(session, config)
      }
    } catch {
      case control: ControlThrowable =>
        throw control
      case NonFatal(e) =>
        DriverLogger.emitAlways(
          DriverLogLevel.Error,
          "scan_failed",
          "exception" -> e.getClass.getSimpleName,
          "reason" -> Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        )
        exitWith(1)
    } finally {
      spark.foreach(_.stop())
    }
  }

  private[privyspark] def buildDefaultSparkSession(): SparkSession = {
    SparkSession.builder().appName("PrivySpark").getOrCreate()
  }

  private def runScan(spark: SparkSession, config: CliConfig): Unit = {
    val summary = ScanPipeline.run(
      spark,
      config,
      ScanPipeline.Hooks(
        warnUnusedExcelMaxRowsInMemory = warnUnusedExcelMaxRowsInMemory,
        writeReviewHtml = (conf, outputPath, inputPath, resultDf, sampleMode, reviewHtmlDir, reviewStateRoot) =>
          ReviewHtmlWriter.write(conf, outputPath, inputPath, resultDf, sampleMode, reviewHtmlDir, reviewStateRoot)
      )
    )
    println(summary.consoleLine)
  }

  private[privyspark] def warnUnusedExcelMaxRowsInMemory(configured: Option[Int]): Unit = {
    configured.foreach { value =>
      DriverLogger.warn(
        "excel_max_rows_in_memory_unused",
        "argument" -> "--excel-max-rows-in-memory",
        "value" -> value,
        "reason" -> "executor_side_xlsx_scan"
      )
    }
  }
}
