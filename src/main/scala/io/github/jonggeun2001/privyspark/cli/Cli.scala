package io.github.jonggeun2001.privyspark.cli

import io.github.jonggeun2001.privyspark.report.OutputFormats
import scopt.OParser
import scopt.{DefaultOParserSetup, OEffect, OEffectSetup}

import scala.collection.mutable.ArrayBuffer

final case class CliConfig(
  inputPath: String = "",
  outputPath: String = "",
  ruleset: String = "default",
  sampleRatio: Double = 0.2,
  fileSampleRatio: Option[Double] = None,
  fileSampleMinFiles: Int = 10,
  preScanParallelism: Option[Int] = None,
  groupParallelism: Option[Int] = None,
  fileParallelism: Option[Int] = None,
  outputFormats: Seq[String] = Seq.empty,
  ignorePatterns: Seq[String] = Seq.empty,
  ignoreFile: Option[String] = None,
  allowlist: Option[String] = None
) {
  def effectiveOutputFormats: Seq[String] = OutputFormats.normalizeAll(outputFormats)
}

final case class ReviewApplyCliConfig(
  scanResultsPath: String = "",
  inputRoot: String = "",
  allowlistPath: String = "",
  reviewer: String = "",
  dryRun: Boolean = false
)

sealed trait CliCommand

object CliCommand {
  final case class Scan(config: CliConfig) extends CliCommand
  final case class ReviewApply(config: ReviewApplyCliConfig) extends CliCommand
}

private[privyspark] final case class CliParseResult(command: Option[CliCommand], errors: Seq[String])

object Cli {
  private val scanBuilder = OParser.builder[CliConfig]
  private val reviewApplyBuilder = OParser.builder[ReviewApplyCliConfig]

  private object QuietParserSetup extends DefaultOParserSetup {
    override def showUsageOnError: Option[Boolean] = Some(false)
  }

  private val scanParser = {
    import scanBuilder._

    OParser.sequence(
      programName("privyspark scan"),
      head("PrivySpark", "0.1.0"),
      opt[String]("path")
        .required()
        .action((value, config) => config.copy(inputPath = value))
        .text("절대경로 입력 경로"),
      opt[String]("output")
        .required()
        .action((value, config) => config.copy(outputPath = value))
        .text("절대경로 출력 경로"),
      opt[String]("ruleset")
        .optional()
        .action((value, config) => config.copy(ruleset = value))
        .text("규칙셋 파일 경로 또는 default"),
      opt[Double]("sample-ratio")
        .optional()
        .action((value, config) => config.copy(sampleRatio = value))
        .validate { value =>
          if (value > 0.0 && value <= 1.0) success
          else failure("sample-ratio must be > 0.0 and <= 1.0")
        }
        .text("샘플링 비율(0.0, 1.0]"),
      opt[Double]("file-sample-ratio")
        .optional()
        .action((value, config) => config.copy(fileSampleRatio = Some(value)))
        .validate { value =>
          if (value > 0.0 && value <= 1.0) success
          else failure("file-sample-ratio must be > 0.0 and <= 1.0")
        }
        .text("그룹 batch scan 파일 샘플링 비율(0.0, 1.0]"),
      opt[Int]("file-sample-min-files")
        .optional()
        .action((value, config) => config.copy(fileSampleMinFiles = value))
        .validate { value =>
          if (value >= 1) success
          else failure("file-sample-min-files must be >= 1")
        }
        .text("file-sample-ratio를 적용할 최소 그룹 파일 수(정수 >= 1)"),
      opt[Int]("pre-scan-parallelism")
        .optional()
        .action((value, config) => config.copy(preScanParallelism = Some(value)))
        .validate { value =>
          if (value > 0) success
          else failure("pre-scan-parallelism must be > 0")
        }
        .text("파일 pre-scan 확장 병렬도(정수 > 0)"),
      opt[Int]("group-parallelism")
        .optional()
        .action((value, config) => config.copy(groupParallelism = Some(value)))
        .validate { value =>
          if (value > 0) success
          else failure("group-parallelism must be > 0")
        }
        .text("그룹 스캔 병렬도(정수 > 0)"),
      opt[Int]("file-parallelism")
        .optional()
        .action((value, config) => config.copy(fileParallelism = Some(value)))
        .validate { value =>
          if (value > 0) success
          else failure("file-parallelism must be > 0")
        }
        .text("파일 폴백 스캔 병렬도(정수 > 0)"),
      opt[String]("output-format")
        .unbounded()
        .optional()
        .action((value, config) =>
          OutputFormats.validate(value).fold(
            _ => config,
            format => config.copy(outputFormats = config.outputFormats :+ format)
          )
        )
        .validate(value => OutputFormats.validate(value).fold(failure, _ => success))
        .text("최종 출력 포맷(parquet, csv, excel). 반복 지정 가능, 기본값 parquet"),
      opt[String]("ignore")
        .unbounded()
        .optional()
        .action((value, config) => config.copy(ignorePatterns = config.ignorePatterns :+ value))
        .text("gitignore 스타일 glob 패턴으로 스캔 대상을 제외"),
      opt[String]("ignore-file")
        .optional()
        .action((value, config) => config.copy(ignoreFile = Some(value)))
        .text("줄 단위 ignore 패턴 파일 경로"),
      opt[String]("allowlist")
        .optional()
        .action((value, config) => config.copy(allowlist = Some(value)))
        .text("false positive suppression allowlist JSONL 경로")
    )
  }

  private val reviewApplyParser = {
    import reviewApplyBuilder._

    OParser.sequence(
      programName("privyspark review apply"),
      head("PrivySpark", "0.1.0"),
      opt[String]("scan-results")
        .required()
        .action((value, config) => config.copy(scanResultsPath = value))
        .text("편집된 scan_results 입력 파일 경로"),
      opt[String]("input-root")
        .required()
        .action((value, config) => config.copy(inputRoot = value))
        .text("원본 스캔 대상 루트 경로"),
      opt[String]("allowlist")
        .required()
        .action((value, config) => config.copy(allowlistPath = value))
        .text("allowlist JSONL 출력 경로"),
      opt[String]("reviewer")
        .required()
        .action((value, config) => config.copy(reviewer = value))
        .text("검토자 식별자"),
      opt[Unit]("dry-run")
        .optional()
        .action((_, config) => config.copy(dryRun = true))
        .text("쓰기 없이 적용 예정 내용만 출력")
    )
  }

  def parse(args: Array[String]): Option[CliCommand] = parseWithErrors(args).command

  private[privyspark] def parseWithErrors(args: Array[String]): CliParseResult = {
    args.toList match {
      case "review" :: "apply" :: tail =>
        parseReviewApply(tail)
      case "review" :: _ =>
        CliParseResult(None, Seq("review subcommand must be one of: apply"))
      case "scan" :: tail =>
        parseScan(tail)
      case _ =>
        parseScan(args.toSeq)
    }
  }

  private def parseScan(args: Seq[String]): CliParseResult = {
    val (config, effects) = OParser.runParser(scanParser, args, CliConfig(), QuietParserSetup)
    CliParseResult(config.map(CliCommand.Scan), collectErrors(effects))
  }

  private def parseReviewApply(args: Seq[String]): CliParseResult = {
    val (config, effects) = OParser.runParser(reviewApplyParser, args, ReviewApplyCliConfig(), QuietParserSetup)
    CliParseResult(config.map(CliCommand.ReviewApply), collectErrors(effects))
  }

  private def collectErrors(effects: Seq[OEffect]): Seq[String] = {
    val errors = ArrayBuffer.empty[String]

    OParser.runEffects(effects.toList, new OEffectSetup {
      override def displayToOut(message: String): Unit = ()

      override def displayToErr(message: String): Unit = ()

      override def reportError(message: String): Unit = {
        errors += message
      }

      override def reportWarning(message: String): Unit = ()

      override def terminate(exitState: Either[String, Unit]): Unit = ()
    })

    errors.toSeq.filter(_.trim.nonEmpty).distinct
  }
}
