package io.github.jonggeun2001.privyspark

import scopt.OParser
import scopt.{DefaultOParserSetup, OEffectSetup}

import scala.collection.mutable.ArrayBuffer

final case class CliConfig(
  inputPath: String = "",
  outputPath: String = "",
  ruleset: String = "default",
  sampleRatio: Double = 0.2,
  fileSampleRatio: Option[Double] = None,
  preScanParallelism: Option[Int] = None,
  groupParallelism: Option[Int] = None,
  fileParallelism: Option[Int] = None,
  ignorePatterns: Seq[String] = Seq.empty,
  ignoreFile: Option[String] = None
)

private[privyspark] final case class CliParseResult(config: Option[CliConfig], errors: Seq[String])

object Cli {
  private val builder = OParser.builder[CliConfig]
  private object QuietParserSetup extends DefaultOParserSetup {
    override def showUsageOnError: Option[Boolean] = Some(false)
  }

  private val parser = {
    import builder._

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
      opt[String]("ignore")
        .unbounded()
        .optional()
        .action((value, config) => config.copy(ignorePatterns = config.ignorePatterns :+ value))
        .text("gitignore 스타일 glob 패턴으로 스캔 대상을 제외"),
      opt[String]("ignore-file")
        .optional()
        .action((value, config) => config.copy(ignoreFile = Some(value)))
        .text("줄 단위 ignore 패턴 파일 경로")
    )
  }

  def parse(args: Array[String]): Option[CliConfig] = parseWithErrors(args).config

  private[privyspark] def parseWithErrors(args: Array[String]): CliParseResult = {
    val (config, effects) = OParser.runParser(parser, args.toSeq, CliConfig(), QuietParserSetup)
    val errors = ArrayBuffer.empty[String]

    OParser.runEffects(effects, new OEffectSetup {
      override def displayToOut(message: String): Unit = ()

      override def displayToErr(message: String): Unit = ()

      override def reportError(message: String): Unit = {
        errors += message
      }

      override def reportWarning(message: String): Unit = ()

      override def terminate(exitState: Either[String, Unit]): Unit = ()
    })

    CliParseResult(config, errors.toSeq.filter(_.trim.nonEmpty).distinct)
  }
}
