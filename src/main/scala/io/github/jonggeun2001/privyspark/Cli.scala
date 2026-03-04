package io.github.jonggeun2001.privyspark

import scopt.OParser

final case class CliConfig(
  inputPath: String = "",
  outputPath: String = "",
  ruleset: String = "default",
  sampleRatio: Double = 0.2
)

object Cli {
  private val builder = OParser.builder[CliConfig]

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
        .text("샘플링 비율(0.0, 1.0]")
    )
  }

  def parse(args: Array[String]): Option[CliConfig] = OParser.parse(parser, args, CliConfig())
}
