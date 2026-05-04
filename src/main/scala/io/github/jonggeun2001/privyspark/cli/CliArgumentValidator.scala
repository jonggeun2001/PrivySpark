package io.github.jonggeun2001.privyspark.cli

import io.github.jonggeun2001.privyspark.util.{DriverLogLevel, DriverLogger}

private[privyspark] object CliArgumentValidator {
  def validate(command: CliCommand): Option[Int] = {
    command match {
      case CliCommand.Scan(config) =>
        firstErrorCode(
          () => validateAbsoluteArgument("--path", config.inputPath),
          () => validateAbsoluteArgument("--output", config.outputPath),
          () => validateOptionalAbsoluteArgument("--allowlist", config.allowlist),
          () => validateOptionalAbsoluteArgument("--review-state-root", config.reviewStateRoot),
          () => validateOptionalAbsoluteArgument("--review-html-dir", config.reviewHtmlDir),
          () => validateOptionalAbsoluteArgument("--hive-metastore-password-file", config.hiveMetastorePasswordFile)
        )
      case CliCommand.ReviewApply(config) =>
        firstErrorCode(
          () => validateAbsoluteArgument("--scan-results", config.scanResultsPath),
          () => validateAbsoluteArgument("--input-root", config.inputRoot),
          () => validateAbsoluteArgument("--allowlist", config.allowlistPath)
        )
      case CliCommand.ReviewCollect(config) =>
        firstErrorCode(
          () =>
            if (config.scanResultsPath.trim.nonEmpty) {
              validateAbsoluteArgument("--scan-results", config.scanResultsPath)
            } else {
              None
            },
          () => validateAbsoluteArgument("--review-state-root", config.reviewStateRoot)
        )
    }
  }

  private def firstErrorCode(validators: (() => Option[Int])*): Option[Int] =
    validators.iterator.map(_.apply()).collectFirst { case Some(code) => code }

  private def validateOptionalAbsoluteArgument(argument: String, value: Option[String]): Option[Int] =
    value.flatMap(path => validateAbsoluteArgument(argument, path))

  private def validateAbsoluteArgument(argument: String, value: String): Option[Int] = {
    if (PathValidator.isAbsolute(value)) {
      None
    } else {
      emitAbsolutePathError(argument, value)
      Some(2)
    }
  }

  private def emitAbsolutePathError(argument: String, value: String): Unit = {
    DriverLogger.emitAlways(
      DriverLogLevel.Error,
      "cli_argument_invalid",
      "argument" -> argument,
      "reason" -> "must_be_absolute_path_or_uri",
      "value" -> value
    )
  }
}
