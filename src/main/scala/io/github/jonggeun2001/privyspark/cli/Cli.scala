package io.github.jonggeun2001.privyspark.cli

import io.github.jonggeun2001.privyspark.report.OutputFormats
import scopt.OParser
import scopt.{DefaultOParserSetup, OEffect, OEffectSetup}

import java.util.Locale
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
  excelMaxRowsInMemory: Option[Int] = None,
  excelByteArrayMaxOverride: Option[Int] = None,
  outputFormats: Seq[String] = Seq.empty,
  ignorePatterns: Seq[String] = Seq.empty,
  ignoreFile: Option[String] = None,
  allowlist: Option[String] = None,
  reviewStateRoot: Option[String] = None,
  reviewHtmlDir: Option[String] = None,
  reviewSampleMode: String = "masked",
  suppressions: Seq[String] = Seq.empty,
  suppressionFile: Option[String] = None,
  hiveMetastoreJdbcUrl: Option[String] = None,
  hiveMetastoreUser: Option[String] = None,
  hiveMetastorePasswordFile: Option[String] = None,
  hiveMetastoreJdbcDriverClass: Option[String] = None
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

final case class ReviewCollectCliConfig(
  scanResultsPath: String = "",
  reviewStateRoot: String = ""
)

sealed trait CliCommand

object CliCommand {
  final case class Scan(config: CliConfig) extends CliCommand
  final case class ReviewApply(config: ReviewApplyCliConfig) extends CliCommand
  final case class ReviewCollect(config: ReviewCollectCliConfig) extends CliCommand
}

private[privyspark] final case class CliParseResult(command: Option[CliCommand], errors: Seq[String])

object Cli {
  private val scanBuilder = OParser.builder[CliConfig]
  private val reviewApplyBuilder = OParser.builder[ReviewApplyCliConfig]
  private val reviewCollectBuilder = OParser.builder[ReviewCollectCliConfig]

  private object QuietParserSetup extends DefaultOParserSetup {
    override def showUsageOnError: Option[Boolean] = Some(false)
  }

  private def validateSuppressionArgument(value: String): Option[String] = {
    val trimmed = Option(value).map(_.trim).getOrElse("")
    val delimiterIndex = trimmed.lastIndexOf(':')
    if (delimiterIndex <= 0 || delimiterIndex >= trimmed.length - 1) {
      Some("suppress must use column:pii_type with non-empty values")
    } else {
      val columnName = trimmed.substring(0, delimiterIndex).trim
      val piiType = trimmed.substring(delimiterIndex + 1).trim
      if (columnName.nonEmpty && piiType.nonEmpty) None
      else Some("suppress must use column:pii_type with non-empty values")
    }
  }

  private def validateReviewHtmlDir(value: String): Option[String] = {
    val trimmed = Option(value).map(_.trim).getOrElse("")
    if (trimmed.isEmpty) {
      Some("review-html-dir must not be blank")
    } else {
      val normalized = trimmed.reverse.dropWhile(ch => ch == '/' || ch == '\\').reverse
      val lastSlashIndex = math.max(normalized.lastIndexOf('/'), normalized.lastIndexOf('\\'))
      val leafName =
        if (lastSlashIndex >= 0) normalized.substring(lastSlashIndex + 1)
        else normalized
      val lowerLeafName = leafName.toLowerCase(Locale.ROOT)
      if (lowerLeafName.endsWith(".html") || lowerLeafName.endsWith(".htm") ||
          lowerLeafName.endsWith(".xlsm") || lowerLeafName.endsWith(".xlsx")) {
        Some("review-html-dir must be a directory path, not a review file path")
      } else {
        None
      }
    }
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
      opt[Int]("excel-max-rows-in-memory")
        .optional()
        .action((value, config) => config.copy(excelMaxRowsInMemory = Some(value)))
        .validate { value =>
          if (value > 0) success
          else failure("excel-max-rows-in-memory must be > 0")
        }
        .text("호환용 xlsx 옵션(정수 > 0, 현재 scan 경로에서는 사용하지 않음)"),
      opt[Int]("excel-byte-array-max-override")
        .optional()
        .action((value, config) => config.copy(excelByteArrayMaxOverride = Some(value)))
        .validate { value =>
          if (value > 0) success
          else failure("excel-byte-array-max-override must be > 0")
        }
        .text("POI IOUtils.setByteArrayMaxOverride 값(정수 > 0, 미지정 시 300000000)"),
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
        .text("false positive suppression allowlist JSONL 경로"),
      opt[String]("review-state-root")
        .optional()
        .action((value, config) => config.copy(reviewStateRoot = Some(value)))
        .text("누적 offline review state root 경로"),
      opt[String]("review-html-dir")
        .optional()
        .action((value, config) => config.copy(reviewHtmlDir = Some(value.trim)))
        .validate(value => validateReviewHtmlDir(value).fold(success)(failure))
        .text("offline review HTML 출력 디렉토리(미지정 시 <output>/review, 파일명은 review.html 고정)"),
      opt[String]("review-sample-mode")
        .optional()
        .action((value, config) => config.copy(reviewSampleMode = value.trim.toLowerCase))
        .validate { value =>
          val normalized = Option(value).map(_.trim.toLowerCase).getOrElse("")
          if (Set("raw", "masked", "none").contains(normalized)) success
          else failure("review-sample-mode must be one of: raw, masked, none")
        }
        .text("review.html 샘플 표시 방식(raw, masked, none)"),
      opt[String]("suppress")
        .unbounded()
        .optional()
        .action((value, config) => config.copy(suppressions = config.suppressions :+ value.trim))
        .validate(value => validateSuppressionArgument(value).fold(success)(failure))
        .text("column:pii_type 조합을 결과에서 제외. 반복 지정 가능"),
      opt[String]("suppression-file")
        .optional()
        .action((value, config) => config.copy(suppressionFile = Some(value)))
        .validate { value =>
          if (Option(value).exists(_.trim.nonEmpty)) success
          else failure("suppression-file must not be blank")
        }
        .text("줄 단위 suppression 파일 경로"),
      opt[String]("hive-metastore-jdbc-url")
        .optional()
        .action((value, config) => config.copy(hiveMetastoreJdbcUrl = Some(value.trim)))
        .validate { value =>
          if (Option(value).exists(_.trim.nonEmpty)) success
          else failure("hive-metastore-jdbc-url must not be blank")
        }
        .text("Hive Metastore JDBC URL"),
      opt[String]("hive-metastore-user")
        .optional()
        .action((value, config) => config.copy(hiveMetastoreUser = Some(value.trim)))
        .validate { value =>
          if (Option(value).exists(_.trim.nonEmpty)) success
          else failure("hive-metastore-user must not be blank")
        }
        .text("Hive Metastore read-only user"),
      opt[String]("hive-metastore-password-file")
        .optional()
        .action((value, config) => config.copy(hiveMetastorePasswordFile = Some(value.trim)))
        .validate { value =>
          if (Option(value).exists(_.trim.nonEmpty)) success
          else failure("hive-metastore-password-file must not be blank")
        }
        .text("Hive Metastore password file path or URI"),
      opt[String]("hive-metastore-jdbc-driver-class")
        .optional()
        .action((value, config) => config.copy(hiveMetastoreJdbcDriverClass = Some(value.trim)))
        .validate { value =>
          if (Option(value).exists(_.trim.nonEmpty)) success
          else failure("hive-metastore-jdbc-driver-class must not be blank")
        }
        .text("Hive Metastore JDBC driver class name; falls back to spark.privyspark.hiveMetastore.jdbcDriverClass, then org.mariadb.jdbc.Driver"),
      checkConfig { config =>
        val configured = Seq(config.hiveMetastoreJdbcUrl, config.hiveMetastoreUser, config.hiveMetastorePasswordFile).count(_.nonEmpty)
        if (configured == 0 || configured == 3) success
        else failure("hive metastore lookup requires all of --hive-metastore-jdbc-url, --hive-metastore-user, and --hive-metastore-password-file")
      }
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

  private val reviewCollectParser = {
    import reviewCollectBuilder._

    OParser.sequence(
      programName("privyspark review collect"),
      head("PrivySpark", "0.1.0"),
      opt[String]("scan-results")
        .optional()
        .action((value, config) => config.copy(scanResultsPath = value))
        .text("deprecated: recurring review collect는 response JSON만 사용합니다"),
      opt[String]("review-state-root")
        .required()
        .action((value, config) => config.copy(reviewStateRoot = value))
        .text("누적 offline review state root 경로")
    )
  }

  def parse(args: Array[String]): Option[CliCommand] = parseWithErrors(args).command

  private[privyspark] def parseWithErrors(args: Array[String]): CliParseResult = {
    args.toList match {
      case "review" :: "apply" :: tail =>
        parseReviewApply(tail)
      case "review" :: "collect" :: tail =>
        parseReviewCollect(tail)
      case "review" :: _ =>
        CliParseResult(None, Seq("review subcommand must be one of: apply, collect"))
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

  private def parseReviewCollect(args: Seq[String]): CliParseResult = {
    val (config, effects) = OParser.runParser(reviewCollectParser, args, ReviewCollectCliConfig(), QuietParserSetup)
    CliParseResult(config.map(CliCommand.ReviewCollect), collectErrors(effects))
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
