package io.github.jonggeun2001.privyspark.format

object FormatDetector {
  final case class DetectedInput(
    baseFormat: Option[String],
    codec: Option[String],
    archiveFormat: Option[String],
    isArchive: Boolean
  )

  private val ProbeSkipExtensions = Set(
    ".class", ".7z", ".tar", ".rar",
    ".pdf", ".doc", ".docx", ".ppt", ".pptx",
    ".png", ".jpg", ".jpeg", ".gif", ".mp4", ".mp3"
  )

  private val BaseFormatsBySuffix = Seq(
    ".jsonl" -> "json",
    ".ndjson" -> "json",
    ".json" -> "json",
    ".csv" -> "csv",
    ".parquet" -> "parquet",
    ".orc" -> "orc",
    ".avro" -> "avro",
    ".xlsx" -> "xlsx"
  )

  private val ArchiveAliases = Seq(
    ".tgz" -> ("tar", Some("gz")),
    ".tbz2" -> ("tar", Some("bz2")),
    ".txz" -> ("tar", Some("xz")),
    ".tzst" -> ("tar", Some("zst")),
    ".zip" -> ("zip", None),
    ".jar" -> ("jar", None),
    ".7z" -> ("7z", None),
    ".rar" -> ("rar", None)
  )

  private val CodecSuffixes = Seq(".gz" -> "gz", ".bz2" -> "bz2", ".xz" -> "xz", ".zst" -> "zst")

  def infer(filePath: String): Option[String] = {
    detect(filePath).flatMap(detected => detected.archiveFormat.orElse(detected.baseFormat))
  }

  def detect(filePath: String): Option[DetectedInput] = {
    val lower = Option(filePath).getOrElse("").toLowerCase

    ArchiveAliases.collectFirst {
      case (suffix, (archiveFormat, codec)) if lower.endsWith(suffix) =>
        DetectedInput(baseFormat = None, codec = codec, archiveFormat = Some(archiveFormat), isArchive = true)
    }.orElse {
      CodecSuffixes.collectFirst(Function.unlift {
        case (suffix, codec) if lower.endsWith(suffix) =>
          val withoutCodec = lower.dropRight(suffix.length)
          if (withoutCodec.endsWith(".tar")) {
            Some(DetectedInput(baseFormat = None, codec = Some(codec), archiveFormat = Some("tar"), isArchive = true))
          } else {
            BaseFormatsBySuffix.collectFirst {
              case (baseSuffix, baseFormat) if withoutCodec.endsWith(baseSuffix) =>
                DetectedInput(baseFormat = Some(baseFormat), codec = Some(codec), archiveFormat = None, isArchive = false)
            }
          }
        case _ => None
      })
    }.orElse {
      BaseFormatsBySuffix.collectFirst {
        case (suffix, baseFormat) if lower.endsWith(suffix) =>
          DetectedInput(baseFormat = Some(baseFormat), codec = None, archiveFormat = None, isArchive = false)
      }
    }.orElse {
      if (lower.endsWith(".tar")) {
        Some(DetectedInput(baseFormat = None, codec = None, archiveFormat = Some("tar"), isArchive = true))
      } else {
        None
      }
    }
  }

  def shouldSkipProbe(filePath: String): Boolean = {
    val lower = Option(filePath).getOrElse("").toLowerCase
    ProbeSkipExtensions.exists(lower.endsWith)
  }
}
