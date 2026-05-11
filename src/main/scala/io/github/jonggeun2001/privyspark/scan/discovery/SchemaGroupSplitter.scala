package io.github.jonggeun2001.privyspark.scan.discovery

import io.github.jonggeun2001.privyspark.format.CsvInference._
import io.github.jonggeun2001.privyspark.fsio.RetryIO.withFileReadRetry
import io.github.jonggeun2001.privyspark.model.{ScanError, ScanGroup}
import io.github.jonggeun2001.privyspark.scan.{CsvHeadCache, ParseOkCache, SchemaSignatureCache}
import io.github.jonggeun2001.privyspark.util.{DriverLogger, RpcGate}
import io.github.jonggeun2001.privyspark.util.ParallelismConfig.{executeInParallel, resolveParallelism}
import io.github.jonggeun2001.privyspark.util.PathIdentifiers._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

private[privyspark] final case class SplitAndFinalizeResult(
  groups: Seq[ScanGroup],
  errors: Seq[ScanError],
  directoriesWithPreScanErrors: Set[String]
)

private[privyspark] object SchemaGroupSplitter {
  def splitAndFinalize(
    spark: SparkSession,
    datasetPath: String,
    inputPath: String,
    timestamp: String,
    inputPathIsFile: Boolean,
    groupedByDirectoryAndFormat: Seq[ScanGroup],
    directoriesWithPreScanErrors: Set[String],
    parallelism: Int,
    csvHeadCache: CsvHeadCache,
    schemaSigCache: SchemaSignatureCache,
    parseOkCache: ParseOkCache,
    rpcGate: Option[RpcGate] = None
  ): SplitAndFinalizeResult = {
    val schemaAwareGroups = ArrayBuffer.empty[ScanGroup]
    val errors = ArrayBuffer.empty[ScanError]
    val directoriesWithErrors = scala.collection.mutable.Set.empty[String] ++ directoriesWithPreScanErrors
    val schemaSplitParallelism = resolveParallelism(groupedByDirectoryAndFormat.size, parallelism)
    val fileSchemaSplitParallelism = math.max(1, parallelism)

    DriverLogger.debug(
      "scan_directory_schema_split_parallelism",
      "groups" -> groupedByDirectoryAndFormat.size,
      "parallelism" -> schemaSplitParallelism,
      "file_schema_parallelism" -> fileSchemaSplitParallelism,
      "driver_rpc_concurrency" -> rpcGate.map(_.permits).getOrElse("disabled")
    )
    val schemaSplitOutcomes = executeSchemaSplitTasks(schemaSplitParallelism, groupedByDirectoryAndFormat, rpcGate = None) { group =>
      val (splitGroups, splitErrors) = splitGroupBySchemaFast(
        spark,
        datasetPath,
        timestamp,
        group,
        csvHeadCache,
        schemaSigCache,
        parseOkCache,
        fileSchemaSplitParallelism,
        rpcGate
      )
      (group, splitGroups, splitErrors)
    }

    schemaSplitOutcomes.foreach {
      case (group, splitGroups, splitErrors) =>
        schemaAwareGroups ++= splitGroups
        errors ++= splitErrors
        DriverLogger.debug(
          "scan_directory_group_schema_split",
          "directory" -> group.directoryPath,
          "format" -> group.format,
          "input_files" -> group.filePaths.size,
          "split_groups" -> splitGroups.size,
          "split_errors" -> splitErrors.size
        )
        if (splitErrors.nonEmpty) {
          directoriesWithErrors += group.directoryPath
        }
    }

    val groupsPerDirectory = schemaAwareGroups.groupBy(_.directoryPath).map {
      case (directoryPath, groups) => directoryPath -> groups.size
    }

    val finalizedGroups = schemaAwareGroups.map { group =>
      val isInputRootGroup = comparableGroupingPath(group.directoryPath) == comparableGroupingPath(inputPath)
      val directoryIdentifierEligible =
        !inputPathIsFile &&
          group.allowDirectoryIdentifier &&
          groupsPerDirectory.getOrElse(group.directoryPath, 0) == 1 &&
          (group.filePaths.size > 1 || !isInputRootGroup) &&
          !directoriesWithErrors.contains(group.directoryPath)
      val finalizedGroup = group.copy(
        useDirectoryIdentifier = directoryIdentifierEligible && !group.schemaSampled,
        directoryIdentifierEligible = directoryIdentifierEligible
      )
      DriverLogger.debug(
        "scan_group_planned",
        "directory" -> finalizedGroup.directoryPath,
        "format" -> finalizedGroup.format,
        "schema" -> finalizedGroup.schemaSignature,
        "files" -> finalizedGroup.filePaths.size,
        "use_directory_identifier" -> finalizedGroup.useDirectoryIdentifier,
        "schema_sampled" -> finalizedGroup.schemaSampled,
        "csv_has_header" -> finalizedGroup.csvHasHeader
      )
      finalizedGroup
    }

    SplitAndFinalizeResult(
      groups = finalizedGroups.toSeq,
      errors = errors.toSeq,
      directoriesWithPreScanErrors = directoriesWithErrors.toSet
    )
  }

  private[discovery] def executeSchemaSplitTasks[A](
    parallelism: Int,
    groups: Seq[ScanGroup],
    rpcGate: Option[RpcGate]
  )(task: ScanGroup => A): Seq[A] = {
    executeInParallel(
      parallelism,
      groups.map(group => () => task(group)),
      gate = rpcGate
    )
  }

  private[discovery] def executeFileSchemaTasks[A](
    parallelism: Int,
    sourceKeys: Seq[String],
    rpcGate: Option[RpcGate]
  )(task: String => A): Seq[A] = {
    executeInParallel(
      parallelism,
      sourceKeys.map(sourceKey => () => task(sourceKey)),
      gate = rpcGate
    )
  }

  def splitGroupBySchemaFast(
    spark: SparkSession,
    datasetPath: String,
    timestamp: String,
    group: ScanGroup,
    csvHeadCache: CsvHeadCache = new CsvHeadCache(),
    schemaSigCache: SchemaSignatureCache = new SchemaSignatureCache(),
    parseOkCache: ParseOkCache = new ParseOkCache(),
    schemaSplitParallelism: Int = 1,
    rpcGate: Option[RpcGate] = None
  ): (Seq[ScanGroup], Seq[ScanError]) = {
    if (group.filePaths.size <= 1 || group.readOptionsByKey.nonEmpty) {
      splitGroupBySchema(spark, datasetPath, timestamp, group, csvHeadCache, schemaSigCache, schemaSplitParallelism, rpcGate)
    } else {
      DriverLogger.debug(
        "scan_group_schema_sample_start",
        "directory" -> group.directoryPath,
        "format" -> group.format,
        "files" -> group.filePaths.size
      )

      val sampledSourceKey = group.filePaths.head
      val sampledPhysicalPath = resolvePhysicalPath(group, sampledSourceKey)
      val sampledReadOptions = resolveReadOptions(group, sampledSourceKey)
      val sampledSchemaResult = withRpcGate(rpcGate) {
        if (group.format == "csv") {
          inferCsvSchemaSignature(spark, sampledPhysicalPath, csvHeadCache, schemaSigCache, sampledReadOptions)
        } else {
          inferSchemaSignature(spark, group.format, sampledPhysicalPath, sampledReadOptions, schemaSigCache)
            .map(signature => (signature, true))
        }
      }

      sampledSchemaResult match {
        case Right((schemaSignature, csvHasHeader)) =>
          val (validatedFilePaths, validationErrors) =
            if (group.format == "json") {
              validateSampledJsonFiles(
                spark,
                datasetPath,
                timestamp,
                group,
                parseOkCache,
                schemaSplitParallelism,
                rpcGate
              )
            } else {
              (group.filePaths, Seq.empty)
            }

          if (validatedFilePaths.isEmpty) {
            return (Seq.empty, validationErrors)
          }

          val sampledGroup = group.copy(
            schemaSignature = schemaSignature,
            filePaths = validatedFilePaths.sorted,
            schemaSampled = validatedFilePaths.size > 1,
            csvHasHeader = csvHasHeader
          )
          DriverLogger.debug(
            "scan_group_schema_sample_complete",
            "directory" -> group.directoryPath,
            "format" -> group.format,
            "schema" -> schemaSignature,
            "files" -> validatedFilePaths.size,
            "filtered_errors" -> validationErrors.size,
            "csv_has_header" -> csvHasHeader
          )
          (Seq(sampledGroup), validationErrors)
        case Left(errorMessage) =>
          DriverLogger.debug(
            "scan_group_schema_sample_fallback",
            "directory" -> group.directoryPath,
            "format" -> group.format,
            "files" -> group.filePaths.size,
            "reason" -> errorMessage
          )
          splitGroupBySchema(spark, datasetPath, timestamp, group, csvHeadCache, schemaSigCache, schemaSplitParallelism, rpcGate)
      }
    }
  }

  private final case class JsonValidationOutcome(sourceKey: String, error: Option[ScanError])

  private def validateSampledJsonFiles(
    spark: SparkSession,
    datasetPath: String,
    timestamp: String,
    group: ScanGroup,
    parseOkCache: ParseOkCache = new ParseOkCache(),
    schemaSplitParallelism: Int = 1,
    rpcGate: Option[RpcGate] = None
  ): (Seq[String], Seq[ScanError]) = {
    val validFilePaths = ArrayBuffer.empty[String]
    val errors = ArrayBuffer.empty[ScanError]
    val parallelism = resolveParallelism(group.filePaths.size, schemaSplitParallelism)

    val outcomes = executeFileSchemaTasks(parallelism, group.filePaths, rpcGate) { sourceKey =>
      val physicalPath = resolvePhysicalPath(group, sourceKey)
      val logicalIdentifier = resolveLogicalIdentifier(group, datasetPath, sourceKey)
      if (parseOkCache.isOk(physicalPath)) {
        JsonValidationOutcome(sourceKey, None)
      } else {
        try {
          withFileReadRetry(spark, Seq(physicalPath), "schema_detection") {
            readSchemaSource(spark, group.format, physicalPath, group.csvHasHeader)
            ()
          }
          parseOkCache.markOk(physicalPath)
          JsonValidationOutcome(sourceKey, None)
        } catch {
          case NonFatal(e) =>
            val errorMessage = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
            DriverLogger.debug(
              "group_schema_signature_failed",
              "directory" -> group.directoryPath,
              "file" -> physicalPath,
              "format" -> group.format,
              "reason" -> errorMessage
            )
            JsonValidationOutcome(
              sourceKey,
              Some(ScanError(
                datasetPath,
                timestamp,
                logicalIdentifier,
                s"Schema detection failed: $errorMessage"
              ))
            )
        }
      }
    }

    outcomes.foreach {
      case JsonValidationOutcome(sourceKey, None) =>
        validFilePaths += sourceKey
      case JsonValidationOutcome(_, Some(error)) =>
        errors += error
    }

    (validFilePaths.toSeq, errors.toSeq)
  }

  private final case class FileSchemaOutcome(
    sourceKey: String,
    schema: Option[(String, Boolean)],
    error: Option[ScanError]
  )

  def splitGroupBySchema(
    spark: SparkSession,
    datasetPath: String,
    timestamp: String,
    group: ScanGroup,
    csvHeadCache: CsvHeadCache = new CsvHeadCache(),
    schemaSigCache: SchemaSignatureCache = new SchemaSignatureCache(),
    schemaSplitParallelism: Int = 1,
    rpcGate: Option[RpcGate] = None
  ): (Seq[ScanGroup], Seq[ScanError]) = {
    val parallelism = resolveParallelism(group.filePaths.size, schemaSplitParallelism)
    DriverLogger.debug(
      "scan_group_schema_split_start",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "files" -> group.filePaths.size,
      "parallelism" -> parallelism,
      "driver_rpc_concurrency" -> rpcGate.map(_.permits).getOrElse("disabled")
    )
    val filesBySchema = scala.collection.mutable.Map.empty[(String, Boolean), ArrayBuffer[String]]
    val errors = ArrayBuffer.empty[ScanError]

    val outcomes = executeFileSchemaTasks(parallelism, group.filePaths, rpcGate) { sourceKey =>
      val physicalPath = resolvePhysicalPath(group, sourceKey)
      val readOptions = resolveReadOptions(group, sourceKey)
      val schemaResult = if (group.format == "csv") {
        inferCsvSchemaSignature(spark, physicalPath, csvHeadCache, schemaSigCache, readOptions)
      } else {
        inferSchemaSignature(spark, group.format, physicalPath, readOptions, schemaSigCache)
          .map(signature => (signature, true))
      }

      schemaResult match {
        case Right((schemaSignature, csvHasHeader)) =>
          DriverLogger.debug(
            "group_schema_signature_detected",
            "directory" -> group.directoryPath,
            "file" -> physicalPath,
            "format" -> group.format,
            "schema" -> schemaSignature,
            "csv_has_header" -> csvHasHeader
          )
          FileSchemaOutcome(sourceKey, Some((schemaSignature, csvHasHeader)), None)
        case Left(errorMessage) =>
          if (isEmptyWorkbookSheetSchemaError(group.format, errorMessage)) {
            DriverLogger.debug(
              "group_schema_signature_empty_xlsx_sheet",
              "directory" -> group.directoryPath,
              "file" -> physicalPath,
              "format" -> group.format,
              "file_identifier" -> resolveLogicalIdentifier(group, datasetPath, sourceKey)
            )
            FileSchemaOutcome(sourceKey, None, None)
          } else {
            DriverLogger.debug(
              "group_schema_signature_failed",
              "directory" -> group.directoryPath,
              "file" -> physicalPath,
              "format" -> group.format,
              "reason" -> errorMessage
            )
            FileSchemaOutcome(
              sourceKey,
              None,
              Some(ScanError(
                datasetPath,
                timestamp,
                resolveLogicalIdentifier(group, datasetPath, sourceKey),
                s"Schema detection failed: $errorMessage"
              ))
            )
          }
      }
    }

    outcomes.foreach {
      case FileSchemaOutcome(sourceKey, Some((schemaSignature, csvHasHeader)), None) =>
        val groupedFiles = filesBySchema.getOrElseUpdate((schemaSignature, csvHasHeader), ArrayBuffer.empty[String])
        groupedFiles += sourceKey
      case FileSchemaOutcome(_, None, Some(error)) =>
        errors += error
      case FileSchemaOutcome(_, None, None) =>
      case FileSchemaOutcome(_, Some(_), Some(error)) =>
        errors += error
    }

    val groups = filesBySchema.toSeq
      .sortBy { case ((schemaSignature, csvHasHeader), _) => (schemaSignature, csvHasHeader) }
      .map {
        case ((schemaSignature, csvHasHeader), groupedFiles) =>
          group.copy(
            schemaSignature = schemaSignature,
            filePaths = groupedFiles.toSeq.sorted,
            schemaSampled = false,
            csvHasHeader = csvHasHeader
          )
      }

    DriverLogger.debug(
      "scan_group_schema_split_complete",
      "directory" -> group.directoryPath,
      "format" -> group.format,
      "schema_groups" -> groups.size,
      "errors" -> errors.size
    )
    (groups, errors.toSeq)
  }

  private def isEmptyWorkbookSheetSchemaError(format: String, errorMessage: String): Boolean =
    format == XlsxFormat && errorMessage == "head of empty list"

  private def withRpcGate[A](rpcGate: Option[RpcGate])(block: => A): A =
    rpcGate match {
      case Some(gate) => gate.withPermit(block)
      case None => block
    }
}
