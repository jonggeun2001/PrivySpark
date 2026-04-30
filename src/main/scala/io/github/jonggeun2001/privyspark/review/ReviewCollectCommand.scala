package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.cli.ReviewCollectCliConfig
import io.github.jonggeun2001.privyspark.report.JsonCodec.{extractJsonLongField, extractJsonObjectArrayField, extractJsonStringField, jsonString}
import io.github.jonggeun2001.privyspark.util.DriverLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.time.{Instant, LocalDate}
import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

private[privyspark] object ReviewCollectCommand {
  private final case class ResponseEnvelope(
    sourcePath: String,
    scanPath: String,
    responder: String,
    respondedAt: String,
    responses: Seq[ResponseItem]
  )

  private final case class ResponseItem(
    findingKey: String,
    findingHash: String,
    fileIdentifier: String,
    fileIdentifierPattern: String,
    hiveDatabase: String,
    hiveTable: String,
    hiveTableFqn: String,
    columnName: String,
    piiType: String,
    sampleRowCount: Long,
    matchCount: Long,
    nonEmptyMatchRatio: Double,
    decision: String,
    falsePositiveReason: String,
    allowlistScope: String,
    expiresAt: String,
    actionPlan: String,
    actionDueDate: String
  )

  private final case class AcceptedResponse(
    scanPath: String,
    item: ResponseItem,
    responder: String,
    respondedAt: String,
    sourcePath: String
  )

  private final case class RejectedResponse(sourcePath: String, reason: String)

  private final case class ActionPlan(
    findingKey: String,
    scanPath: String,
    fileIdentifier: String,
    hiveDatabase: String,
    hiveTable: String,
    hiveTableFqn: String,
    columnName: String,
    piiType: String,
    actionPlan: String,
    actionDueDate: String,
    responder: String,
    respondedAt: String,
    status: String
  )

  def run(spark: SparkSession, config: ReviewCollectCliConfig): Unit = {
    val conf = spark.sparkContext.hadoopConfiguration
    val root = config.reviewStateRoot.stripSuffix("/")
    val inboxPath = s"$root/inbox"
    val currentPath = s"$root/current"

    val envelopes = readResponseEnvelopes(conf, inboxPath)
    val rejected = ArrayBuffer.empty[RejectedResponse]
    val accepted = ArrayBuffer.empty[AcceptedResponse]

    envelopes.foreach {
      case Left(rejection) =>
        rejected += rejection
      case Right(envelope) =>
        validateEnvelope(envelope) match {
          case Some(reason) =>
            rejected += RejectedResponse(envelope.sourcePath, reason)
          case None =>
            envelope.responses.foreach { item =>
              validateItem(item) match {
                case Some(reason) =>
                  rejected += RejectedResponse(envelope.sourcePath, reason)
                case None =>
                  accepted += AcceptedResponse(envelope.scanPath, item, envelope.responder, envelope.respondedAt, envelope.sourcePath)
              }
            }
        }
    }

    val latestAccepted = accepted
      .groupBy(_.item.findingKey)
      .values
      .map(_.maxBy(response => instantOrderingKey(response.respondedAt)))
      .toSeq

    val latestFindingKeys = latestAccepted.map(_.item.findingKey).toSet
    val existingAllowlistPath = s"$currentPath/allowlist.jsonl"
    val existingAllowlistReadablePath = resolveReadableStateFilePath(conf, existingAllowlistPath)
    val existingRecurring = existingAllowlistReadablePath.map(AllowlistMatcher.loadRecurringEntries(conf, _)).getOrElse(Seq.empty)
    val retainedRecurring = existingRecurring.filterNot(entry =>
      latestFindingKeys.contains(entry.sourceFindingKey) ||
        latestAccepted.exists(responseCoversRecurring(entry, _))
    )
    val recurringEntries = (retainedRecurring ++ latestAccepted
      .filter(_.item.decision == ReviewStatus.FalsePositive)
      .map(toRecurringAllowlistEntry)
    ).groupBy(_.key)
      .map(_._2.maxBy(_.reviewedAt))
      .toSeq
      .sortBy(entry => (entry.scanPath, entry.hiveTableFqn, entry.fileIdentifierPattern, entry.columnName, entry.piiType))

    val existingActionPlans = loadActionPlans(conf, s"$currentPath/action_plan.jsonl")
    val retainedActionPlans = existingActionPlans.filterNot(plan =>
      latestFindingKeys.contains(plan.findingKey) ||
        latestAccepted.exists(responseCoversActionPlan(plan, _))
    )
    val actionPlans = (retainedActionPlans ++ latestAccepted
      .filter(_.item.decision == ReviewStatus.TruePositive)
      .map(toActionPlan)
    ).groupBy(_.findingKey)
      .map(_._2.maxBy(_.respondedAt))
      .toSeq
      .sortBy(plan => (plan.scanPath, plan.hiveTableFqn, plan.fileIdentifier, plan.columnName, plan.piiType))

    val collectRunId = s"${Instant.now().toString.replace(':', '-')}-${UUID.randomUUID().toString.take(8)}"
    val tempCurrentPath = s"$root/current.tmp-$collectRunId"
    writeState(conf, tempCurrentPath, recurringEntries, actionPlans, latestAccepted)
    replacePath(conf, tempCurrentPath, currentPath)

    DriverLogger.info(
      "review_collect_complete",
      "scan_results" -> (config.scanResultsPath.trim match {
        case "" => "not_used"
        case value => value
      }),
      "review_state_root" -> config.reviewStateRoot,
      "accepted" -> accepted.size,
      "rejected" -> rejected.size
    )
  }

  private def validateEnvelope(envelope: ResponseEnvelope): Option[String] = {
    if (ReviewPathNormalizer.normalizeScanPath(envelope.scanPath).trim.isEmpty) {
      Some("scan_path is required")
    } else if (envelope.responder.trim.isEmpty) {
      Some("responder is required")
    } else if (Try(Instant.parse(envelope.respondedAt)).isFailure) {
      Some("responded_at must be an ISO-8601 instant")
    } else if (envelope.responses.isEmpty) {
      Some("responses must not be empty")
    } else {
      None
    }
  }

  private def validateItem(item: ResponseItem): Option[String] = {
    if (item.findingKey.trim.isEmpty) {
      Some("finding_key is required")
    } else if (item.columnName.trim.isEmpty) {
      Some(s"column_name is required: ${item.findingKey}")
    } else if (item.piiType.trim.isEmpty) {
      Some(s"pii_type is required: ${item.findingKey}")
    } else {
      item.decision match {
        case ReviewStatus.FalsePositive =>
          validateFalsePositive(item)
        case ReviewStatus.TruePositive =>
          validateTruePositive(item)
        case other =>
          Some(s"unsupported decision: $other")
      }
    }
  }

  private def validateFalsePositive(item: ResponseItem): Option[String] = {
    val scope = item.allowlistScope.trim
    val filePattern = recurringFileIdentifierPattern(item)
    if (scope.nonEmpty && scope != "recurring") {
      Some(s"unsupported allowlist_scope: $scope")
    } else if (item.falsePositiveReason.trim.isEmpty) {
      Some(s"false_positive_reason is required: ${item.findingKey}")
    } else if (item.expiresAt.trim.isEmpty) {
      Some(s"expires_at is required for recurring allowlist: ${item.findingKey}")
    } else if (Try(LocalDate.parse(item.expiresAt)).isFailure) {
      Some(s"expires_at must use YYYY-MM-DD: ${item.findingKey}")
    } else if (item.hiveTableFqn.trim.isEmpty && filePattern.trim.isEmpty) {
      Some(s"file_identifier_pattern is required when hive_table_fqn is empty: ${item.findingKey}")
    } else {
      None
    }
  }

  private def validateTruePositive(item: ResponseItem): Option[String] = {
    if (item.actionPlan.trim.isEmpty || item.actionDueDate.trim.isEmpty) {
      Some(s"action_plan and action_due_date are required: ${item.findingKey}")
    } else if (Try(LocalDate.parse(item.actionDueDate)).isFailure) {
      Some(s"action_due_date must use YYYY-MM-DD: ${item.findingKey}")
    } else {
      None
    }
  }

  private def instantOrderingKey(value: String): (Long, Int) = {
    val instant = Instant.parse(value)
    instant.getEpochSecond -> instant.getNano
  }

  private def toRecurringAllowlistEntry(response: AcceptedResponse): RecurringAllowlistEntry =
    RecurringAllowlistEntry(
      scanPath = response.scanPath,
      hiveTableFqn = response.item.hiveTableFqn,
      fileIdentifierPattern =
        if (response.item.hiveTableFqn.trim.nonEmpty) "" else recurringFileIdentifierPattern(response.item),
      columnName = response.item.columnName,
      piiType = response.item.piiType,
      reason = response.item.falsePositiveReason,
      reviewer = response.responder,
      reviewedAt = response.respondedAt,
      expiresAt = response.item.expiresAt,
      sourceFindingKey = response.item.findingKey,
      sampleRowCount = response.item.sampleRowCount,
      matchCount = response.item.matchCount,
      nonEmptyMatchRatio = response.item.nonEmptyMatchRatio
    )

  private def responseCoversRecurring(entry: RecurringAllowlistEntry, response: AcceptedResponse): Boolean = {
    val sameScanAndType =
      ReviewPathNormalizer.normalizeScanPath(entry.scanPath) == ReviewPathNormalizer.normalizeScanPath(response.scanPath) &&
        entry.columnName == response.item.columnName &&
        entry.piiType == response.item.piiType
    if (!sameScanAndType) {
      false
    } else if (entry.hiveTableFqn.trim.nonEmpty || response.item.hiveTableFqn.trim.nonEmpty) {
      entry.hiveTableFqn == response.item.hiveTableFqn
    } else {
      val responsePattern = recurringFileIdentifierPattern(response.item)
      entry.fileIdentifierPattern == responsePattern ||
        wildcardMatches(entry.fileIdentifierPattern, response.item.fileIdentifier) ||
        wildcardMatches(responsePattern, entry.fileIdentifierPattern) ||
        wildcardCoversRepresentative(entry.fileIdentifierPattern, responsePattern)
    }
  }

  private def responseCoversActionPlan(plan: ActionPlan, response: AcceptedResponse): Boolean = {
    val sameScanAndType =
      ReviewPathNormalizer.normalizeScanPath(plan.scanPath) == ReviewPathNormalizer.normalizeScanPath(response.scanPath) &&
        plan.columnName == response.item.columnName &&
        plan.piiType == response.item.piiType
    if (!sameScanAndType) {
      false
    } else if (plan.hiveTableFqn.trim.nonEmpty || response.item.hiveTableFqn.trim.nonEmpty) {
      plan.hiveTableFqn == response.item.hiveTableFqn
    } else {
      plan.fileIdentifier == response.item.fileIdentifier
    }
  }

  private def recurringFileIdentifierPattern(item: ResponseItem): String =
    Option(item.fileIdentifierPattern).map(_.trim).filter(_.nonEmpty)
      .getOrElse(Option(item.fileIdentifier).map(_.trim).getOrElse(""))

  private def wildcardMatches(pattern: String, value: String): Boolean = {
    val normalizedPattern = Option(pattern).map(_.trim).filter(_.nonEmpty).getOrElse("")
    val normalizedValue = Option(value).getOrElse("")
    if (normalizedPattern.isEmpty) {
      false
    } else {
      val regex = normalizedPattern.flatMap {
        case '*' => ".*"
        case ch if "\\.[]{}()+-^$?|".contains(ch) => "\\" + ch
        case ch => ch.toString
      }
      normalizedValue.matches(regex)
    }
  }

  private def wildcardCoversRepresentative(pattern: String, representative: String): Boolean = {
    val normalizedPattern = Option(pattern).map(_.trim.stripSuffix("/")).getOrElse("")
    val normalizedRepresentative = Option(representative).map(_.trim.stripSuffix("/")).getOrElse("")
    normalizedPattern.endsWith("/*") &&
      normalizedPattern.stripSuffix("/*") == normalizedRepresentative
  }

  private def toActionPlan(response: AcceptedResponse): ActionPlan = {
    val (fallbackDatabase, fallbackTable) = splitHiveTableFqn(response.item.hiveTableFqn)
    ActionPlan(
      findingKey = response.item.findingKey,
      scanPath = response.scanPath,
      fileIdentifier = response.item.fileIdentifier,
      hiveDatabase = emptyTo(response.item.hiveDatabase, fallbackDatabase),
      hiveTable = emptyTo(response.item.hiveTable, fallbackTable),
      hiveTableFqn = response.item.hiveTableFqn,
      columnName = response.item.columnName,
      piiType = response.item.piiType,
      actionPlan = response.item.actionPlan,
      actionDueDate = response.item.actionDueDate,
      responder = response.responder,
      respondedAt = response.respondedAt,
      status = RemediationPlanned
    )
  }

  private def emptyTo(value: String, fallback: String): String =
    Option(value).map(_.trim).filter(_.nonEmpty).getOrElse(fallback)

  private def readResponseEnvelopes(conf: Configuration, inboxPath: String): Seq[Either[RejectedResponse, ResponseEnvelope]] = {
    val path = new Path(inboxPath)
    val fs = path.getFileSystem(conf)
    if (!fs.exists(path)) {
      Seq.empty
    } else {
      fs.listStatus(path)
        .filter(status => status.isFile && status.getPath.getName.endsWith(".json"))
        .toSeq
        .sortBy(_.getPath.toString)
        .map { status =>
          val sourcePath = status.getPath.toString
          parseEnvelope(sourcePath, readText(conf, sourcePath)).left.map(reason => RejectedResponse(sourcePath, reason))
        }
    }
  }

  private def pathExists(conf: Configuration, path: String): Boolean = {
    val hadoopPath = new Path(path)
    hadoopPath.getFileSystem(conf).exists(hadoopPath)
  }

  private def resolveReadableStateFilePath(conf: Configuration, path: String): Option[String] =
    Seq(path, s"$path.bak").find(pathExists(conf, _))

  private def parseEnvelope(sourcePath: String, json: String): Either[String, ResponseEnvelope] = {
    val schemaVersion = extractJsonStringField(json, "schema_version").orElse(extractNumericField(json, "schema_version")).getOrElse("")
    if (schemaVersion != "1") {
      Left(s"unsupported schema_version: $schemaVersion")
    } else {
      val responseObjects = extractJsonObjectArrayField(json, "responses").getOrElse(Seq.empty)
      Right(ResponseEnvelope(
        sourcePath = sourcePath,
        scanPath = extractJsonStringField(json, "scan_path").getOrElse(""),
        responder = extractJsonStringField(json, "responder").getOrElse(""),
        respondedAt = extractJsonStringField(json, "responded_at").getOrElse(""),
        responses = responseObjects.map(parseItem)
      ))
    }
  }

  private def parseItem(json: String): ResponseItem = {
    val hiveTableFqn = extractJsonStringField(json, "hive_table_fqn").getOrElse("")
    val (fallbackDatabase, fallbackTable) = splitHiveTableFqn(hiveTableFqn)
    ResponseItem(
      findingKey = extractJsonStringField(json, "finding_key").getOrElse(""),
      findingHash = extractJsonStringField(json, "finding_hash").getOrElse(""),
      fileIdentifier = extractJsonStringField(json, "file_identifier").getOrElse(""),
      fileIdentifierPattern = extractJsonStringField(json, "file_identifier_pattern").getOrElse(""),
      hiveDatabase = extractJsonStringField(json, "hive_database").getOrElse(fallbackDatabase),
      hiveTable = extractJsonStringField(json, "hive_table").getOrElse(fallbackTable),
      hiveTableFqn = hiveTableFqn,
      columnName = extractJsonStringField(json, "column_name").getOrElse(""),
      piiType = extractJsonStringField(json, "pii_type").getOrElse(""),
      sampleRowCount = extractJsonLongField(json, "sample_row_count")
        .orElse(extractJsonLongField(json, "sampled_row_count"))
        .getOrElse(0L),
      matchCount = extractJsonLongField(json, "match_count").getOrElse(0L),
      nonEmptyMatchRatio = extractJsonDoubleField(json, "non_empty_match_ratio").getOrElse(0.0),
      decision = extractJsonStringField(json, "decision").getOrElse(""),
      falsePositiveReason = extractJsonStringField(json, "false_positive_reason").getOrElse(""),
      allowlistScope = extractJsonStringField(json, "allowlist_scope").getOrElse(""),
      expiresAt = extractJsonStringField(json, "expires_at").getOrElse(""),
      actionPlan = extractJsonStringField(json, "action_plan").getOrElse(""),
      actionDueDate = extractJsonStringField(json, "action_due_date").getOrElse("")
    )
  }

  private def loadActionPlans(conf: Configuration, path: String): Seq[ActionPlan] = {
    resolveReadableStateFilePath(conf, path) match {
      case None =>
        Seq.empty
      case Some(readablePath) =>
        readLines(conf, readablePath).map { line =>
          val hiveTableFqn = extractJsonStringField(line, "hive_table_fqn").getOrElse("")
          val (fallbackDatabase, fallbackTable) = splitHiveTableFqn(hiveTableFqn)
          ActionPlan(
            findingKey = extractJsonStringField(line, "finding_key").getOrElse(""),
            scanPath = extractJsonStringField(line, "scan_path").getOrElse(""),
            fileIdentifier = extractJsonStringField(line, "file_identifier").getOrElse(""),
            hiveDatabase = extractJsonStringField(line, "hive_database").getOrElse(fallbackDatabase),
            hiveTable = extractJsonStringField(line, "hive_table").getOrElse(fallbackTable),
            hiveTableFqn = hiveTableFqn,
            columnName = extractJsonStringField(line, "column_name").getOrElse(""),
            piiType = extractJsonStringField(line, "pii_type").getOrElse(""),
            actionPlan = extractJsonStringField(line, "action_plan").getOrElse(""),
            actionDueDate = extractJsonStringField(line, "action_due_date").getOrElse(""),
            responder = extractJsonStringField(line, "responder").getOrElse(""),
            respondedAt = extractJsonStringField(line, "responded_at").getOrElse(""),
            status = extractJsonStringField(line, "status").getOrElse(RemediationPlanned)
          )
        }
    }
  }

  private def extractNumericField(json: String, field: String): Option[String] = {
    val pattern = ("\"" + field + "\"\\s*:\\s*([0-9]+)").r
    pattern.findFirstMatchIn(json).map(_.group(1))
  }

  private def extractJsonDoubleField(json: String, field: String): Option[Double] = {
    val pattern = ("\"" + field + "\"\\s*:\\s*(-?[0-9]+(?:\\.[0-9]+)?)").r
    pattern.findFirstMatchIn(json).flatMap(matchResult => Try(matchResult.group(1).toDouble).toOption)
  }

  private def writeState(
    conf: Configuration,
    statePath: String,
    recurringEntries: Seq[RecurringAllowlistEntry],
    actionPlans: Seq[ActionPlan],
    accepted: Seq[AcceptedResponse]
  ): Unit = {
    writeLines(conf, s"$statePath/allowlist.jsonl", recurringEntries.map(AllowlistJson.recurringEntryToJson))
    writeLines(conf, s"$statePath/action_plan.jsonl", actionPlans.map(actionPlanToJson))
    writeLines(conf, s"$statePath/finding_status.jsonl", findingStatusLines(accepted, actionPlans))
    writeLines(conf, s"$statePath/response_ledger.jsonl", accepted.map(acceptedToJson))
  }

  private def replacePath(conf: Configuration, sourcePath: String, destinationPath: String): Unit = {
    val source = new Path(sourcePath)
    val destination = new Path(destinationPath)
    val fs = destination.getFileSystem(conf)
    if (!fs.exists(source)) {
      throw new IllegalStateException(s"Replacement source does not exist: $sourcePath")
    }
    val parent = destination.getParent
    if (parent != null) {
      fs.mkdirs(parent)
    }

    if (!fs.exists(destination)) {
      if (!fs.rename(source, destination)) {
        throw new IllegalStateException(s"Failed to replace review state: $destinationPath")
      }
      return
    }

    fs.mkdirs(destination)
    fs.listStatus(source)
      .filter(_.isFile)
      .sortBy(_.getPath.getName)
      .foreach(status => replaceFile(fs, status.getPath, new Path(destination, status.getPath.getName)))
    fs.delete(source, true)
  }

  private def replaceFile(fs: org.apache.hadoop.fs.FileSystem, source: Path, destination: Path): Unit = {
    val backup = new Path(s"${destination.toString}.bak")
    val primaryExists = fs.exists(destination)
    if (primaryExists) {
      if (fs.exists(backup) && !fs.delete(backup, false)) {
        throw new IllegalStateException(s"Stale backup cleanup failed: ${backup.toString}")
      }
      if (!fs.rename(destination, backup)) {
        throw new IllegalStateException(s"Existing file backup failed: ${destination.toString}")
      }
    }
    if (fs.rename(source, destination)) {
      if (fs.exists(backup) && !fs.delete(backup, false)) {
        DriverLogger.warn("review_state_backup_cleanup_failed", "backup" -> backup.toString)
      }
    } else {
      if (primaryExists && !fs.exists(destination) && fs.exists(backup) && !fs.rename(backup, destination)) {
        throw new IllegalStateException(s"Review state restore failed: ${destination.toString}")
      }
      throw new IllegalStateException(s"Failed to replace review state file: ${destination.toString}")
    }
  }

  private def writeLines(conf: Configuration, path: String, lines: Seq[String]): Unit = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(conf)
    val parent = hadoopPath.getParent
    if (parent != null) {
      fs.mkdirs(parent)
    }
    val writer = new BufferedWriter(new OutputStreamWriter(fs.create(hadoopPath, true), StandardCharsets.UTF_8))
    try {
      lines.foreach { line =>
        writer.write(line)
        writer.newLine()
      }
    } finally {
      writer.close()
    }
  }

  private def readText(conf: Configuration, path: String): String = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(conf)
    val reader = new BufferedReader(new InputStreamReader(fs.open(hadoopPath), StandardCharsets.UTF_8))
    val builder = new StringBuilder
    try {
      var line = reader.readLine()
      while (line != null) {
        builder.append(line).append('\n')
        line = reader.readLine()
      }
    } finally {
      reader.close()
    }
    builder.toString()
  }

  private def readLines(conf: Configuration, path: String): Seq[String] =
    readText(conf, path).split("\\r?\\n").toSeq.map(_.trim).filter(_.nonEmpty)

  private def actionPlanToJson(plan: ActionPlan): String =
    s"""{"finding_key":${jsonString(plan.findingKey)},"scan_path":${jsonString(plan.scanPath)},"file_identifier":${jsonString(plan.fileIdentifier)},"hive_database":${jsonString(plan.hiveDatabase)},"hive_table":${jsonString(plan.hiveTable)},"hive_table_fqn":${jsonString(plan.hiveTableFqn)},"column_name":${jsonString(plan.columnName)},"pii_type":${jsonString(plan.piiType)},"action_plan":${jsonString(plan.actionPlan)},"action_due_date":${jsonString(plan.actionDueDate)},"responder":${jsonString(plan.responder)},"responded_at":${jsonString(plan.respondedAt)},"status":${jsonString(plan.status)}}"""

  private def acceptedToJson(response: AcceptedResponse): String =
    s"""{"source_path":${jsonString(response.sourcePath)},"scan_path":${jsonString(response.scanPath)},"file_identifier":${jsonString(response.item.fileIdentifier)},"hive_table_fqn":${jsonString(response.item.hiveTableFqn)},"finding_key":${jsonString(response.item.findingKey)},"finding_hash":${jsonString(response.item.findingHash)},"column_name":${jsonString(response.item.columnName)},"pii_type":${jsonString(response.item.piiType)},"decision":${jsonString(response.item.decision)},"responder":${jsonString(response.responder)},"responded_at":${jsonString(response.respondedAt)}}"""

  private def findingStatusLines(
    accepted: Seq[AcceptedResponse],
    actionPlans: Seq[ActionPlan]
  ): Seq[String] = {
    val latestFindingKeys = accepted.map(_.item.findingKey).toSet
    accepted.map(acceptedStatusToJson) ++
      actionPlans
        .filterNot(plan => latestFindingKeys.contains(plan.findingKey))
        .map(plan => actionPlanStatusToJson(plan, currentActionStatus(plan)))
  }

  private def acceptedStatusToJson(response: AcceptedResponse): String = {
    val status =
      if (response.item.decision == ReviewStatus.FalsePositive) ReviewStatus.FalsePositive
      else RemediationPlanned
    s"""{"finding_key":${jsonString(response.item.findingKey)},"scan_path":${jsonString(response.scanPath)},"file_identifier":${jsonString(response.item.fileIdentifier)},"hive_table_fqn":${jsonString(response.item.hiveTableFqn)},"column_name":${jsonString(response.item.columnName)},"pii_type":${jsonString(response.item.piiType)},"status":${jsonString(status)}}"""
  }

  private def actionPlanStatusToJson(plan: ActionPlan, status: String): String =
    s"""{"finding_key":${jsonString(plan.findingKey)},"scan_path":${jsonString(plan.scanPath)},"file_identifier":${jsonString(plan.fileIdentifier)},"hive_table_fqn":${jsonString(plan.hiveTableFqn)},"column_name":${jsonString(plan.columnName)},"pii_type":${jsonString(plan.piiType)},"status":${jsonString(status)}}"""

  private def currentActionStatus(plan: ActionPlan): String = {
    if (plan.status == Verified) {
      Verified
    } else if (Try(LocalDate.parse(plan.actionDueDate)).toOption.exists(_.isBefore(LocalDate.now()))) {
      Overdue
    } else {
      RemediationPlanned
    }
  }

  private def splitHiveTableFqn(hiveTableFqn: String): (String, String) = {
    val normalized = Option(hiveTableFqn).map(_.trim).getOrElse("")
    val delimiterIndex = normalized.lastIndexOf('.')
    if (delimiterIndex > 0 && delimiterIndex < normalized.length - 1) {
      normalized.substring(0, delimiterIndex) -> normalized.substring(delimiterIndex + 1)
    } else {
      "" -> normalized
    }
  }

  private val RemediationPlanned = "remediation_planned"
  private val Overdue = "overdue"
  private val Verified = "verified"
}
