package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.cli.ReviewCollectCliConfig
import io.github.jonggeun2001.privyspark.report.JsonCodec.{extractJsonObjectArrayField, extractJsonStringField, jsonString}
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
    scanResultsFingerprint: String,
    responder: String,
    respondedAt: String,
    responses: Seq[ResponseItem]
  )

  private final case class ResponseItem(
    findingKey: String,
    findingHash: String,
    decision: String,
    falsePositiveReason: String,
    allowlistScope: String,
    fileIdentifierPattern: String,
    columnNamePattern: String,
    piiTypePattern: String,
    expiresAt: String,
    actionPlan: String,
    actionDueDate: String
  )

  private final case class AcceptedResponse(
    finding: ReviewFinding,
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
    val findings = ReviewFindingBuilder.fromScanResultsIterator(
      ScanResultsReader.iterateScanResults(ScanResultsReader.read(spark, config.scanResultsPath), ordered = true),
      ReviewFindingBuilder.DefaultMaxEvidenceSamples
    )
    val findingsByKey = findings.map(finding => finding.findingKey -> finding).toMap
    val expectedScanFingerprint = ReviewFindingBuilder.scanResultsFingerprint(findings)
    val expectedScanPaths = findings.map(_.scanPath).distinct.toSet
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
        if (!expectedScanPaths.contains(envelope.scanPath)) {
          rejected += RejectedResponse(envelope.sourcePath, s"scan_path mismatch: ${envelope.scanPath}")
        } else if (envelope.scanResultsFingerprint != expectedScanFingerprint) {
          rejected += RejectedResponse(envelope.sourcePath, "scan_results_fingerprint mismatch")
        } else if (envelope.responder.trim.isEmpty) {
          rejected += RejectedResponse(envelope.sourcePath, "responder is required")
        } else if (Try(Instant.parse(envelope.respondedAt)).isFailure) {
          rejected += RejectedResponse(envelope.sourcePath, "responded_at must be an ISO-8601 instant")
        } else if (envelope.responses.isEmpty) {
          rejected += RejectedResponse(envelope.sourcePath, "responses must not be empty")
        } else {
          envelope.responses.foreach { item =>
            validateItem(item, findingsByKey) match {
              case Some(reason) =>
                rejected += RejectedResponse(envelope.sourcePath, reason)
              case None =>
                accepted += AcceptedResponse(findingsByKey(item.findingKey), item, envelope.responder, envelope.respondedAt, envelope.sourcePath)
            }
          }
        }
    }

    val latestAccepted = accepted
      .groupBy(_.finding.findingKey)
      .values
      .map(_.maxBy(response => instantOrderingKey(response.respondedAt)))
      .toSeq

    val existingAllowlistPath = s"$currentPath/allowlist.jsonl"
    val existingAllowlistReadablePath = resolveReadableStateFilePath(conf, existingAllowlistPath)
    val existingExact = existingAllowlistReadablePath.map(AllowlistMatcher.loadEntries(conf, _)).getOrElse(Seq.empty)
    val existingPatterns = existingAllowlistReadablePath.map(AllowlistMatcher.loadPatternEntries(conf, _)).getOrElse(Seq.empty)
    val latestFindingKeys = latestAccepted.map(_.finding.findingKey).toSet
    val exactScope = collectExactScope(spark, config, latestAccepted)
    val affectedExactKeys = exactScope.affectedKeys
    val retainedExact = existingExact.filterNot(entry =>
      latestFindingKeys.contains(entry.sourceRunId) || affectedExactKeys.contains(entry.key)
    )
    val reviewedFindings = latestAccepted.map(_.finding)
    val retainedPatterns = existingPatterns.filterNot(entry =>
      latestFindingKeys.contains(entry.sourceFindingKey) ||
        reviewedFindings.exists(patternCoversFinding(entry, _))
    )
    val exactEntries = (retainedExact ++ exactScope.entries).groupBy(_.key).map(_._2.last).toSeq
      .sortBy(entry => (entry.datasetPath, entry.fileIdentifier, entry.columnName, entry.piiType))
    val patternEntries = (retainedPatterns ++ latestAccepted.filter(_.item.decision == ReviewStatus.FalsePositive)
      .filter(_.item.allowlistScope == "pattern")
      .map(toPatternAllowlistEntry)).groupBy(_.key).map(_._2.last).toSeq
      .sortBy(entry => (entry.datasetPath, entry.fileIdentifierPattern, entry.columnNamePattern, entry.piiTypePattern))
    val existingActionPlans = loadActionPlans(conf, s"$currentPath/action_plan.jsonl")
    val retainedActionPlans = existingActionPlans.filterNot(plan =>
      latestFindingKeys.contains(plan.findingKey) ||
        reviewedFindings.exists(actionPlanCoversFinding(plan, _))
    )
    val actionPlans = (retainedActionPlans ++ latestAccepted.filter(_.item.decision == ReviewStatus.TruePositive)
      .map(toActionPlan)
    ).groupBy(_.findingKey)
      .map(_._2.maxBy(_.respondedAt))
      .toSeq
      .sortBy(plan => (plan.scanPath, plan.fileIdentifier, plan.columnName, plan.piiType))

    val collectRunId = s"${Instant.now().toString.replace(':', '-')}-${UUID.randomUUID().toString.take(8)}"
    val tempCurrentPath = s"$root/current.tmp-$collectRunId"
    writeState(conf, tempCurrentPath, findings, exactEntries, patternEntries, actionPlans, latestAccepted)
    replacePath(conf, tempCurrentPath, currentPath)

    DriverLogger.info(
      "review_collect_complete",
      "scan_results" -> config.scanResultsPath,
      "review_state_root" -> config.reviewStateRoot,
      "findings" -> findings.size,
      "accepted" -> accepted.size,
      "rejected" -> rejected.size
    )
  }

  private def validateItem(item: ResponseItem, findingsByKey: Map[String, ReviewFinding]): Option[String] = {
    findingsByKey.get(item.findingKey) match {
      case None =>
        Some(s"unknown finding_key: ${item.findingKey}")
      case Some(finding) if finding.findingHash != item.findingHash =>
        Some(s"finding_hash mismatch: ${item.findingKey}")
      case Some(finding) =>
        item.decision match {
          case ReviewStatus.FalsePositive =>
            if (item.falsePositiveReason.trim.isEmpty) {
              Some(s"false_positive_reason is required: ${item.findingKey}")
            } else if (item.allowlistScope == "pattern") {
              validatePatternItem(item, finding)
            } else if (item.allowlistScope == "exact") {
              if (!finding.fingerprintComplete) {
                Some(s"exact allowlist requires fingerprint metadata: ${item.findingKey}")
              } else {
                None
              }
            } else {
              Some(s"unsupported allowlist_scope: ${item.allowlistScope}")
            }
          case ReviewStatus.TruePositive =>
            if (item.actionPlan.trim.isEmpty || item.actionDueDate.trim.isEmpty) {
              Some(s"action_plan and action_due_date are required: ${item.findingKey}")
            } else if (Try(LocalDate.parse(item.actionDueDate)).isFailure) {
              Some(s"action_due_date must use YYYY-MM-DD: ${item.findingKey}")
            } else {
              None
            }
          case other =>
            Some(s"unsupported decision: $other")
        }
    }
  }

  private def validatePatternItem(item: ResponseItem, finding: ReviewFinding): Option[String] = {
    if (item.expiresAt.trim.isEmpty) {
      Some(s"expires_at is required for pattern allowlist: ${item.findingKey}")
    } else if (Try(LocalDate.parse(item.expiresAt)).isFailure) {
      Some(s"expires_at must use YYYY-MM-DD: ${item.findingKey}")
    } else if (Seq(item.fileIdentifierPattern, item.columnNamePattern, item.piiTypePattern).forall(_.trim.isEmpty)) {
      Some(s"at least one pattern field is required: ${item.findingKey}")
    } else if (item.piiTypePattern.trim == "*") {
      Some(s"pii_type=* is not allowed: ${item.findingKey}")
    } else if (item.fileIdentifierPattern.trim.isEmpty && finding.hasMultipleFileEvidence) {
      Some(s"file_identifier_pattern is required for multi-file findings: ${item.findingKey}")
    } else {
      None
    }
  }

  private def instantOrderingKey(value: String): (Long, Int) = {
    val instant = Instant.parse(value)
    instant.getEpochSecond -> instant.getNano
  }

  private final case class ExactScope(affectedKeys: Set[AllowlistKey], entries: Seq[AllowlistEntry])

  private def collectExactScope(
    spark: SparkSession,
    config: ReviewCollectCliConfig,
    latestAccepted: Seq[AcceptedResponse]
  ): ExactScope = {
    val latestFindingKeys = latestAccepted.map(_.finding.findingKey).toSet
    val exactFalsePositiveByKey = latestAccepted
      .filter(response => response.item.decision == ReviewStatus.FalsePositive && response.item.allowlistScope == "exact")
      .map(response => response.finding.findingKey -> response)
      .toMap

    if (latestFindingKeys.isEmpty) {
      ExactScope(Set.empty, Seq.empty)
    } else {
      val affectedKeys = scala.collection.mutable.Set.empty[AllowlistKey]
      val entries = ArrayBuffer.empty[AllowlistEntry]
      ScanResultsReader
        .iterateScanResults(ScanResultsReader.read(spark, config.scanResultsPath), ordered = true)
        .foreach { result =>
          val findingKey = ReviewFindingBuilder.findingKeyForResult(result)
          if (latestFindingKeys.contains(findingKey)) {
            ReviewFindingBuilder.evidenceFromScanResult(result).foreach { evidence =>
              affectedKeys += AllowlistKey(result.dataset_path, evidence.fileIdentifier, result.column_name, result.pii_type)
              exactFalsePositiveByKey.get(findingKey).foreach { response =>
                entries += AllowlistEntry(
                  datasetPath = result.dataset_path,
                  fileIdentifier = evidence.fileIdentifier,
                  columnName = result.column_name,
                  piiType = result.pii_type,
                  reason = response.item.falsePositiveReason,
                  reviewer = response.responder,
                  reviewedAt = response.respondedAt,
                  sourceRunId = findingKey,
                  fileSize = evidence.fileSize,
                  fileMtimeEpochMs = evidence.fileMtimeEpochMs,
                  fileChecksumAlgo = evidence.fileChecksumAlgo,
                  fileChecksum = evidence.fileChecksum
                )
              }
            }
          }
        }
      ExactScope(affectedKeys.toSet, entries.toSeq)
    }
  }

  private def toPatternAllowlistEntry(response: AcceptedResponse): PatternAllowlistEntry = {
    val filePattern = response.item.fileIdentifierPattern.trim match {
      case "" => response.finding.evidence.headOption.map(_.fileIdentifier).getOrElse("*")
      case value => value
    }
    PatternAllowlistEntry(
      datasetPath = response.finding.scanPath,
      fileIdentifierPattern = filePattern,
      columnNamePattern = emptyTo(response.item.columnNamePattern, response.finding.columnName),
      piiTypePattern = emptyTo(response.item.piiTypePattern, response.finding.piiType),
      reason = response.item.falsePositiveReason,
      reviewer = response.responder,
      reviewedAt = response.respondedAt,
      expiresAt = response.item.expiresAt,
      sourceFindingKey = response.finding.findingKey
    )
  }

  private def patternCoversFinding(entry: PatternAllowlistEntry, finding: ReviewFinding): Boolean =
    entry.datasetPath == finding.scanPath &&
      findingIdentifiers(finding).exists(wildcardMatches(entry.fileIdentifierPattern, _)) &&
      wildcardMatches(entry.columnNamePattern, finding.columnName) &&
      wildcardMatches(entry.piiTypePattern, finding.piiType)

  private def findingIdentifiers(finding: ReviewFinding): Seq[String] =
    (Seq(finding.fileIdentifier) ++ finding.evidence.map(_.fileIdentifier))
      .map(identifier => Option(identifier).getOrElse(""))
      .filter(_.nonEmpty)
      .distinct

  private def wildcardMatches(pattern: String, value: String): Boolean = {
    val normalizedPattern = Option(pattern).getOrElse("")
    val normalizedValue = Option(value).getOrElse("")
    val regex = normalizedPattern.flatMap {
      case '*' => ".*"
      case ch if "\\.[]{}()+-^$?|".contains(ch) => "\\" + ch
      case ch => ch.toString
    }
    normalizedValue.matches(regex)
  }

  private def actionPlanCoversFinding(plan: ActionPlan, finding: ReviewFinding): Boolean = {
    val sameScanAndType = plan.scanPath == finding.scanPath &&
      plan.columnName == finding.columnName &&
      plan.piiType == finding.piiType
    if (!sameScanAndType) {
      false
    } else if (plan.fileIdentifier.trim.nonEmpty) {
      plan.fileIdentifier == finding.fileIdentifier
    } else {
      plan.hiveTableFqn.trim.isEmpty || plan.hiveTableFqn == finding.hiveTableFqn
    }
  }

  private def toActionPlan(response: AcceptedResponse): ActionPlan =
    ActionPlan(
      findingKey = response.finding.findingKey,
      scanPath = response.finding.scanPath,
      fileIdentifier = response.finding.fileIdentifier,
      hiveDatabase = response.finding.hiveDatabase,
      hiveTable = response.finding.hiveTable,
      hiveTableFqn = response.finding.hiveTableFqn,
      columnName = response.finding.columnName,
      piiType = response.finding.piiType,
      actionPlan = response.item.actionPlan,
      actionDueDate = response.item.actionDueDate,
      responder = response.responder,
      respondedAt = response.respondedAt,
      status = RemediationPlanned
    )

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
        scanResultsFingerprint = extractJsonStringField(json, "scan_results_fingerprint").getOrElse(""),
        responder = extractJsonStringField(json, "responder").getOrElse(""),
        respondedAt = extractJsonStringField(json, "responded_at").getOrElse(""),
        responses = responseObjects.map(parseItem)
      ))
    }
  }

  private def parseItem(json: String): ResponseItem =
    ResponseItem(
      findingKey = extractJsonStringField(json, "finding_key").getOrElse(""),
      findingHash = extractJsonStringField(json, "finding_hash").getOrElse(""),
      decision = extractJsonStringField(json, "decision").getOrElse(""),
      falsePositiveReason = extractJsonStringField(json, "false_positive_reason").getOrElse(""),
      allowlistScope = extractJsonStringField(json, "allowlist_scope").getOrElse(""),
      fileIdentifierPattern = extractJsonStringField(json, "file_identifier_pattern").getOrElse(""),
      columnNamePattern = extractJsonStringField(json, "column_name_pattern").getOrElse(""),
      piiTypePattern = extractJsonStringField(json, "pii_type_pattern").getOrElse(""),
      expiresAt = extractJsonStringField(json, "expires_at").getOrElse(""),
      actionPlan = extractJsonStringField(json, "action_plan").getOrElse(""),
      actionDueDate = extractJsonStringField(json, "action_due_date").getOrElse("")
    )

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

  private def writeState(
    conf: Configuration,
    statePath: String,
    findings: Seq[ReviewFinding],
    exactEntries: Seq[AllowlistEntry],
    patternEntries: Seq[PatternAllowlistEntry],
    actionPlans: Seq[ActionPlan],
    accepted: Seq[AcceptedResponse]
  ): Unit = {
    val allowlistLines = exactEntries.map(AllowlistJson.exactEntryToJson) ++ patternEntries.map(AllowlistJson.patternEntryToJson)
    writeLines(conf, s"$statePath/allowlist.jsonl", allowlistLines)
    writeLines(conf, s"$statePath/action_plan.jsonl", actionPlans.map(actionPlanToJson))
    writeLines(conf, s"$statePath/finding_status.jsonl", findingStatusLines(findings, accepted, actionPlans))
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
    s"""{"source_path":${jsonString(response.sourcePath)},"scan_path":${jsonString(response.finding.scanPath)},"file_identifier":${jsonString(response.finding.fileIdentifier)},"finding_key":${jsonString(response.finding.findingKey)},"column_name":${jsonString(response.finding.columnName)},"pii_type":${jsonString(response.finding.piiType)},"decision":${jsonString(response.item.decision)},"responder":${jsonString(response.responder)},"responded_at":${jsonString(response.respondedAt)}}"""

  private def findingStatusLines(
    findings: Seq[ReviewFinding],
    accepted: Seq[AcceptedResponse],
    actionPlans: Seq[ActionPlan]
  ): Seq[String] = {
    val activeFindingKeys = findings.map(_.findingKey).toSet
    findings.map(findingStatusToJson(_, accepted, actionPlans)) ++
      actionPlans
        .filterNot(plan => activeFindingKeys.contains(plan.findingKey))
        .map(plan => actionPlanStatusToJson(plan, RemediatedCandidate))
  }

  private def findingStatusToJson(
    finding: ReviewFinding,
    accepted: Seq[AcceptedResponse],
    actionPlans: Seq[ActionPlan]
  ): String = {
    val status =
      if (accepted.exists(response => response.finding.findingKey == finding.findingKey && response.item.decision == ReviewStatus.FalsePositive)) {
        ReviewStatus.FalsePositive
      } else {
        actionPlans.find(_.findingKey == finding.findingKey).map(currentActionStatus).getOrElse(ReviewStatus.Pending)
      }
    s"""{"finding_key":${jsonString(finding.findingKey)},"scan_path":${jsonString(finding.scanPath)},"file_identifier":${jsonString(finding.fileIdentifier)},"hive_table_fqn":${jsonString(finding.hiveTableFqn)},"column_name":${jsonString(finding.columnName)},"pii_type":${jsonString(finding.piiType)},"status":${jsonString(status)}}"""
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
  private val RemediatedCandidate = "remediated_candidate"
  private val Verified = "verified"
}
