package io.github.jonggeun2001.privyspark.review.collect

import io.github.jonggeun2001.privyspark.report.JsonCodec.{extractJsonStringField, jsonString}
import io.github.jonggeun2001.privyspark.review.collect.ResponseEnvelopeReader.splitHiveTableFqn
import io.github.jonggeun2001.privyspark.review.collect.ReviewStateStatuses._
import io.github.jonggeun2001.privyspark.review.{AcceptedResponse, ActionPlan, AllowlistJson, RecurringAllowlistEntry, ReviewStatus}
import io.github.jonggeun2001.privyspark.util.DriverLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.time.LocalDate
import scala.util.Try

private[privyspark] object ReviewStateWriter {
  def resolveReadableStateFilePath(conf: Configuration, path: String): Option[String] =
    Seq(path, s"$path.bak").find(pathExists(conf, _))

  def loadActionPlans(conf: Configuration, path: String): Seq[ActionPlan] = {
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

  def writeState(
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

  def replacePath(conf: Configuration, sourcePath: String, destinationPath: String): Unit = {
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

  private def pathExists(conf: Configuration, path: String): Boolean = {
    val hadoopPath = new Path(path)
    hadoopPath.getFileSystem(conf).exists(hadoopPath)
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
}
