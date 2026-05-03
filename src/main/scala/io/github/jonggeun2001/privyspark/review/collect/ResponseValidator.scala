package io.github.jonggeun2001.privyspark.review.collect

import io.github.jonggeun2001.privyspark.review.{ResponseEnvelope, ResponseItem, ReviewPathNormalizer, ReviewStatus}

import java.time.{Instant, LocalDate}
import scala.util.Try

private[privyspark] object ResponseValidator {
  def validateEnvelope(envelope: ResponseEnvelope): Option[String] = {
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

  def validateItem(item: ResponseItem): Option[String] = {
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
    } else if (hasFieldWildcard(item.columnName) || hasFieldWildcard(item.piiType)) {
      Some(s"column_name and pii_type must be exact values without wildcard '*': ${item.findingKey}")
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

  private[privyspark] def recurringFileIdentifierPattern(item: ResponseItem): String =
    Option(item.fileIdentifierPattern).filter(_.trim.nonEmpty)
      .getOrElse(Option(item.fileIdentifier).getOrElse(""))

  private def hasFieldWildcard(value: String): Boolean =
    Option(value).exists(_.contains("*"))
}
