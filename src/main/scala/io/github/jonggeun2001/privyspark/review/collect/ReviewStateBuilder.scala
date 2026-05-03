package io.github.jonggeun2001.privyspark.review.collect

import io.github.jonggeun2001.privyspark.review.collect.ResponseEnvelopeReader.splitHiveTableFqn
import io.github.jonggeun2001.privyspark.review.collect.ReviewStateStatuses._
import io.github.jonggeun2001.privyspark.review.collect.ResponseValidator.recurringFileIdentifierPattern
import io.github.jonggeun2001.privyspark.review.{AcceptedResponse, ActionPlan, RecurringAllowlistEntry, ReviewPathNormalizer, ReviewStatus}

import java.time.Instant

private[privyspark] final case class ReviewCurrentState(
  recurringEntries: Seq[RecurringAllowlistEntry],
  actionPlans: Seq[ActionPlan],
  latestAccepted: Seq[AcceptedResponse]
)

private[privyspark] object ReviewStateStatuses {
  val RemediationPlanned = "remediation_planned"
  val Overdue = "overdue"
  val Verified = "verified"
}

private[privyspark] object ReviewStateBuilder {
  def build(
    accepted: Seq[AcceptedResponse],
    existingRecurring: Seq[RecurringAllowlistEntry],
    existingActionPlans: Seq[ActionPlan]
  ): ReviewCurrentState = {
    val latestAccepted = accepted
      .groupBy(_.item.findingKey)
      .values
      .map(_.maxBy(response => instantOrderingKey(response.respondedAt)))
      .toSeq

    val latestFindingKeys = latestAccepted.map(_.item.findingKey).toSet
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

    ReviewCurrentState(recurringEntries, actionPlans, latestAccepted)
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
        fieldMatches(entry.columnName, response.item.columnName, entry.fieldWildcardsEnabled) &&
        fieldMatches(entry.piiType, response.item.piiType, entry.fieldWildcardsEnabled)
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

  private def fieldMatches(pattern: String, value: String, wildcardsEnabled: Boolean): Boolean = {
    val normalizedPattern = Option(pattern).getOrElse("")
    val normalizedValue = Option(value).getOrElse("")
    if (wildcardsEnabled && normalizedPattern.contains("*")) wildcardMatches(normalizedPattern, normalizedValue)
    else normalizedPattern == normalizedValue
  }

  private def wildcardMatches(pattern: String, value: String): Boolean = {
    val normalizedPattern = Option(pattern).getOrElse("")
    val normalizedValue = Option(value).getOrElse("")
    if (normalizedPattern.trim.isEmpty) {
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
    val normalizedPattern = Option(pattern).map(_.stripSuffix("/")).getOrElse("")
    val normalizedRepresentative = Option(representative).map(_.stripSuffix("/")).getOrElse("")
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
}
