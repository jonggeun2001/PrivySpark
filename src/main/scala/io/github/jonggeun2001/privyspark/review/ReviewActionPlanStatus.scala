package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.review.collect.ReviewStateStatuses._
import io.github.jonggeun2001.privyspark.review.collect.ReviewStateWriter
import org.apache.hadoop.conf.Configuration

import java.time.LocalDate
import scala.util.Try

private[privyspark] final case class ReviewActionPlanStatus(
  actionPlan: String,
  actionDueDate: String,
  responder: String,
  respondedAt: String,
  status: String,
  statusLabel: String
)

private[privyspark] object ReviewActionPlanStatus {
  def load(conf: Configuration, reviewStateRoot: Option[String]): Seq[ActionPlan] =
    reviewStateRoot.toSeq.flatMap { root =>
      ReviewStateWriter.loadActionPlans(conf, s"${root.stripSuffix("/")}/current/action_plan.jsonl")
    }

  def matchFindings(
    findings: Seq[ReviewFinding],
    actionPlans: Seq[ActionPlan]
  ): Map[String, ReviewActionPlanStatus] =
    findings.flatMap { finding =>
      latestMatchingPlan(finding, actionPlans).map(plan => finding.findingKey -> fromActionPlan(plan))
    }.toMap

  private def latestMatchingPlan(finding: ReviewFinding, actionPlans: Seq[ActionPlan]): Option[ActionPlan] =
    actionPlans
      .filter(plan => matches(finding, plan))
      .sortBy(plan => (plan.respondedAt, plan.findingKey))
      .lastOption

  private def matches(finding: ReviewFinding, plan: ActionPlan): Boolean = {
    val sameScanAndType =
      ReviewPathNormalizer.normalizeScanPath(plan.scanPath) == ReviewPathNormalizer.normalizeScanPath(finding.scanPath) &&
        plan.columnName == finding.columnName &&
        plan.piiType == finding.piiType
    if (!sameScanAndType) {
      false
    } else if (plan.hiveTableFqn.trim.nonEmpty || finding.hiveTableFqn.trim.nonEmpty) {
      plan.hiveTableFqn == finding.hiveTableFqn
    } else {
      plan.fileIdentifier == finding.fileIdentifier
    }
  }

  private def fromActionPlan(plan: ActionPlan): ReviewActionPlanStatus = {
    val status = effectiveStatus(plan)
    ReviewActionPlanStatus(
      actionPlan = plan.actionPlan,
      actionDueDate = plan.actionDueDate,
      responder = plan.responder,
      respondedAt = plan.respondedAt,
      status = status,
      statusLabel = statusLabel(status, plan.actionPlan)
    )
  }

  private def effectiveStatus(plan: ActionPlan): String = {
    if (plan.status == Verified) {
      Verified
    } else if (plan.status == Overdue) {
      Overdue
    } else if (Try(LocalDate.parse(plan.actionDueDate)).toOption.exists(_.isBefore(LocalDate.now()))) {
      Overdue
    } else {
      RemediationPlanned
    }
  }

  private def statusLabel(status: String, actionPlan: String): String = status match {
    case Verified => "조치 확인됨"
    case Overdue => "조치 기한 초과"
    case _ =>
      val normalizedPlan = Option(actionPlan).getOrElse("")
      if (normalizedPlan.contains("삭제")) {
        "삭제 조치 필요"
      } else if (normalizedPlan.contains("마스킹")) {
        "마스킹 조치 필요"
      } else {
        "조치 필요"
      }
  }
}
