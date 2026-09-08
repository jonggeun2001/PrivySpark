package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.review.collect.ReviewStateStatuses._
import io.github.jonggeun2001.privyspark.review.collect.ReviewStateWriter
import org.apache.hadoop.conf.Configuration

import java.time.LocalDate
import scala.collection.mutable
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
  ): Map[String, ReviewActionPlanStatus] = {
    if (findings.isEmpty || actionPlans.isEmpty) return Map.empty

    val latestPlans = mutable.Map.empty[MatchKey, ActionPlan]
    val ordering = implicitly[Ordering[(String, String)]]
    actionPlans.foreach { plan =>
      val key = matchingKey(plan.scanPath, plan.columnName, plan.piiType, plan.hiveTableFqn, plan.fileIdentifier)
      val isLatest = latestPlans.get(key).forall { previous =>
        ordering.gteq((plan.respondedAt, plan.findingKey), (previous.respondedAt, previous.findingKey))
      }
      if (isLatest) latestPlans.update(key, plan)
    }
    findings.flatMap { finding =>
      val key = matchingKey(finding.scanPath, finding.columnName, finding.piiType, finding.hiveTableFqn, finding.fileIdentifier)
      latestPlans.get(key).map(plan => finding.findingKey -> fromActionPlan(plan))
    }.toMap
  }

  private final case class MatchKey(
    scanPath: String,
    columnName: String,
    piiType: String,
    hiveTable: Boolean,
    identifier: String
  )

  private def matchingKey(
    scanPath: String,
    columnName: String,
    piiType: String,
    hiveTableFqn: String,
    fileIdentifier: String
  ): MatchKey = {
    val hiveTable = hiveTableFqn.trim.nonEmpty
    MatchKey(ReviewPathNormalizer.normalizeScanPath(scanPath), columnName, piiType, hiveTable,
      if (hiveTable) hiveTableFqn else fileIdentifier)
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
