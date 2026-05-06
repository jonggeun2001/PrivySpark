package io.github.jonggeun2001.privyspark.review

import io.github.jonggeun2001.privyspark.cli.ReviewCollectCliConfig
import io.github.jonggeun2001.privyspark.review.collect.{ResponseEnvelopeReader, ResponseValidator, ReviewCollectLock, ReviewStateBuilder, ReviewStateWriter}
import io.github.jonggeun2001.privyspark.util.DriverLogger
import org.apache.spark.sql.SparkSession

import java.time.Instant
import java.util.UUID
import scala.collection.mutable.ArrayBuffer

private[privyspark] final case class ResponseEnvelope(
  sourcePath: String,
  scanPath: String,
  responder: String,
  respondedAt: String,
  responses: Seq[ResponseItem]
)

private[privyspark] final case class ResponseItem(
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

private[privyspark] final case class AcceptedResponse(
  scanPath: String,
  item: ResponseItem,
  responder: String,
  respondedAt: String,
  sourcePath: String
)

private[privyspark] final case class RejectedResponse(sourcePath: String, reason: String)

private[privyspark] final case class ActionPlan(
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

private[privyspark] object ReviewCollectCommand {
  def run(spark: SparkSession, config: ReviewCollectCliConfig): Unit = {
    val conf = spark.sparkContext.hadoopConfiguration
    val root = config.reviewStateRoot.stripSuffix("/")
    val inboxPath = s"$root/inbox"
    val currentPath = s"$root/current"

    ReviewCollectLock.withLock(conf, root) {
      val envelopes = ResponseEnvelopeReader.readResponseEnvelopes(conf, inboxPath)
      val rejected = ArrayBuffer.empty[RejectedResponse]
      val accepted = ArrayBuffer.empty[AcceptedResponse]

      envelopes.foreach {
        case Left(rejection) =>
          rejected += rejection
        case Right(envelope) =>
          ResponseValidator.validateEnvelope(envelope) match {
            case Some(reason) =>
              rejected += RejectedResponse(envelope.sourcePath, reason)
            case None =>
              envelope.responses.foreach { item =>
                ResponseValidator.validateItem(item) match {
                  case Some(reason) =>
                    rejected += RejectedResponse(envelope.sourcePath, reason)
                  case None =>
                    accepted += AcceptedResponse(envelope.scanPath, item, envelope.responder, envelope.respondedAt, envelope.sourcePath)
                }
              }
          }
      }

      if (rejected.nonEmpty) {
        val reasons = rejected.map(rejection => s"${rejection.sourcePath}: ${rejection.reason}")
        throw new IllegalStateException(s"Rejected review responses: ${reasons.mkString("; ")}")
      }

      val existingAllowlistPath = s"$currentPath/allowlist.jsonl"
      val existingAllowlistReadablePath = ReviewStateWriter.resolveReadableStateFilePath(conf, existingAllowlistPath)
      val existingRecurring = existingAllowlistReadablePath.map(AllowlistMatcher.loadRecurringEntries(conf, _)).getOrElse(Seq.empty)
      val existingActionPlans = ReviewStateWriter.loadActionPlans(conf, s"$currentPath/action_plan.jsonl")
      val currentState = ReviewStateBuilder.build(accepted.toSeq, existingRecurring, existingActionPlans)

      val collectRunId = s"${Instant.now().toString.replace(':', '-')}-${UUID.randomUUID().toString.take(8)}"
      val tempCurrentPath = s"$root/current.tmp-$collectRunId"
      ReviewStateWriter.writeState(
        conf,
        tempCurrentPath,
        currentState.recurringEntries,
        currentState.actionPlans,
        currentState.latestAccepted
      )
      ReviewStateWriter.replacePath(conf, tempCurrentPath, currentPath)

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
  }
}
