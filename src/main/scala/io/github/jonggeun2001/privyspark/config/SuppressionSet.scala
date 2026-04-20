package io.github.jonggeun2001.privyspark.config

import io.github.jonggeun2001.privyspark.model.Suppression

import java.util.Locale

final case class SuppressionSet private (entries: Set[(String, String)]) {
  def isSuppressed(normalizedColumn: String, piiType: String): Boolean = {
    entries.contains((normalizedColumn, piiType))
  }

  def merge(other: SuppressionSet): SuppressionSet = {
    SuppressionSet(entries ++ other.entries)
  }

  def isEmpty: Boolean = entries.isEmpty

  def size: Int = entries.size
}

object SuppressionSet {
  val empty: SuppressionSet = SuppressionSet(Set.empty)

  def from(items: Seq[Suppression]): SuppressionSet = {
    SuppressionSet(
      items.iterator
        .map(suppression => (normalizeColumnName(suppression.columnName), suppression.piiType.trim))
        .filter { case (columnName, piiType) => columnName.nonEmpty && piiType.nonEmpty }
        .toSet
    )
  }

  private def normalizeColumnName(columnName: String): String = {
    Option(columnName).map(_.trim.toLowerCase(Locale.ROOT)).getOrElse("")
  }
}
