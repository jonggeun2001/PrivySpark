package io.github.jonggeun2001.privyspark.hive

import org.apache.spark.broadcast.Broadcast

private[privyspark] object HiveTableFqnResolver {
  def resolve(hiveLookup: Option[Broadcast[HiveTableLookupIndex]], rawPath: String): String =
    hiveLookup.map(_.value.lookup(HiveTableLookup.stripCompositeIdentifier(rawPath))).getOrElse("")
}
