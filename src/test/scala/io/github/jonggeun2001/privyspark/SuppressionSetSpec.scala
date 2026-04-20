package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.config.SuppressionSet
import io.github.jonggeun2001.privyspark.model.Suppression
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SuppressionSetSpec extends AnyFunSuite {
  test("from normalizes case and whitespace and removes duplicates") {
    val suppressions = SuppressionSet.from(Seq(
      Suppression(" PRDCTCD ", " driver_license_number "),
      Suppression("prdctcd", "driver_license_number"),
      Suppression("   ", "phone_number"),
      Suppression("phone", "   ")
    ))

    assert(suppressions.isSuppressed("prdctcd", "driver_license_number"))
    assert(!suppressions.isSuppressed("PRDCTCD", "driver_license_number"))
    assert(!suppressions.isSuppressed("phone", "phone_number"))
  }

  test("merge unions yaml and cli suppressions without duplicating entries") {
    val yamlSuppressions = SuppressionSet.from(Seq(
      Suppression("prdctcd", "driver_license_number"),
      Suppression("email", "phone_number")
    ))
    val cliSuppressions = SuppressionSet.from(Seq(
      Suppression("PRDCTCD", "driver_license_number"),
      Suppression("phone", "email")
    ))

    val merged = yamlSuppressions.merge(cliSuppressions)

    assert(merged.isSuppressed("prdctcd", "driver_license_number"))
    assert(merged.isSuppressed("email", "phone_number"))
    assert(merged.isSuppressed("phone", "email"))
  }

  test("empty suppression set never suppresses metrics") {
    assert(!SuppressionSet.empty.isSuppressed("prdctcd", "driver_license_number"))
    assert(SuppressionSet.empty.isEmpty)
  }
}
