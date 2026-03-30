package io.github.jonggeun2001.privyspark

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DriverLicenseNumberValidatorSpec extends AnyFunSuite {
  test("accepts valid legacy and current driver license numbers") {
    assert(DriverLicenseNumberValidator.isValid("12-345678-90"))
    assert(DriverLicenseNumberValidator.isValid("1234567890"))
    assert(DriverLicenseNumberValidator.isValid("11-12-345678-90"))
    assert(DriverLicenseNumberValidator.isValid("111234567890"))
  }

  test("rejects invalid driver license numbers") {
    assert(!DriverLicenseNumberValidator.isValid("27-12-345678-90"))
    assert(!DriverLicenseNumberValidator.isValid("271234567890"))
    assert(!DriverLicenseNumberValidator.isValid("11-12-345678-9"))
    assert(!DriverLicenseNumberValidator.isValid("12 345678 90"))
    assert(!DriverLicenseNumberValidator.isValid("noise"))
  }
}
