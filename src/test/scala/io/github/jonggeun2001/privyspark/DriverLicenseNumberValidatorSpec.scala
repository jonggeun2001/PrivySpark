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
    assert(DriverLicenseNumberValidator.isValid("서울 07 - 111111 - 10"))
    assert(DriverLicenseNumberValidator.isValid("부산0711111110"))
  }

  test("rejects invalid driver license numbers") {
    assert(!DriverLicenseNumberValidator.isValid("27-12-345678-90"))
    assert(!DriverLicenseNumberValidator.isValid("271234567890"))
    assert(!DriverLicenseNumberValidator.isValid("11-12-345678-9"))
    assert(!DriverLicenseNumberValidator.isValid("12 345678 90"))
    assert(!DriverLicenseNumberValidator.isValid("세종 07 - 111111 - 10"))
    assert(!DriverLicenseNumberValidator.isValid("noise"))
  }

  test("finds at least one valid candidate in free-form text") {
    assert(DriverLicenseNumberValidator.containsValidCandidate("이전 번호 27-12-345678-90, 현재 번호 11-12-345678-90"))
    assert(DriverLicenseNumberValidator.containsValidCandidate("구형 면허번호 서울 07 - 111111 - 10"))
    assert(!DriverLicenseNumberValidator.containsValidCandidate("이전 번호 27-12-345678-90, 현재 번호 29-12-345678-90"))
  }

  test("extracts the first valid candidate from free-form text") {
    assert(
      DriverLicenseNumberValidator.findFirstValidCandidate("이전 번호 27-12-345678-90, 현재 번호 11-12-345678-90")
        .exists(_.candidate == "11-12-345678-90")
    )
    assert(
      DriverLicenseNumberValidator.findFirstValidCandidate("메모: 서울 07 - 111111 - 10 재발급")
        .exists(_.candidate == "서울 07 - 111111 - 10")
    )
    assert(DriverLicenseNumberValidator.findFirstValidCandidate("면허번호 없음").isEmpty)
  }
}
