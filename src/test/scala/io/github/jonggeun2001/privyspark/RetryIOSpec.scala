package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.fsio.RetryIO

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RetryIOSpec extends AnyFunSuite {
  test("retry refresh targets omit parent paths by default") {
    assert(
      RetryIO.refreshTargetsForRetry(Seq("/warehouse/table/part-0001.csv"), refreshParent = false) ==
        Seq("/warehouse/table/part-0001.csv")
    )
  }

  test("retry refresh targets can opt back into parent path refresh") {
    assert(
      RetryIO.refreshTargetsForRetry(Seq("/warehouse/table/part-0001.csv"), refreshParent = true) ==
        Seq("/warehouse/table/part-0001.csv", "/warehouse/table")
    )
  }

  test("retry delay uses exponential backoff with bounded jitter") {
    assert(RetryIO.retryDelayMillis(baseDelayMs = 200L, nextAttempt = 2, jitterRatio = 0.0, randomFraction = 0.0) == 200L)
    assert(RetryIO.retryDelayMillis(baseDelayMs = 200L, nextAttempt = 3, jitterRatio = 0.0, randomFraction = 0.0) == 400L)
    assert(RetryIO.retryDelayMillis(baseDelayMs = 200L, nextAttempt = 3, jitterRatio = 0.25, randomFraction = 1.0) == 500L)
  }
}
