package io.github.jonggeun2001.privyspark.util

private[privyspark] object DetectionMetricMath {
  def ratio(numerator: Long, denominator: Long): Double = {
    if (denominator <= 0L) {
      0.0
    } else {
      roundProbability(numerator.toDouble / denominator.toDouble)
    }
  }

  def wilsonLowerBound(successes: Long, trials: Long): Double = {
    if (trials <= 0L) {
      0.0
    } else {
      val n = trials.toDouble
      val p = math.max(0.0, math.min(1.0, successes.toDouble / n))
      val z = 1.96
      val z2 = z * z
      val center = p + z2 / (2.0 * n)
      val margin = z * math.sqrt(p * (1.0 - p) / n + z2 / (4.0 * n * n))
      val denominator = 1.0 + z2 / n
      roundProbability(math.max(0.0, math.min(1.0, (center - margin) / denominator)))
    }
  }

  def roundProbability(value: Double): Double = {
    BigDecimal.decimal(value)
      .setScale(2, scala.math.BigDecimal.RoundingMode.HALF_UP)
      .toDouble
  }
}
