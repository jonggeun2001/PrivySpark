package io.github.jonggeun2001.privyspark.detect
package testing

object DetectionFaultInjectors {
  def withForcedDatasetBatchFailure[A](block: => A): A = {
    withFaultInjector(new DetectionAggregator.FaultInjector {
      override def beforeDatasetBatchAggregation(): Unit = {
        throw new RuntimeException("forced-dataset-batch-failure")
      }
    })(block)
  }

  def withForcedFileBatchFailure[A](block: => A): A = {
    withFaultInjector(new DetectionAggregator.FaultInjector {
      override def beforeFileBatchAggregation(): Unit = {
        throw new RuntimeException("forced-file-batch-failure")
      }
    })(block)
  }

  def withForcedFileSampleBatchFailure[A](block: => A): A = {
    withFaultInjector(new DetectionAggregator.FaultInjector {
      private var consumed = false

      override def beforeFileSampleCollection(): Unit = synchronized {
        if (!consumed) {
          consumed = true
          throw new RuntimeException("forced-file-sample-batch-failure")
        }
      }
    })(block)
  }

  private def withFaultInjector[A](injector: DetectionAggregator.FaultInjector)(block: => A): A = {
    val previous = DetectionAggregator.faultInjector
    DetectionAggregator.faultInjector = injector
    try {
      block
    } finally {
      DetectionAggregator.faultInjector = previous
    }
  }
}
