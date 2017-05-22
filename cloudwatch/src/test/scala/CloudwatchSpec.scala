package com.mfglabs.commons.aws

import org.scalatest._
import concurrent.ScalaFutures
import org.scalatest.time.{Minutes, Millis, Span}

class CloudwatchSpec extends FlatSpec with Matchers with ScalaFutures {
  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(2, Minutes), interval = Span(5, Millis))

  val CW = cloudwatch.AmazonCloudwatchClient()()

  "Cloudwatch client" should "retrieve all metrics" in {
    whenReady(CW.listMetrics()) { s => s.getMetrics should not be 'empty }
  }

}
