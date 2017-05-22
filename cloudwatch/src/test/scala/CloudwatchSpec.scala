package com.mfglabs.commons.aws

import collection.mutable.Stack
import org.scalatest._
import concurrent.ScalaFutures
import org.scalatest.time.{Minutes, Millis, Seconds, Span}
import scala.concurrent.Future

class CloudwatchSpec extends FlatSpec with Matchers with ScalaFutures {
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(2, Minutes), interval = Span(5, Millis))

  // val cred = new com.amazonaws.auth.BasicAWSCredentials("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
  val CW = new cloudwatch.AmazonCloudwatchClient()

  "Cloudwatch client" should "retrieve all metrics" in {
    whenReady(CW.listMetrics()) { s => s.getMetrics should not be 'empty }
  }

}
