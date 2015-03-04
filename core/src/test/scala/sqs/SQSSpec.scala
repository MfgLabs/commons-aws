package com.mfglabs.commons.aws

import akka.actor._
import akka.stream._
import akka.stream.scaladsl._

import com.amazonaws.auth.{BasicAWSCredentials, AWSCredentials, AWSCredentialsProvider}
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.{Regions, Region}
import com.amazonaws.services.sqs.{AmazonSQSAsyncClient, AmazonSQSClient}
import com.amazonaws.services.sqs.model.{Message, CreateQueueRequest}
import com.mfglabs.commons.stream.MFGSink
import com.mfglabs.stream.{FlowExt, SinkExt}
import com.pellucid.wrap.sqs.AmazonSQSScalaClient
import org.apache.http.impl.client.BasicCredentialsProvider

import org.scalatest.concurrent.{AsyncAssertions, ScalaFutures}
import org.scalatest.time.{Seconds, Millis, Minutes, Span}
import org.scalatest.{Matchers, FlatSpec}
import sqs.SQSStreamBuilder

import scala.concurrent._
import scala.concurrent.duration._

import scala.util.{Try, Random}

class SQSSpec extends FlatSpec with Matchers with ScalaFutures {
  import scala.collection.JavaConversions._

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(60, Seconds), interval = Span(5, Millis))

  implicit val as = ActorSystem()
  implicit val fm = ActorFlowMaterializer()

  import scala.concurrent.ExecutionContext.Implicits.global

  val sqs = new AmazonSQSScalaClient(new AmazonSQSAsyncClient(), scala.concurrent.ExecutionContext.Implicits.global)
  val builder = SQSStreamBuilder(sqs)

  val testQueueName = "commons-aws-sqs-test-" + Random.nextInt()

  "SQS client" should "send message and receive them as streams" in {
    sqs.client.setRegion(Region.getRegion(Regions.EU_WEST_1))

    sqs.listQueues("commons-aws-sqs-test-").futureValue.headOption.foreach(queueUrl => sqs.deleteQueue(queueUrl).futureValue)
    val newQueueReq = new CreateQueueRequest()
    newQueueReq.setAttributes(Map("VisibilityTimeout" -> 10.toString)) // 10 seconds
    newQueueReq.setQueueName(testQueueName)
    val queueUrl = sqs.createQueue(newQueueReq).futureValue.getQueueUrl

    val msgs = for (i <- 1 to 200) yield s"Message $i"
    val futSent = Source(msgs).via(builder.sendMessageAsStream(queueUrl)).take(200).runWith(SinkExt.collect)
    val futReceived = builder.receiveMessageAsStream(queueUrl, autoAck = true).take(200).runWith(SinkExt.collect)

    val (sent, received) = futSent.zip(futReceived).futureValue

    received.map(_.getBody).sorted shouldEqual msgs.sorted

    // testing auto-ack (queue must be empty)
    val res = builder
      .receiveMessageAsStream(queueUrl, longPollingMaxWait = 1 second)
      .takeWithin(10 seconds)
      .runWith(SinkExt.collect)
      .futureValue
    res shouldBe empty

    sqs.deleteQueue(queueUrl).futureValue
  }
}

