package com.mfglabs.commons.aws
package sqs

import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import com.amazonaws.services.sqs.model.{CreateQueueRequest, MessageAttributeValue, SendMessageRequest}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.util.Random

class SQSSpec extends FlatSpec with Matchers with ScalaFutures {
  import com.amazonaws.regions.Regions
  import scala.collection.JavaConverters._

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(60, Seconds), interval = Span(5, Millis))

  implicit val as = ActorSystem()
  implicit val fm = ActorMaterializer()

  val sqsClient  = AmazonSQSClient.from(Regions.EU_WEST_1)()

  val testQueueName = "commons-aws-sqs-test-" + Random.nextInt()
  val testQueueName2 = "commons-aws-sqs-test-" + Random.nextInt()

  val messageAttributeKey = "attribute_key"

  "SQS client" should "delete all test queue if any" in {
    sqsClient.listQueues("commons-aws-sqs-test-").futureValue.headOption.foreach(queueUrl => sqsClient.deleteQueue(queueUrl).futureValue)
  }

  it should "send message and receive them as streams" in {
    val newQueueReq = new CreateQueueRequest()
    newQueueReq.setAttributes(Map("VisibilityTimeout" -> 10.toString).asJava) // 10 seconds
    newQueueReq.setQueueName(testQueueName)
    val queueUrl = sqsClient.createQueue(newQueueReq).futureValue.getQueueUrl

    val msgs = for (i <- 1 to 200) yield s"Message $i"
    val futSent = Source(msgs)
      .map { body =>
      val req = new SendMessageRequest()
      req.addMessageAttributesEntry(messageAttributeKey, new MessageAttributeValue().withDataType("String").withStringValue(body))
      req.setMessageBody(body)
      req.setQueueUrl(queueUrl)
      req
    }
      .via(sqsClient.sendMessageAsStream())
      .take(200)
      .runWith(Sink.seq)
    val futReceived = sqsClient.receiveMessageAsStream(queueUrl, autoAck = true, messageAttributeNames = List(messageAttributeKey)).take(200).runWith(Sink.seq)

    val (_, received) = futSent.zip(futReceived).futureValue

    received.map(_.getBody).sorted shouldEqual msgs.sorted
    received.map(_.getMessageAttributes.get(messageAttributeKey).getStringValue).sorted shouldEqual msgs.sorted

    // testing auto-ack (queue must be empty)
    val res = sqsClient
      .receiveMessageAsStream(queueUrl, longPollingMaxWait = 1 second)
      .takeWithin(10 seconds)
      .runWith(Sink.seq)
      .futureValue
    res shouldBe empty

    sqsClient.deleteQueue(queueUrl).futureValue
  }


  it should "send message and receive them as streams with retry mechanism" in {
    val newQueueReq = new CreateQueueRequest()
    newQueueReq.setAttributes(Map("VisibilityTimeout" -> 10.toString).asJava) // 10 seconds
    newQueueReq.setQueueName(testQueueName2)
    val queueUrl = sqsClient.createQueue(newQueueReq).futureValue.getQueueUrl

    val msgs = for (i <- 1 to 200) yield s"Message $i"
    val futSent = Source(msgs)
      .map { body =>
        val req = new SendMessageRequest()
        req.setMessageBody(body)
        req.setQueueUrl(queueUrl)
        req
      }
      .via(sqsClient.sendMessageAsStream())
      .take(200)
      .runWith(Sink.seq)
    val futReceived = sqsClient.receiveMessageAsStreamWithRetryExpBackoff(queueUrl, autoAck = true).take(200).runWith(Sink.seq)

    val (_, received) = futSent.zip(futReceived).futureValue

    received.map(_.getBody).sorted shouldEqual msgs.sorted

    // testing auto-ack (queue must be empty)
    val res = sqsClient
      .receiveMessageAsStream(queueUrl, longPollingMaxWait = 1 second)
      .takeWithin(10 seconds)
      .runWith(Sink.seq)
      .futureValue
    res shouldBe empty

    sqsClient.deleteQueue(queueUrl).futureValue
  }

}

