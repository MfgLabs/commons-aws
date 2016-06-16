package com.mfglabs.commons.aws
package sqs

import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import com.amazonaws.services.sqs.model.{CreateQueueRequest, MessageAttributeValue, SendMessageRequest}
import com.mfglabs.stream.SinkExt
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.util.Random

class SQSSpec extends FlatSpec with Matchers with ScalaFutures {

  import scala.collection.JavaConversions._

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(60, Seconds), interval = Span(5, Millis))

  implicit val as = ActorSystem()
  implicit val fm = ActorMaterializer()

  val sqs = new AmazonSQSClient( scala.concurrent.ExecutionContext.Implicits.global)
  val builder = SQSStreamBuilder(sqs)

  val testQueueName = "commons-aws-sqs-test-" + Random.nextInt()
  val testQueueName2 = "commons-aws-sqs-test-" + Random.nextInt()

  val messageAttributeKey = "attribute_key"

  "SQS client" should "send message and receive them as streams" in {
    sqs.client.setRegion(Region.getRegion(Regions.EU_WEST_1))

    sqs.listQueues("commons-aws-sqs-test-").futureValue.headOption.foreach(queueUrl => sqs.deleteQueue(queueUrl).futureValue)
    val newQueueReq = new CreateQueueRequest()
    newQueueReq.setAttributes(Map("VisibilityTimeout" -> 10.toString)) // 10 seconds
    newQueueReq.setQueueName(testQueueName)
    val queueUrl = sqs.createQueue(newQueueReq).futureValue.getQueueUrl

    val msgs = for (i <- 1 to 200) yield s"Message $i"
    val futSent = Source(msgs)
      .map { body =>
      val req = new SendMessageRequest()
      req.addMessageAttributesEntry(messageAttributeKey, new MessageAttributeValue().withDataType("String").withStringValue(body))
      req.setMessageBody(body)
      req.setQueueUrl(queueUrl)
      req
    }
      .via(builder.sendMessageAsStream())
      .take(200)
      .runWith(SinkExt.collect)
    val futReceived = builder.receiveMessageAsStream(queueUrl, autoAck = true, messageAttributeNames = List(messageAttributeKey)).take(200).runWith(SinkExt.collect)

    val (sent, received) = futSent.zip(futReceived).futureValue

    received.map(_.getBody).sorted shouldEqual msgs.sorted
    received.map(_.getMessageAttributes.get(messageAttributeKey).getStringValue).sorted shouldEqual msgs.sorted

    // testing auto-ack (queue must be empty)
    val res = builder
      .receiveMessageAsStream(queueUrl, longPollingMaxWait = 1 second)
      .takeWithin(10 seconds)
      .runWith(SinkExt.collect)
      .futureValue
    res shouldBe empty

    sqs.deleteQueue(queueUrl).futureValue
  }


  "SQS client" should "send message and receive them as streams with retry mechanism" in {
    sqs.client.setRegion(Region.getRegion(Regions.EU_WEST_1))

    sqs.listQueues("commons-aws-sqs-test-").futureValue.headOption.foreach(queueUrl => sqs.deleteQueue(queueUrl).futureValue)
    val newQueueReq = new CreateQueueRequest()
    newQueueReq.setAttributes(Map("VisibilityTimeout" -> 10.toString)) // 10 seconds
    newQueueReq.setQueueName(testQueueName2)
    val queueUrl = sqs.createQueue(newQueueReq).futureValue.getQueueUrl

    val msgs = for (i <- 1 to 200) yield s"Message $i"
    val futSent = Source(msgs)
      .map { body =>
        val req = new SendMessageRequest()
        req.setMessageBody(body)
        req.setQueueUrl(queueUrl)
        req
      }
      .via(builder.sendMessageAsStream())
      .take(200)
      .runWith(SinkExt.collect)
    val futReceived = builder.receiveMessageAsStreamWithRetryExpBackoff(queueUrl, autoAck = true).take(200).runWith(SinkExt.collect)

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

