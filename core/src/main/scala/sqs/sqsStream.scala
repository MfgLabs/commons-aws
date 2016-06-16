package com.mfglabs.commons.aws
package sqs

import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import com.amazonaws.services.sqs.model._
import scala.concurrent._
import scala.concurrent.duration._

import com.mfglabs.stream._

import scala.concurrent.duration.FiniteDuration

trait SQSStreamBuilder {

  import scala.collection.JavaConversions._

  val sqs: AmazonSQSClient

  import sqs.execCtx

  val defaultMessageOpsConcurrency = 16

  /**
   * Send SQS messages as a stream
   * Important note: SQS does not ensure message ordering, so setting messageSendingConcurrency parameter equal to 1 does not
   * guarantee total message ordering.
   * @param messageSendingConcurrency
   */
  def sendMessageAsStream(messageSendingConcurrency: Int = defaultMessageOpsConcurrency): Flow[SendMessageRequest, SendMessageResult, akka.NotUsed] = {
    Flow[SendMessageRequest].mapAsync(messageSendingConcurrency) { msg =>
      sqs.sendMessage(msg)
    }
  }

  /**
   * Receive messages from a SQS queue as a stream.
   * @param queueUrl SQS queue url
   * @param longPollingMaxWait SQS long-polling parameter.
   * @param autoAck If true, the SQS messages will be automatically ack once they are received. If false, you must call
   *                sqs.deleteMessage yourself when you want to ack the message.
   * @param messageAttributeNames the message attribute names asked to be returned
   */
  def receiveMessageAsStream(queueUrl: String, messageAckingConcurrency: Int = defaultMessageOpsConcurrency,
                             longPollingMaxWait: FiniteDuration = 20 seconds, autoAck: Boolean = false,
                             messageAttributeNames: Seq[String] = Seq.empty): Source[Message, ActorRef] = {
    val source = SourceExt.bulkPullerAsync(0L) { (total, currentDemand) =>
      val msg = new ReceiveMessageRequest(queueUrl)
      msg.setWaitTimeSeconds(longPollingMaxWait.toSeconds.toInt) // > 0 seconds allow long-polling. 20 seconds is the maximum
      msg.setMaxNumberOfMessages(Math.min(currentDemand, 10)) // 10 is SQS limit
      msg.setMessageAttributeNames(messageAttributeNames)
      sqs.receiveMessage(msg).map(res => (res.getMessages.toSeq, false))
    }

    if (autoAck)
      source.mapAsync(messageAckingConcurrency) { msg =>
        sqs.deleteMessage(queueUrl, msg.getReceiptHandle).map(_ => msg)
      }
    else source
  }


  /**
   * Receive messages from a SQS queue as a stream and if connection closes,
   * retry using an exponential backoff reconnection strategy (2^retryNb * retryMinInterval).
   *
   * @param queueUrl SQS queue url
   * @param maxRetryDuration maximum retry duration.
   * @param retryMinInterval minimum delay before retrying.
   * @param longPollingMaxWait SQS long-polling parameter.
   * @param autoAck If true, the SQS messages will be automatically ack once they are received. If false, you must call
   *                sqs.deleteMessage yourself when you want to ack the message.
   * @param messageAttributeNames the message attribute names asked to be returned
   */
  def receiveMessageAsStreamWithRetryExpBackoff(
      queueUrl: String, messageAckingConcurrency: Int = defaultMessageOpsConcurrency,
      maxRetryDuration: FiniteDuration = 540.seconds, retryMinInterval: FiniteDuration = 1.second,
      longPollingMaxWait: FiniteDuration = 20 seconds, autoAck: Boolean = false,
      messageAttributeNames: Seq[String] = Seq.empty): Source[Message, ActorRef] = {
    val source = SourceExt.bulkPullerAsyncWithErrorExpBackoff(0L, maxRetryDuration, retryMinInterval) { (total, currentDemand) =>
      val msg = new ReceiveMessageRequest(queueUrl)
      msg.setWaitTimeSeconds(longPollingMaxWait.toSeconds.toInt) // > 0 seconds allow long-polling. 20 seconds is the maximum
      msg.setMaxNumberOfMessages(Math.min(currentDemand, 10)) // 10 is SQS limit
      msg.setMessageAttributeNames(messageAttributeNames)
      sqs.receiveMessage(msg).map(res => (res.getMessages.toSeq, false))
    }

    if (autoAck)
      source.mapAsync(messageAckingConcurrency) { msg =>
        sqs.deleteMessage(queueUrl, msg.getReceiptHandle).map(_ => msg)
      }
    else source
  }
}

object SQSStreamBuilder {
  def apply(sqsClient: AmazonSQSClient) = new SQSStreamBuilder {
    override val sqs: AmazonSQSClient = sqsClient
  }
}
