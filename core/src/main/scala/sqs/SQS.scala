package com.mfglabs.commons.aws
package sqs

import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import com.amazonaws.services.sqs.{AmazonSQSAsyncClient, AmazonSQSClient}
import com.amazonaws.services.sqs.model._
import com.github.dwhjames.awswrap.sqs.AmazonSQSScalaClient
import scala.concurrent._
import scala.concurrent.duration._

import com.mfglabs.stream._

import scala.concurrent.duration.FiniteDuration

trait SQSStreamBuilder {

  import scala.collection.JavaConversions._

  val sqs: AmazonSQSScalaClient

  import sqs.execCtx

  val defaultMessageOpsConcurrency = 16

  /**
   * Send SQS messages as a stream
   * Important note: SQS does not ensure message ordering, so setting messageSendingConcurrency parameter equal to 1 does not
   * guarantee total message ordering.
   * @param messageSendingConcurrency
   */
  def sendMessageAsStream(messageSendingConcurrency: Int = defaultMessageOpsConcurrency): Flow[SendMessageRequest, SendMessageResult, Unit] = {
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

}

object SQSStreamBuilder {
  def apply(sqsClient: AmazonSQSScalaClient) = new SQSStreamBuilder {
    override val sqs: AmazonSQSScalaClient = sqsClient
  }
}
