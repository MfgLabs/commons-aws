package com.mfglabs.commons.aws
package sqs

import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import com.amazonaws.services.sqs.{AmazonSQSAsyncClient, AmazonSQSClient}
import com.amazonaws.services.sqs.model._
import com.pellucid.wrap.sqs.AmazonSQSScalaClient
import scala.concurrent._
import scala.concurrent.duration._

import com.mfglabs.stream._

import scala.concurrent.duration.FiniteDuration

trait SQSStreamBuilder {
  import scala.collection.JavaConversions._

  val sqs: AmazonSQSScalaClient

  import sqs.execCtx

  def sendMessageAsStream: Flow[SendMessageRequest, SendMessageResult] = {
    // note: SQS does not guarantee ordering with high-throughput,
    // so using FlowExt.mapAsyncWithOrderedSideEffect to try to guarantee ordering is useless
    Flow[SendMessageRequest].mapAsync { msg =>
      sqs.sendMessage(msg)
    }
  }

  def receiveMessageAsStream(queueUrl: String, longPollingMaxWait: FiniteDuration = 20 seconds, autoAck: Boolean = false): Source[Message] = {
    val source = SourceExt.bulkPullerAsync(0L) { (total, currentDemand) =>
      val msg = new ReceiveMessageRequest(queueUrl)
      msg.setWaitTimeSeconds(longPollingMaxWait.toSeconds.toInt) // > 0 seconds allow long-polling. 20 seconds is the maximum
      msg.setMaxNumberOfMessages(Math.min(currentDemand, 10)) // 10 is SQS limit

      sqs.receiveMessage(msg).map(res => (res.getMessages.toSeq, false))
    }

    if (autoAck)
      source.mapAsync { msg =>
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
