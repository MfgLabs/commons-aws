package sqs


import java.io.InputStream
import java.util.concurrent.ExecutorService

import akka.actor.Status.Failure
import akka.actor.{Props, ActorLogging, Stash}
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Cancel
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.scaladsl.Source
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.sqs.{AmazonSQSAsyncClient, AmazonSQSClient}
import com.amazonaws.services.sqs.model.{ReceiveMessageRequest, Message}
import com.mfglabs.commons.stream.MFGSource
import scala.annotation.tailrec
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConversions._


object MFGSQS {

  def receiveAndEmit(sqsC : AmazonSQSClient,sqsUrl:String,executor: ExecutionContext)(counter: Long, demand: Int) : Future[(Seq[Message],Boolean)] = {
    val rcvMsgReq = new ReceiveMessageRequest(sqsUrl)
    rcvMsgReq.setMaxNumberOfMessages(demand)
    Future{(sqsC.receiveMessage(rcvMsgReq).getMessages().toList,false)}(executor) //TODO accepter un executionContext en parametre
  }

  def source(sqsC : AmazonSQSClient,sqsUrl : String)(implicit executor: ExecutionContext) : Source[Message] =
    MFGSource.bulkPullerAsync[Message](0)(receiveAndEmit(sqsC,sqsUrl,executor))

}
