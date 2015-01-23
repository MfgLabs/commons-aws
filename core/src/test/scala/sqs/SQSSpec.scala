package com.mfglabs.commons.aws

import akka.actor.ActorSystem
import akka.stream.FlowMaterializer
import akka.stream.scaladsl.Flow
import com.amazonaws.auth.{BasicAWSCredentials, AWSCredentials, AWSCredentialsProvider}
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.{Regions, Region}
import com.amazonaws.services.sqs.AmazonSQSClient
import com.amazonaws.services.sqs.model.{Message, CreateQueueRequest}
import com.mfglabs.commons.stream.MFGSink
import org.apache.http.impl.client.BasicCredentialsProvider
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Millis, Minutes, Span}
import org.scalatest.{Matchers, FlatSpec}
import sqs.MFGSQS
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.ExecutionContext
import scala.util.Random
import scala.collection.JavaConversions._
/**
 * Created by damien on 21/01/15.
 */
class SQSSpec extends FlatSpec with Matchers with ScalaFutures {

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(5, Millis))


  implicit val as = ActorSystem()
  implicit val fm = FlowMaterializer()

  "SQS client" should "read data from a SQS queue" in {
    val nbMessages = 100
    val messages = Stream.fill(nbMessages)(Stream.continually(util.Random.nextPrintableChar).take(5).mkString).toList
    val sqsName = "commons_aws_test_" + Random.nextInt()
    val sqsUrl = "https://sqs.eu-west-1.amazonaws.com/896733075612/" + sqsName
    val damienCreds = new BasicAWSCredentials("AKIAIMEROIY4REJGDCJA", "jRA6V6ZMaPt+oxPzU696uRTqYY+Aj//wKAT+aA/v") //TODO remove with dedicated IAM user

    val sqsC = new AmazonSQSClient(damienCreds)
    sqsC.setRegion(Region.getRegion(Regions.EU_WEST_1))
    if (sqsC.listQueues().getQueueUrls.find(_.equals(sqsUrl)).isDefined) {
      println(s"queue $sqsUrl exists, deleting")
      sqsC.deleteQueue(sqsUrl)
    }
    val createRes = sqsC.createQueue(sqsName)
    println("created queue " + createRes.getQueueUrl)
    messages.foreach( m => sqsC.sendMessage(sqsUrl,m))
    whenReady(
      MFGSQS.source(sqsC, sqsUrl).via(Flow[Message].take(nbMessages)).runWith(MFGSink.collect)
    ){ res =>
      res.map(msg => sqsC.deleteMessage(sqsUrl,msg.getReceiptHandle))
      sqsC.deleteQueue(sqsUrl)
      res.map(_.getBody).toSet shouldEqual messages.toSet
    }
  }
}

