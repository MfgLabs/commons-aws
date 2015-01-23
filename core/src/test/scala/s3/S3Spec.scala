package com.mfglabs.commons.aws

import java.nio.charset.Charset

import akka.actor.ActorSystem
import akka.stream.FlowMaterializer
import akka.stream.scaladsl.{FoldSink, Flow, Source}
import com.amazonaws.services.s3.model.DeleteObjectsRequest
import com.mfglabs.commons.stream.MFGSink
import com.mfglabs.commons.stream.{MFGFlow, MFGSource}

import collection.mutable.Stack
import org.scalatest._
import concurrent.ScalaFutures
import org.scalatest.time.{Minutes, Millis, Seconds, Span}
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.collection.JavaConversions._

class S3Spec extends FlatSpec with Matchers with ScalaFutures {

  import s3._
  import scala.concurrent.ExecutionContext.Implicits.global

  val bucket = "mfg-commons-aws"

  val keyPrefix = "test/core"
  val multipartkeyPrefix = "test/multipart-upload-core"
  val resDir = "core/src/test/resources"

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(2, Minutes), interval = Span(5, Millis))

  implicit val as = ActorSystem("test")
  implicit val fm = FlowMaterializer()
  // val cred = new com.amazonaws.auth.BasicAWSCredentials("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
  val S3 = new s3.AmazonS3Client()

  "S3 client" should "accept default constructor" in {
    whenReady(S3.getBucketLocation(bucket)) { s => s should equal("eu-west-1")}
  }

  it should "upload/list/delete files" in {
    whenReady(
      for {
        _ <- S3.deleteFiles(bucket, s"$keyPrefix")
        _ <- S3.uploadFile(bucket, s"$keyPrefix/small.txt", new java.io.File(s"$resDir/small.txt"))
        l <- S3.listFiles(bucket, Some(keyPrefix))
        _ <- S3.deleteFile(bucket, s"$keyPrefix/small.txt")
        l2 <- S3.listFiles(bucket, Some(keyPrefix))
      } yield (l, l2)
    ) { case (l, l2) =>
      (l map (_._1)) should equal(List(s"$keyPrefix/small.txt"))
      l2 should be('empty)
    }
  }

  it should "upstream files" in {
    whenReady(
      for {
        _ <- S3.uploadStream(bucket, s"$keyPrefix/big.txt", MFGSource.fromFile(new java.io.File(s"$resDir/big.txt")), 1)
        l <- S3.listFiles(bucket, Some(keyPrefix))
        _ <- S3.deleteFile(bucket, s"$keyPrefix/big.txt")
        l2 <- S3.listFiles(bucket, Some(keyPrefix))
      } yield (l, l2)
    ) { case (l, l2) =>
      (l map (_._1)) should equal(List(s"$keyPrefix/big.txt"))
      l2 should be('empty)
    }
  }

  it should "download a file as a stream" in {
    List("medium.txt", "big.txt").map { file =>
      whenReady(
        for {
          _ <- S3.deleteFile(bucket, s"$keyPrefix/$file")
          initContent <- MFGSource.fromFile(new java.io.File(s"$resDir/$file")).runWith(MFGSink.collect)
          _ <- S3.uploadStream(bucket, s"$keyPrefix/$file", MFGSource.fromFile(new java.io.File(s"$resDir/$file")))
          downloadContent <- S3.getStream(bucket, s"$keyPrefix/$file").runWith(MFGSink.collect)
          _ <- S3.deleteFile(bucket, s"$keyPrefix/$file")
        } yield (initContent, downloadContent)
      ) { case (initContent, downloadContent) =>
        initContent.map(_.to[List]).reduce(_ ++ _) shouldEqual (downloadContent.map(_.to[List])).reduce(_ ++ _)

      }
    }
  }

  it should "download a multipart file as a stream" in {
    whenReady(
      for {
        _ <- S3.uploadStream(bucket, s"$keyPrefix/part.1.txt", MFGSource.fromFile(new java.io.File(s"$resDir/part.1.txt")))
        _ <- S3.uploadStream(bucket, s"$keyPrefix/part.2.txt", MFGSource.fromFile(new java.io.File(s"$resDir/part.2.txt")))
        downloadContent <- S3.getStreamMultipartFile(bucket, s"$keyPrefix/part").via(MFGFlow.byteArrayToString(Charset.forName("UTF-8"))).runWith(MFGSink.collect)
        downloadContentByLine <- S3.getStreamMultipartFileByLine(bucket, s"$keyPrefix/part").runWith(MFGSink.collect)
        _ <- S3.deleteFile(bucket, s"$keyPrefix/part.1.txt")
        _ <- S3.deleteFile(bucket, s"$keyPrefix/part.2.txt")
      } yield (downloadContent, downloadContentByLine)
    ) { case (downloadContent, downloadContentByLine) =>
      downloadContentByLine shouldEqual List("part1", "part12", "part2", "part22")
      downloadContent shouldEqual List("part1", "part12part2", "part22")
    }
  }


  it should "upload a stream as a multipart file" in {
    val tickSource =
      Source(initialDelay = 0 second, interval = 2 second, () => "tick".toCharArray.map(_.toByte))
        .takeWithin(10 seconds)

    whenReady(
      for {
        listObj <- S3.deleteFiles(bucket, multipartkeyPrefix + "/multipart-upload")
        nbSent <- tickSource
          .via(S3.uploadStreamMultipartFile(bucket, multipartkeyPrefix + "/multipart-upload", 10, 5 seconds))
          .runWith(FoldSink[Int, Int](0) { (z, c) => z + 1})
        nbFiles <- S3.listObjects(bucket, multipartkeyPrefix + "/multipart-upload")
        _ <- S3.deleteFiles(bucket, multipartkeyPrefix + "/multipart-upload")
      } yield (nbSent, nbFiles)
    ) {
      case (nbSent, nbFiles) =>
        nbSent shouldEqual (2)
        nbFiles.getObjectSummaries.size shouldEqual (2)
    }
  }
}
