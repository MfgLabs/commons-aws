package com.mfglabs.commons.aws
package s3

import java.io.File
import java.nio.charset.Charset

import akka.actor.ActorSystem
import akka.stream.{FlattenStrategy, FlowMaterializer}
import akka.stream.scaladsl.{FoldSink, Flow, Source}
import akka.util.ByteString
import com.amazonaws.services.s3.model.{CompleteMultipartUploadResult, DeleteObjectsRequest}
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
    PatienceConfig(timeout = Span(3, Minutes), interval = Span(20, Millis))

  // val cred = new com.amazonaws.auth.BasicAWSCredentials("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
  val S3 = new s3.AmazonS3Client()

  import S3.ecForBlockingOps
  import S3.fm

  "S3 client" should "accept default constructor" in {
    whenReady(S3.getBucketLocation(bucket)) { s => s should equal("eu-west-1")}
  }

  it should "upload/list/delete small files" in {
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

  it should "upload and download a big file as a single file" in {
    val futBytes = MFGSource
      .fromFile(new java.io.File(s"$resDir/big.txt"))
      .via(S3.uploadStreamAsFile(bucket, s"$keyPrefix/big", chunkUploadConcurrency = 2))
      .map(_ => S3.getFileAsStream(bucket, s"$keyPrefix/big"))
      .flatten(FlattenStrategy.concat)
      .runFold(ByteString.empty)(_ ++ _)
      .map(_.compact)

    val expectedBytes = MFGSource.fromFile(new java.io.File(s"$resDir/big.txt")).runFold(ByteString.empty)(_ ++ _).map(_.compact)

    whenReady(futBytes zip expectedBytes) { case (bytes, expectedBytes) =>
      bytes shouldEqual expectedBytes
    }
  }

  it should "download a big file and chunk it by line" in {
    val futLines = S3.getFileAsStream(bucket, s"$keyPrefix/big")
      .via(MFGFlow.rechunkByteString(2 * 1024 * 1024))
      .via(MFGFlow.byteStringToLines(separator = "\n"))
      .runWith(MFGSink.collect)

    val futExpectedLines =
      MFGSource.fromFile(new File(s"$resDir/big.txt"))
        .runFold(ByteString.empty)(_ ++ _)
        .map(_.compact.utf8String)
        .map(_.split("\n").to[scala.collection.immutable.Seq])

    whenReady(futLines zip futExpectedLines) { case (lines, expectedLines) =>
      lines shouldEqual expectedLines
    }
  }


  it should "upload and download a big file as a multipart file" in {
    val futBytes = MFGSource
      .fromFile(new java.io.File(s"$resDir/big.txt"), maxChunkSize = 2 * 1024 * 1024)
      .via(S3.uploadStreamAsMultipartFile(bucket, s"$keyPrefix/big", nbChunkPerFile = 1, chunkUploadConcurrency = 2))
      .via(MFGFlow.fold[CompleteMultipartUploadResult, Vector[CompleteMultipartUploadResult]](Vector.empty)(_ :+ _))
      .map(_ => S3.getMultipartFileAsStream(bucket, s"$keyPrefix/big.part"))
      .flatten(FlattenStrategy.concat)
      .runFold(ByteString.empty)(_ ++ _)
      .map(_.compact)

   val futExpectedBytes = MFGSource.fromFile(new java.io.File(s"$resDir/big.txt")).runFold(ByteString.empty)(_ ++ _).map(_.compact)

    whenReady(futBytes zip futExpectedBytes) { case (bytes, expectedBytes) =>
      bytes shouldEqual expectedBytes
    }
  }

}
