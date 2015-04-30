package com.mfglabs.commons.aws
package s3

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import com.amazonaws.services.s3.model.{CompleteMultipartUploadResult, DeleteObjectsRequest}
import com.mfglabs.stream._
import org.scalatest._
import concurrent.ScalaFutures
import org.scalatest.time.{Minutes, Millis, Seconds, Span}

class S3Spec extends FlatSpec with Matchers with ScalaFutures {
  import s3._
  import scala.concurrent.ExecutionContext.Implicits.global

  val bucket = "mfg-commons-aws"
  val keyPrefix = "test/core"
  val multipartkeyPrefix = "test/multipart-upload-core"

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(3, Minutes), interval = Span(20, Millis))

  implicit val system = ActorSystem()
  implicit val fm = ActorFlowMaterializer()

  val streamBuilder = S3StreamBuilder(new s3.AmazonS3AsyncClient())
  val ops = new streamBuilder.MaterializedOps(fm)

  import streamBuilder.ecForBlockingOps

  it should "upload/list/delete small files" in {
    whenReady(
      for {
        _ <- ops.deleteFiles(bucket, s"$keyPrefix")
        _ <- ops.uploadFile(bucket, s"$keyPrefix/small.txt", new java.io.File(getClass.getResource("/small.txt").getPath))
        l <- ops.listFiles(bucket, Some(keyPrefix))
        _ <- ops.deleteFile(bucket, s"$keyPrefix/small.txt")
        l2 <- ops.listFiles(bucket, Some(keyPrefix))
      } yield (l, l2)
    ) { case (l, l2) =>
      (l map (_._1)) should equal(List(s"$keyPrefix/small.txt"))
      l2 should be('empty)
    }
  }

  it should "upload and download a big file as a single file" in {
    val futBytes = SourceExt
      .fromFile(new java.io.File(getClass.getResource("/big.txt").getPath))
      .via(streamBuilder.uploadStreamAsFile(bucket, s"$keyPrefix/big", chunkUploadConcurrency = 2))
      .map(_ => streamBuilder.getFileAsStream(bucket, s"$keyPrefix/big"))
      .flatten(FlattenStrategy.concat)
      .runFold(ByteString.empty)(_ ++ _)
      .map(_.compact)

    val expectedBytes = SourceExt.fromFile(new java.io.File(getClass.getResource("/big.txt").getPath))
      .runFold(ByteString.empty)(_ ++ _).map(_.compact)

    whenReady(futBytes zip expectedBytes) { case (bytes, expectedBytes) =>
      bytes shouldEqual expectedBytes
    }
  }

  it should "download a big file and chunk it by line" in {
    val futLines = streamBuilder.getFileAsStream(bucket, s"$keyPrefix/big")
      .via(FlowExt.rechunkByteStringBySize(2 * 1024 * 1024))
      .via(FlowExt.rechunkByteStringBySeparator(ByteString("\n"), 8 * 1024))
      .map(_.utf8String)
      .runWith(SinkExt.collect)

    val futExpectedLines =
      SourceExt.fromFile(new java.io.File(getClass.getResource("/big.txt").getPath))
        .runFold(ByteString.empty)(_ ++ _)
        .map(_.compact.utf8String)
        .map(_.split("\n").to[scala.collection.immutable.Seq])

    whenReady(futLines zip futExpectedLines) { case (lines, expectedLines) =>
      lines shouldEqual expectedLines
    }
  }


  it should "upload and download a big file as a multipart file" in {
    val futBytes = SourceExt
      .fromFile(new java.io.File(getClass.getResource("/big.txt").getPath), maxChunkSize = 2 * 1024 * 1024)
      .via(streamBuilder.uploadStreamAsMultipartFile(bucket, s"$keyPrefix/big", nbChunkPerFile = 1, chunkUploadConcurrency = 2))
      .via(FlowExt.fold[CompleteMultipartUploadResult, Vector[CompleteMultipartUploadResult]](Vector.empty)(_ :+ _))
      .map(_ => streamBuilder.getMultipartFileAsStream(bucket, s"$keyPrefix/big.part"))
      .flatten(FlattenStrategy.concat)
      .runFold(ByteString.empty)(_ ++ _)
      .map(_.compact)

   val futExpectedBytes = SourceExt.fromFile(new java.io.File(getClass.getResource("/big.txt").getPath))
     .runFold(ByteString.empty)(_ ++ _).map(_.compact)

    whenReady(futBytes zip futExpectedBytes) { case (bytes, expectedBytes) =>
      bytes shouldEqual expectedBytes
    }
  }

}
