package com.mfglabs.commons.aws
package s3

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import com.amazonaws.services.s3.model.{AmazonS3Exception, CompleteMultipartUploadResult}
import com.mfglabs.stream._
import org.scalatest._
import concurrent.ScalaFutures
import org.scalatest.time.{Minutes, Millis, Span}

import scala.concurrent._
import scala.concurrent.duration._

class S3Spec extends FlatSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
  import s3._
  import scala.concurrent.ExecutionContext.Implicits.global

  val bucket = "mfg-commons-aws"
  val keyPrefix = "test/core"
  val multipartkeyPrefix = "test/multipart-upload-core"

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(3, Minutes), interval = Span(20, Millis))

  implicit val system = ActorSystem()
  implicit val fm = ActorMaterializer()

  val s3Client = s3.AmazonS3AsyncClient(
    new com.amazonaws.auth.profile.ProfileCredentialsProvider("mfg")
  )()

  val streamBuilder = S3StreamBuilder(s3Client)
  val ops = new streamBuilder.MaterializedOps(fm)

  it should "upload/list/delete small files" in {
    ops.deleteObjects(bucket, s"$keyPrefix").futureValue
    ops.putObject(bucket, s"$keyPrefix/small.txt", new java.io.File(getClass.getResource("/small.txt").getPath)).futureValue
    val l = ops.listFiles(bucket, Some(keyPrefix)).futureValue
    ops.deleteObjects(bucket, s"$keyPrefix/small.txt").futureValue
    val l2 = ops.listFiles(bucket, Some(keyPrefix)).futureValue

    (l map (_._1)) should equal(List(s"$keyPrefix/small.txt"))
    l2 should be('empty)
  }

  it should "throw an exception when a file is non-existent" in {
    val source = streamBuilder.getFileAsStream(bucket, "foo")
    intercept[AmazonS3Exception] {
      Await.result(source.runFold(ByteString.empty)(_ ++ _).map(_.compact), 5 seconds)
    }
  }

  it should "upload and download a big file as a single file" in {
    val futBytes = StreamConverters.fromInputStream(() => getClass.getResourceAsStream("/big.txt"))
      .via(streamBuilder.uploadStreamAsFile(bucket, s"$keyPrefix/big", chunkUploadConcurrency = 2))
      .flatMapConcat(_ => streamBuilder.getFileAsStream(bucket, s"$keyPrefix/big"))
      .runFold(ByteString.empty)(_ ++ _)
      .map(_.compact)

    val expectedBytes = StreamConverters.fromInputStream(() => getClass.getResourceAsStream("/big.txt"))
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
      .runWith(Sink.seq)

    val futExpectedLines = StreamConverters
        .fromInputStream(() => getClass.getResourceAsStream("/big.txt"))
        .runFold(ByteString.empty)(_ ++ _)
        .map(_.compact.utf8String)
        .map(_.split("\n").to[scala.collection.immutable.Seq])

    whenReady(futLines zip futExpectedLines) { case (lines, expectedLines) =>
      lines shouldEqual expectedLines
    }
  }


  it should "upload and download a big file as a multipart file" in {
    val bytes = StreamConverters
      .fromInputStream(() => getClass.getResourceAsStream("/big.txt"), chunkSize = 2 * 1024 * 1024)
      .via(streamBuilder.uploadStreamAsMultipartFile(bucket, s"$keyPrefix/big", nbChunkPerFile = 1, chunkUploadConcurrency = 2))
      .via(FlowExt.fold[CompleteMultipartUploadResult, Vector[CompleteMultipartUploadResult]](Vector.empty)(_ :+ _))
      .flatMapConcat(_ => streamBuilder.getMultipartFileAsStream(bucket, s"$keyPrefix/big.part"))
      .runFold(ByteString.empty)(_ ++ _)
      .map(_.compact).futureValue

   val expectedBytes = StreamConverters
      .fromInputStream(() => getClass.getResourceAsStream("/big.txt"))
      .runFold(ByteString.empty)(_ ++ _).map(_.compact).futureValue

    bytes shouldEqual expectedBytes
  }


  override def afterAll() = {
    s3Client.shutdown()
    val _ = system.terminate().futureValue
  }
}
