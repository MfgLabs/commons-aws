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

  val s3Client = AmazonS3Client.from(
    com.amazonaws.regions.Regions.EU_WEST_1,
    new com.amazonaws.auth.profile.ProfileCredentialsProvider("mfg")
  )().materialized(fm)

  it should "upload/list/delete small files" in {
    s3Client.deleteObjects(bucket, s"$keyPrefix").futureValue
    s3Client.putObject(bucket, s"$keyPrefix/small.txt", new java.io.File(getClass.getResource("/small.txt").getPath)).futureValue
    val l = s3Client.listFiles(bucket, Some(keyPrefix)).futureValue
    s3Client.deleteObjects(bucket, s"$keyPrefix/small.txt").futureValue
    val l2 = s3Client.listFiles(bucket, Some(keyPrefix)).futureValue

    (l map (_.getKey)) should equal(List(s"$keyPrefix/small.txt"))
    l2 should be('empty)
  }

  it should "throw an exception when a file is non-existent" in {
    val source = s3Client.getFileAsStream(bucket, "foo")
    intercept[AmazonS3Exception] {
      Await.result(source.runFold(ByteString.empty)(_ ++ _).map(_.compact), 5 seconds)
    }
  }

  it should "upload and download a big file as a single file" in {
    val futBytes = StreamConverters.fromInputStream(() => getClass.getResourceAsStream("/big.txt"))
      .via(s3Client.uploadStreamAsFile(bucket, s"$keyPrefix/big", chunkUploadConcurrency = 2))
      .flatMapConcat(_ => s3Client.getFileAsStream(bucket, s"$keyPrefix/big"))
      .runFold(ByteString.empty)(_ ++ _)
      .map(_.compact)

    val expectedBytes = StreamConverters.fromInputStream(() => getClass.getResourceAsStream("/big.txt"))
      .runFold(ByteString.empty)(_ ++ _).map(_.compact)

    whenReady(futBytes zip expectedBytes) { case (bytes, expectedBytes) =>
      bytes shouldEqual expectedBytes
    }
  }

  it should "download a big file and chunk it by line" in {
    val futLines = s3Client.getFileAsStream(bucket, s"$keyPrefix/big")
      .via(FlowExt.rechunkByteStringBySize(2 * 1024 * 1024))
      .via(Framing.delimiter(ByteString("\n"), 8 * 1024, allowTruncation = true))
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
      .via(s3Client.uploadStreamAsMultipartFile(bucket, s"$keyPrefix/big", nbChunkPerFile = 1, chunkUploadConcurrency = 2))
      .via(FlowExt.fold[CompleteMultipartUploadResult, Vector[CompleteMultipartUploadResult]](Vector.empty)(_ :+ _))
      .flatMapConcat(_ => s3Client.getMultipartFileAsStream(bucket, s"$keyPrefix/big.part"))
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
