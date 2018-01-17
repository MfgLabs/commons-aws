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


  it should "upload and download a big file as a single file" in {
    val futBytes = StreamConverters.fromInputStream(() => getClass.getResourceAsStream("/big.txt"))
      .via(s3Client.uploadStreamAsFile(bucket, s"$keyPrefix/big", chunkUploadConcurrency = 2))
      .flatMapConcat(_ => s3Client.getFileAsStream(bucket, s"$keyPrefix/big"))
      .runFold(ByteString.empty)(_ ++ _)
      .map(_.compact)

    whenReady(futBytes) { case _ => () }

    val expectedBytes = StreamConverters.fromInputStream(() => getClass.getResourceAsStream("/big.txt"))
      .runFold(ByteString.empty)(_ ++ _).map(_.compact)

    whenReady(futBytes zip expectedBytes) { case (bytes, expectedBytes) =>
      bytes shouldEqual expectedBytes
    }
  }

  override def afterAll() = {
    s3Client.shutdown()
    val _ = system.terminate().futureValue
  }
}
