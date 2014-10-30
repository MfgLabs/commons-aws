package com.mfglabs.commons.aws

import collection.mutable.Stack
import org.scalatest._
import concurrent.ScalaFutures
import org.scalatest.time.{Minutes, Millis, Seconds, Span}
import scala.concurrent.Future
import play.api.libs.iteratee._

class S3Spec extends FlatSpec with Matchers with ScalaFutures {
  import s3._
  import scala.concurrent.ExecutionContext.Implicits.global

  val bucket = "mfg-commons-aws"

  val keyPrefix = "test/core"

  val resDir = "core/src/test/resources"

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(2, Minutes), interval = Span(5, Millis))

  // val cred = new com.amazonaws.auth.BasicAWSCredentials("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
  val S3 = new s3.AmazonS3Client()

  "S3 client" should "accept default constructor" in {
    whenReady(S3.getBucketLocation(bucket)) { s => s should equal ("eu-west-1") }
  }

  it should "upload/list/delete files" in {
    whenReady(
      for {
        _   <- S3.uploadFile(bucket, s"$keyPrefix/small.txt", new java.io.File(s"$resDir/small.txt"))
        l   <- S3.listFiles(bucket, Some(keyPrefix))
        _   <- S3.deleteFile(bucket, s"$keyPrefix/small.txt")
        l2  <- S3.listFiles(bucket, Some(keyPrefix))
      } yield (l, l2)
    ) { case (l, l2) =>
      (l map (_._1)) should equal (List(s"$keyPrefix/small.txt"))
      l2 should be ('empty)
    }
  }

  it should "upstream files" in {
    whenReady(
      for {
        _   <- S3.uploadStream(bucket, s"$keyPrefix/big.txt", Enumerator.fromFile(new java.io.File(s"$resDir/big.txt")))
        l   <- S3.listFiles(bucket, Some(keyPrefix))
        _   <- S3.deleteFile(bucket, s"$keyPrefix/big.txt")
        l2  <- S3.listFiles(bucket, Some(keyPrefix))
      } yield (l, l2)
    ) { case (l, l2) =>
      (l map (_._1)) should equal (List(s"$keyPrefix/big.txt"))
      l2 should be ('empty)
    }
  }

  it should "download a file as a stream" in {
    whenReady(
      for {
        initContent <- Enumerator.fromFile(new java.io.File(s"$resDir/big.txt")) |>>> Iteratee.consume()
        _ <- S3.uploadStream(bucket, s"$keyPrefix/big.txt", Enumerator.fromFile(new java.io.File(s"$resDir/big.txt")))
        downloadContent <- S3.getStream(bucket, s"$keyPrefix/big.txt") |>>> Iteratee.consume()
        _ <- S3.deleteFile(bucket, s"$keyPrefix/big.txt")
      } yield (initContent, downloadContent)
    ) { case (initContent, downloadContent) =>
      initContent === downloadContent
    }
  }

  it should "download a multipart file as a stream" in {
    whenReady(
      for {
        _ <- S3.uploadStream(bucket, s"$keyPrefix/part.1.txt", Enumerator.fromFile(new java.io.File(s"$resDir/part.1.txt")))
        _ <- S3.uploadStream(bucket, s"$keyPrefix/part.2.txt", Enumerator.fromFile(new java.io.File(s"$resDir/part.2.txt")))
        downloadContent <- S3.getStreamMultipartFile(bucket, s"$keyPrefix/part") |>>> Iteratee.consume()
        _ <- S3.deleteFile(bucket, s"$keyPrefix/part.1.txt")
        _ <- S3.deleteFile(bucket, s"$keyPrefix/part.2.txt")
      } yield downloadContent
    ) { downloadContent =>
      new String(downloadContent) === "part1\npart2\n"
    }
  }
}
