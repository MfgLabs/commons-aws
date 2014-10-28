package com.mfglabs.commons.aws

import collection.mutable.Stack
import org.scalatest._
import concurrent.ScalaFutures
import org.scalatest.time.{ Millis, Seconds, Span }
import scala.concurrent.Future
import play.api.libs.iteratee._

class S3Spec extends FlatSpec with Matchers with ScalaFutures {
  import s3._
  import scala.concurrent.ExecutionContext.Implicits.global

  val bucket = "mfg-commons-aws"

  val keyPrefix = "test"

  val resDir = "core/src/test/resources"

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(5, Millis))

  // val cred = new com.amazonaws.auth.BasicAWSCredentials("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
  val S3 = new s3.AmazonS3Client()

  "S3 client" should "accept default constructor" in {
    whenReady(S3.getBucketLocation(bucket)) { s => s should equal ("eu-west-1") }
  }

  it should "upload/list/delete files" in {
    whenReady(
      for {
        _   <- S3.uploadFile(bucket, "small.txt", new java.io.File(s"$resDir/small.txt"))
        l   <- S3.listFiles(bucket)
        _   <- S3.deleteFile(bucket, "small.txt")
        l2  <- S3.listFiles(bucket)
      } yield (l, l2)
    ) { case (l, l2) =>
      (l map (_._1)) should equal (List("small.txt"))
      l2 should be ('empty)
    }
  }

  it should "upstream files" in {
    whenReady(
      for {
        _   <- S3.uploadStream(bucket, "big.txt", Enumerator.fromFile(new java.io.File(s"$resDir/big.txt")))
        l   <- S3.listFiles(bucket)
        _   <- S3.deleteFile(bucket, "big.txt")
        l2  <- S3.listFiles(bucket)
      } yield (l, l2)
    ) { case (l, l2) =>
      (l map (_._1)) should equal (List("big.txt"))
      l2 should be ('empty)
    }
  }
}
