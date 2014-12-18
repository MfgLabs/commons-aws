package com.mfglabs.commons.aws

import java.sql.Connection
import java.util.zip.GZIPInputStream

import com.amazonaws.ClientConfiguration
import com.mfglabs.commons.aws.`s3`._
import com.mfglabs.commons.aws.commons.DockerTmpDB
import com.mfglabs.commons.aws.extensions.postgres.PostgresExtensions
import com.mfglabs.commons.aws.extensions.postgres.PostgresExtensions.{Table, PGCopyable}
import com.mfglabs.commons.aws.s3.AmazonS3Client
import org.postgresql.PGConnection
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Minutes, Span, Millis}
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import play.api.libs.iteratee.Iteratee
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.Source
import com.mfglabs.commons.aws._
import scala.concurrent.ExecutionContext.Implicits.global
/**
 * Created by damien on 14/11/14.
 */
class CopyToS3Spec extends FlatSpec with Matchers with ScalaFutures with DockerTmpDB {
  import s3._
  import extensions.postgres._
  import scala.concurrent.ExecutionContext.Implicits.global

  val bucket = "mfg-commons-aws"

  val keyPrefix = "test/extensions/postgres_copy2s3/"
  //val ClientConfiguration
  val s3c = new s3.AmazonS3Client()
  val pgExt = new PostgresExtensions(s3c)

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(5, Minutes), interval = Span(5, Millis))

  it should "copy a table to S3 as flat file" in {
    val stmt = conn.createStatement()
    stmt.execute("CREATE TABLE test_copy_to_s3(id bigint, mot text);")
    stmt.execute("INSERT INTO test_copy_to_s3 (id, mot) VALUES (1, 'veau'),(2, 'vache'),(3, 'cochon');")
    val fileContent = "\n1;veau\n2;vache\n3;cochon"
    val flatS3ObjFut =
      pgExt.copyToS3AsFlatFile(Table("public", "test_copy_to_s3"), ";", bucket, keyPrefix + "flat.tsv")(conn.asInstanceOf[PGConnection])
        .map { _ =>
        s3c.getStream(bucket, keyPrefix + "flat.tsv") |>>> Iteratee.fold("") { (z, x) => z + "\n" + x}
      }
    flatS3ObjFut.futureValue === "\n1;veau\n2;vache\n3;cochon"

    val gzipS3ObjFut =
      pgExt.copyToS3AsGzip(Table("public", "test_copy_to_s3"), ";", bucket, keyPrefix + "flat.tsv.gz")(conn.asInstanceOf[PGConnection])
        .map { _ =>
        s3c.getTransformedStream(bucket, keyPrefix + "flat.tsv.gz", is => {
          new GZIPInputStream(is)
        }) |>>> Iteratee.fold("") { (z, x) => z + "\n" + x}
      }

    gzipS3ObjFut.futureValue === List("1;veau","2;vache","3;cochon")
    Await.result(s3c.deleteObject(bucket, keyPrefix + "flat.tsv"),10 seconds)
    Await.result(s3c.deleteObject(bucket, keyPrefix + "flat.tsv.gz"),10 seconds)

  }
}

