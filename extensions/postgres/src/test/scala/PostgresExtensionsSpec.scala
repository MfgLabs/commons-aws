
package com.mfglabs.commons.aws

import java.sql.{Connection, DriverManager}

import akka.actor.ActorSystem
import com.mfglabs.commons.aws.commons.DockerTmpDB
import org.scalatest.time._
import collection.mutable.Stack
import org.scalatest._
import concurrent.ScalaFutures
import scala.concurrent.Future

/**
 * To run this test, launch a local postgresql instance and put the right connection info into DriverManager.getConnection
 */

class PostgresExtensionsSpec extends FlatSpec with Matchers with ScalaFutures with BeforeAndAfterAll with DockerTmpDB {
  import s3._
  import extensions.postgres._
  import scala.concurrent.ExecutionContext.Implicits.global

  val bucket = "mfg-commons-aws"

  val keyPrefix = "test/extensions/postgres"

  val resDir = "extensions/postgres/src/test/resources"

  implicit val as = ActorSystem()
  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(5, Minutes), interval = Span(5, Millis))

  Class.forName("org.postgresql.Driver")
  //implicit val conn = DriverManager.getConnection("jdbc:postgresql:metadsp", "atamborrino", "password")
  val S3 = new s3.AmazonS3Client()
  val pg = new PostgresExtensions(S3)

  it should "stream a S3 multipart file to postgres" in {


    fail("test dataset is not correct")
  }
   /* // create table
    val stmt = conn.createStatement()
    stmt.execute(
      s"""
         drop table if exists test_postgres_aws_s3
       """
    )
    stmt.execute(
      s"""
         create table test_postgres_aws_s3(
          io_id integer,
          dsp_name text,
          advertiser_id integer,
          campaign_id integer,
          strategy_id integer,
          day date,
          impressions integer,
          clicks integer,
          post_view_conversions float8,
          post_click_conversions float8,
          media_cost float8,
          total_ad_cost float8,
          total_cost float8
         )
       """
    )

    //WARNING data is corrupted

    whenReady(
      for {
//        _ <- S3.uploadStream(bucket, s"$keyPrefix/report.csv0000_part_00",
//                        Enumerator.fromFile(new java.io.File(s"$resDir/report.csv0000_part_00")))
//        _ <- S3.uploadStream(bucket, s"$keyPrefix/report.csv0001_part_00",
//                        Enumerator.fromFile(new java.io.File(s"$resDir/report.csv0001_part_00")))
//        _ <- S3.uploadStream(bucket, s"$keyPrefix/report.csv0002_part_00",
//                        Enumerator.fromFile(new java.io.File(s"$resDir/report.csv0002_part_00")))
//        _ <- S3.uploadStream(bucket, s"$keyPrefix/report.csv0003_part_00",
//                          Enumerator.fromFile(new java.io.File(s"$resDir/report.csv0003_part_00")))
        leftString <- pg.streamMultipartFileFromS3(bucket, s"$keyPrefix/report.csv", "public", "test_postgres_aws_s3")
//        _ <- S3.deleteFile(bucket, s"$keyPrefix/report.csv0000_part_00")
//        _ <- S3.deleteFile(bucket, s"$keyPrefix/report.csv0001_part_00")
//        _ <- S3.deleteFile(bucket, s"$keyPrefix/report.csv0002_part_00")
//        _ <- S3.deleteFile(bucket, s"$keyPrefix/report.csv0003_part_00")
      } yield leftString
    ) { leftString =>
      val rs = stmt.executeQuery("select count(*) from test_postgres_aws_s3")
      rs.next()
      rs.getInt(1) should equal (1150907) // number of lines
      leftString should equal ("")
    }

    stmt.close()
  }
*/
  override def afterAll(): Unit = {
    conn.close()
  }
}

