package com.mfglabs.commons.aws

import java.io.File
import java.sql.{Connection, DriverManager}
import java.util.concurrent.atomic.AtomicLong

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import com.mfglabs.commons.aws.commons.DockerTmpDB
import com.mfglabs.commons.stream.{MFGSink, ExecutionContextForBlockingOps, MFGFlow, MFGSource}
import org.scalatest.time._
import collection.mutable.Stack
import org.scalatest._
import concurrent.ScalaFutures
import scala.concurrent.Future

/**
 * To run this test, launch a local postgresql instance and put the right connection info into DriverManager.getConnection
 */

class PostgresExtensionsSpec extends FlatSpec with Matchers with ScalaFutures with BeforeAndAfterAll with DockerTmpDB {

  import extensions.postgres._

  val bucket = "mfg-commons-aws"

  val keyPrefix = "test/extensions/postgres"

  implicit val as = ActorSystem()
  implicit val fm = ActorFlowMaterializer()
  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(5, Minutes), interval = Span(5, Millis))

  Class.forName("org.postgresql.Driver")
  val pg = PgStream()

  "PgStream" should "stream a file to a postgres table and stream a query from a postgre table" in {
    val stmt = conn.createStatement()
    implicit val pgConn = pg.sqlConnAsPgConnUnsafe(conn)
    import scala.concurrent.ExecutionContext.Implicits.global
    implicit val blockingEc = ExecutionContextForBlockingOps(scala.concurrent.ExecutionContext.Implicits.global)

    stmt.execute(
      s"""
         create table public.test_postgres_aws_s3(
          id serial primary key,
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

    val insertTable = "test_postgres_aws_s3(io_id, dsp_name, advertiser_id, campaign_id, strategy_id, day, impressions, " +
      "clicks, post_view_conversions, post_click_conversions, media_cost, total_ad_cost, total_cost)"

    val nbLinesInserted = new AtomicLong(0L)

    val futLines = MFGSource
      .fromFile(new File(getClass.getResource("/report.csv0000_part_00").getPath), maxChunkSize = 5 * 1024 * 1024)
      .via(MFGFlow.rechunkByteStringBySeparator())
      .via(pg.insertStreamToTable("public", insertTable, chunkInsertionConcurrency = 2))
      .via(MFGFlow.fold(0L)(_ + _))
      .map { total =>
        nbLinesInserted.set(total)
        pg.getQueryResultAsStream("select * from public.test_postgres_aws_s3 order by id")
      }
      .flatten(FlattenStrategy.concat)
      .via(MFGFlow.rechunkByteStringBySize(5 * 1024 * 1024))
      .via(MFGFlow.rechunkByteStringBySeparator())
      .map(_.utf8String)
      .runWith(MFGSink.collect)

    val futExpectedLines = MFGSource
      .fromFile(new File(getClass.getResource("/report.csv0000_part_00").getPath), maxChunkSize = 5 * 1024 * 1024)
      .via(MFGFlow.rechunkByteStringBySeparator())
      .map(_.utf8String)
      .runWith(MFGSink.collect)

    whenReady(futLines zip futExpectedLines) { case (lines, expectedLines) =>
      lines.length shouldEqual expectedLines.length
      lines.length shouldEqual nbLinesInserted.get

      lines.zip(expectedLines).foreach { case (line, expectedLine) =>
        line.split(",")(1) shouldEqual expectedLine.split(",")(0) // comparing io_id
      }

      stmt.close()
    }


  }
}


