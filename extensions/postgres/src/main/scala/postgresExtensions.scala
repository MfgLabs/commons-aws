package com.mfglabs.commons.aws
package extensions.postgres

import java.io._
import java.util.zip

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import com.mfglabs.commons.aws.s3.AmazonS3Client
import com.mfglabs.commons.aws.s3._
import com.mfglabs.commons.stream.{MFGSource, MFGFlow}
import org.postgresql.PGConnection
import scala.concurrent._
import java.sql.{ Connection, DriverManager }

sealed trait PGCopyable {
  def copyStr : String
}
case class Table(schema :String, table:String) extends PGCopyable {
  def copyStr = s"$schema.$table"
}
case class Query(str:String) extends PGCopyable {
  def copyStr = "(" + str + ")"
}

trait PostgresStream {
  def getTableAsStream(tableOrQuery: PGCopyable, delimiter: String = ",", chunkSize: Int = 5 * 1024 * 1024)
                      (implicit conn: PGConnection, blockingEc: ExecutionContext, fm: FlowMaterializer): Source[Array[Byte]] = {
    val copyManager = conn.getCopyAPI()
    val os = new PipedOutputStream()
    val is = new PipedInputStream(os)
    Future {
      copyManager.copyOut(s"COPY ${tableOrQuery.copyStr} TO STDOUT DELIMITER E'$delimiter'", os)
    }(blockingEc)
    MFGSource.fromStream(is, chunkSize)
  }

  def insertStreamAsTable(table: Table, delimiter: String = ",", chunkSize: Int = 5 * 1024 * 1024)
                         (implicit conn: PGConnection, blockingEc: ExecutionContext, fm: FlowMaterializer): Flow[Array[Byte], Long] = {
    MFGFlow.byteArrayToString()
           .via(insertLineStreamAsTable(table, delimiter, chunkSize))
  }

  def insertLineStreamAsTable(table: Table, delimiter: String = ",", chunkSize: Int = 5 * 1024 * 1024)
                         (implicit conn: PGConnection, blockingEc: ExecutionContext, fm: FlowMaterializer): Flow[String, Long] = {
    val copyManager = conn.getCopyAPI()
    Flow[String]
      .grouped(chunkSize)
      .via(MFGFlow.mapAsyncWithOrderedSideEffect { sqlQ => // TODO: why do we limit concurrency here ?
        Future {
          copyManager.copyIn(s"COPY ${table.copyStr} FROM STDIN WITH DELIMITER '$delimiter'", new StringReader(sqlQ.mkString("\n")))
        }
      })
  }

  def sqlConnAsPgConnUnsafe(conn: Connection) = conn.asInstanceOf[PGConnection]
}

class PostgresExtensions(s3c: AmazonS3Client) extends PostgresStream {
  import s3c.executionContext

  /**
   * Stream a multipart S3 file to a postgres db
   * @param s3bucket
   * @param s3path
   * @param table
   * @param delimiter
   * @param insertbatchSize number of lines by COPY batch
   * @param chunkSize size of the downloaded chunks from S3
   * @param conn
   * @param fm flow materializer
   * @return nothing
   */
  def insertMultipartFileFromS3AsTable(s3bucket: String, s3path: String, table: Table,
                                delimiter: String = ",", insertbatchSize : Int = 5000, chunkSize: Int = 5 * 1024 * 1024)
                               (implicit conn: PGConnection, fm: FlowMaterializer): Future[Unit] = {
   val cpManager = conn.getCopyAPI()

     s3c.getStreamMultipartFile(s3bucket, s3path, chunkSize)
        .via(insertStreamAsTable(table, delimiter, chunkSize))
        .runWith(Sink.foreach(_ => ()))
  }

  /**
   * dump a table or a query result as a CSV to S3
   * @param outputStreamTransformer inputStream transformation. (uncompress through a InflaterInputStream for instance).
   * @param tableOrQuery table name + schema or sql query
   * @param delimiter for the resulting CSV
   * @param bucket
   * @param key
   * @param conn
   * @param fm flow materializer
   * @return a successful future of the uploaded number of chunks (or a failure)
   */
  def uploadTableAsS3MultipartFile(tableOrQuery : PGCopyable, delimiter : String, bucket : String, key : String, chunkSize: Int = 5 * 1024 * 1024)
              (outputStreamTransformer : OutputStream => OutputStream)
              (implicit conn: PGConnection, fm: FlowMaterializer) : Future[Int] = {
    val copyManager = conn.getCopyAPI()
    val os = new PipedOutputStream()
    val is = new PipedInputStream(os)
    val tos = outputStreamTransformer(os)
    Future {
      copyManager.copyOut(s"COPY ${tableOrQuery.copyStr} TO STDOUT DELIMITER E'$delimiter'", tos)
      tos.close()
    }
    val en = MFGSource.fromStream(is,chunkSize)
    s3c.uploadStream(bucket, key, en)
  }

  def uploadTableAsFlatS3MultipartFile(tableOrQuery : PGCopyable , delimiter : String, bucket : String, key : String)
                                  (implicit conn: PGConnection, fm: FlowMaterializer): Future[Int] =
    uploadTableAsS3MultipartFile(tableOrQuery, delimiter, bucket, key)(identity)(conn, fm)

  def uploadTableAsGzipS3MultipartFile(tableOrQuery : PGCopyable, delimiter : String, bucket : String, key : String)
                                      (implicit conn : PGConnection, fm: FlowMaterializer) =
    uploadTableAsS3MultipartFile(tableOrQuery,delimiter,bucket,key)(new zip.GZIPOutputStream(_))
}