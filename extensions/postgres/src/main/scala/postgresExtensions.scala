package com.mfglabs.commons.aws
package extensions.postgres

import java.io.{PipedInputStream, PipedOutputStream, OutputStream, StringReader}
import java.util.zip

import com.mfglabs.commons.aws.extensions.postgres.PostgresExtensions.PGCopyable
import akka.actor.ActorSystem
import akka.stream.FlowMaterializer
import akka.stream.scaladsl.{Sink, Flow, FoldSink}
import com.mfglabs.commons.aws.s3.AmazonS3Client
import com.mfglabs.commons.aws.s3._
import com.mfglabs.commons.stream.{MFGSource, MFGFlow}
import org.postgresql.PGConnection
import scala.concurrent._
import java.sql.{ Connection, DriverManager }
object PostgresExtensions {
  sealed trait PGCopyable {def copyStr : String}
  case class Table(schema :String, table:String) extends PGCopyable{
    def copyStr = s"$schema.$table"
  }
  case class Query(str:String) extends PGCopyable{
    def copyStr = "(" + str + ")"
  }
}
class PostgresExtensions(s3c: AmazonS3Client) {
  import s3c.executionContext


  /**
   * Stream a multipart S3 file to a postgres db
   * @param s3bucket
   * @param s3path
   * @param dbSchema
   * @param dbTableName
   * @param delimiter
   * @param insertbatchSize number of lines by COPY batch
   * @param chunkSize size of the downloaded chunks from S3
   * @param sqlConnection
   * @param as
   * @return nothing
   */
  def streamMultipartFileFromS3(s3bucket: String, s3path: String, dbSchema: String, dbTableName: String,
                                delimiter: String = ",", insertbatchSize : Int = 5000, chunkSize: Int = 5 * 1024 * 1024)
                               (implicit sqlConnection: Connection, as : ActorSystem): Future[Unit] = { //

   // implicit val as = ActorSystem()
    implicit val fm = FlowMaterializer()

    val cpManager = sqlConnection.asInstanceOf[PGConnection].getCopyAPI()

   val res : Future[Unit] =
     s3c.getStreamMultipartFile(s3bucket, s3path, chunkSize)
      .via(MFGFlow.byteArrayToString())
      .via(Flow[String].grouped(insertbatchSize))
      .via(MFGFlow.mapAsyncUnorderedWithBoundedParallelism(1){ sqlQ => //java.lang.IllegalStateException: Processor actor terminated abruptly java.lang.IllegalStateException: Input buffer overrun
        Future {
          cpManager.copyIn(s"COPY $dbSchema.$dbTableName FROM STDIN WITH DELIMITER '$delimiter'", new StringReader(sqlQ.mkString("\n")))
          }
      })
      .runWith(FoldSink({})((a,b)=> {})) //FoldSink[String, String]("")(_ + "\n" + _))
    res
  }
  import com.mfglabs.commons.aws.s3.AmazonS3Client
  import com.mfglabs.commons.aws.s3._
  import scala.concurrent.ExecutionContext.Implicits.global

  /**
   * dump a table or a query result as a CSV to S3
   * @param outputStreamTransformer inputStream transformation. (uncompress through a InflaterInputStream for instance).
   * @param tableOrQuery table name + schema or sql query
   * @param delimiter for the resulting CSV
   * @param bucket
   * @param key
   * @param conn
   * @return a successful future of the uploaded number of chunks (or a failure)
   */
  def copyToS3(outputStreamTransformer : OutputStream => OutputStream)
              (tableOrQuery : PGCopyable, delimiter : String, bucket : String, key : String, chunkSize: Int = 81920)(implicit conn : PGConnection)
  : Future[Int] = {
    implicit val as = ActorSystem()
    implicit val fm = FlowMaterializer()
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

  def copyToS3AsFlatFile(tableOrQuery : PGCopyable , delimiter : String, bucket : String, key : String)(implicit conn : PGConnection) =
    copyToS3(identity)(tableOrQuery,delimiter,bucket,key)

  def copyToS3AsGzip(tableOrQuery : PGCopyable, delimiter : String, bucket : String, key : String)(implicit conn : PGConnection) =
    copyToS3(x => {
      new zip.GZIPOutputStream(x)
    })(tableOrQuery,delimiter,bucket,key)
}