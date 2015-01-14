package com.mfglabs.commons.aws
package extensions.postgres

import java.io.{PipedInputStream, PipedOutputStream, OutputStream, StringReader}
import java.util.zip

import com.mfglabs.commons.aws.extensions.postgres.PostgresExtensions.PGCopyable
import akka.actor.ActorSystem
import akka.stream.FlowMaterializer
import akka.stream.scaladsl.FoldSink
import com.mfglabs.commons.aws.s3.AmazonS3Client
import com.mfglabs.commons.aws.s3._
import com.mfglabs.commons.stream.MFGFlow
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

  /** Stream a multipart S3 file to a postgre db
   *
   * @param s3bucket
   * @param s3path
   * @param dbSchema
   * @param dbTableName
   * @param chunkSize
   * @return remaining string if the stream does not end with a '\n'
   */
  def streamMultipartFileFromS3(s3bucket: String, s3path: String, dbSchema: String, dbTableName: String,
                                delimiter: String = ",", chunkSize: Int = 5 * 1024 * 1024)
                               (implicit sqlConnection: Connection): Future[String] = {

    implicit val as = ActorSystem()
    implicit val fm = FlowMaterializer()

    val cpManager = sqlConnection.asInstanceOf[PGConnection].getCopyAPI()

    s3c.getStreamMultipartFile(s3bucket, s3path, chunkSize)
      .via(MFGFlow.byteArrayToString)
      .via(MFGFlow.mapAsyncUnorderedWithBoundedParallelism(5){ sqlQ =>
        Future {
          cpManager.copyIn(s"COPY $dbSchema.$dbTableName FROM STDIN WITH DELIMITER '$delimiter'", new StringReader(sqlQ))
          }.map(_ => sqlQ)})
      .runWith(FoldSink[String, String]("")(_ + "\n" + _))
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
    val copyManager = conn.getCopyAPI()
    val os = new PipedOutputStream()
    val is = new PipedInputStream(os)
    val tos = outputStreamTransformer(os)
    Future {
      copyManager.copyOut(s"COPY ${tableOrQuery.copyStr} TO STDOUT DELIMITER E'$delimiter'", tos)
      tos.close()
    }
    val en = Enumerator.fromStream(is,chunkSize)
    s3c.uploadStream(bucket, key, en)
  }

  def copyToS3AsFlatFile(tableOrQuery : PGCopyable , delimiter : String, bucket : String, key : String)(implicit conn : PGConnection) =
    copyToS3(identity)(tableOrQuery,delimiter,bucket,key)

  def copyToS3AsGzip(tableOrQuery : PGCopyable, delimiter : String, bucket : String, key : String)(implicit conn : PGConnection) =
    copyToS3(x => {
      new zip.GZIPOutputStream(x)
    })(tableOrQuery,delimiter,bucket,key)

}