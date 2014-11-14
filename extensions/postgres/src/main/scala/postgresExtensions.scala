package com.mfglabs.commons.aws
package extensions.postgres

import java.io.{PipedInputStream, PipedOutputStream, OutputStream, StringReader}
import java.util.zip

import com.mfglabs.commons.aws.extensions.postgres.PostgresExtensions.PGCopyable
import com.mfglabs.commons.aws.s3.AmazonS3Client
import com.mfglabs.commons.aws.s3._
import org.postgresql.PGConnection
import play.api.libs.iteratee._
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
class PostgresExtensions(s3: AmazonS3Client) {
  import s3.executionContext

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
    val cpManager = sqlConnection.asInstanceOf[PGConnection].getCopyAPI()

    val s3stream = s3.getStreamMultipartFile(s3bucket, s3path, chunkSize)

    val dbSink = Iteratee.foldM[Array[Byte], String]("") { case (remaining, nextChunk) =>
      val currentChunk = remaining ++ nextChunk.map(_.toChar).mkString
      val (toInsert, newRemaining) = currentChunk.splitAt(currentChunk.lastIndexOf('\n'))

      Future {
        cpManager.copyIn(s"COPY $dbSchema.$dbTableName FROM STDIN WITH DELIMITER '$delimiter'", new StringReader(toInsert))
      }.map(_ => if (newRemaining.head == '\n') newRemaining.tail else newRemaining)
    }

    s3stream |>>> dbSink
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
    s3.uploadStream(bucket, key, en)
  }

  def copyToS3AsFlatFile(tableOrQuery : PGCopyable , delimiter : String, bucket : String, key : String)(implicit conn : PGConnection) =
    copyToS3(identity)(tableOrQuery,delimiter,bucket,key)

  def copyToS3AsGzip(tableOrQuery : PGCopyable, delimiter : String, bucket : String, key : String)(implicit conn : PGConnection) =
  //copyToS3(x => new zip.GZIPInputStream(x))(tableOrQuery,delimiter,bucket,key)
    copyToS3(x => {
      new zip.GZIPOutputStream(x)
    })(tableOrQuery,delimiter,bucket,key)


}