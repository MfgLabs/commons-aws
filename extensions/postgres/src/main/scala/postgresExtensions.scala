package com.mfglabs.commons.aws
package extensions.postgres

import java.io.StringReader

import com.mfglabs.commons.aws.s3.AmazonS3Client
import com.mfglabs.commons.aws.s3._
import org.postgresql.PGConnection
import play.api.libs.iteratee._
import scala.concurrent._
import java.sql.{ Connection, DriverManager }

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

}