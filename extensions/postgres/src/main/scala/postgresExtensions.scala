package com.mfglabs.commons.aws
package extensions.postgres

import java.io.StringReader

import com.mfglabs.commons.aws.s3.AmazonS3Client
import com.mfglabs.commons.aws.s3._
import org.postgresql.PGConnection
import play.api.libs.iteratee._
import scala.concurrent._
import java.sql.{ Connection, DriverManager }

case class PostgresConnectionInfo(url: String, user: String, password: String)

class PostgresExtensions(connectionInfo: PostgresConnectionInfo, s3: AmazonS3Client) {
  import s3.executionContext

  Class.forName("org.postgresql.Driver")
  lazy val futConn = Future(DriverManager.getConnection(connectionInfo.url, connectionInfo.user, connectionInfo.password))

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
                                delimiter: String = ",", chunkSize: Int = 5 * 1024 * 1024): Future[String] = {
    futConn.flatMap { conn =>
      val cpManager = conn.asInstanceOf[PGConnection].getCopyAPI()

      val s3stream = s3.getStreamMultipartFile(s3bucket, s3path, chunkSize)

      val dbSink = Iteratee.foldM[Array[Byte], String]("") { case (remaining, nextChunk) =>
        val currentChunk = remaining ++ nextChunk.map(_.toChar).mkString
        val (toInsert, newRemaining) = currentChunk.splitAt(currentChunk.lastIndexOf('\n'))

        Future {
          cpManager.copyIn(s"COPY $dbSchema.$dbTableName FROM STDIN WITH DELIMITER '$delimiter'", new StringReader(toInsert))
        }.map(_ => if (newRemaining.head == '\n') newRemaining.tail else newRemaining)
      }

      val futRemainingString = s3stream |>>> dbSink

      futRemainingString.onComplete(_ => conn.close())
      futRemainingString
    }
  }

}