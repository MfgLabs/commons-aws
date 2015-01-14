package com.mfglabs.commons.aws
package extensions.postgres

import java.io.StringReader

import akka.actor.ActorSystem
import akka.stream.FlowMaterializer
import akka.stream.scaladsl.FoldSink
import com.mfglabs.commons.aws.s3.AmazonS3Client
import com.mfglabs.commons.aws.s3._
import com.mfglabs.commons.stream.MFGFlow
import org.postgresql.PGConnection
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

    implicit val as = ActorSystem()
    implicit val fm = FlowMaterializer()

    val cpManager = sqlConnection.asInstanceOf[PGConnection].getCopyAPI()

    s3.getStreamMultipartFile(s3bucket, s3path, chunkSize)
      .via(MFGFlow.byteArrayToString)
      .via(MFGFlow.mapAsyncUnorderedWithBoundedParallelism(5){ sqlQ =>
        Future {
          cpManager.copyIn(s"COPY $dbSchema.$dbTableName FROM STDIN WITH DELIMITER '$delimiter'", new StringReader(sqlQ))
          }.map(_ => sqlQ)})
      .runWith(FoldSink[String, String]("")(_ + "\n" + _))
  }
}