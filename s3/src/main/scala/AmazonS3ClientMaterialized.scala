package com.mfglabs.commons.aws
package s3

import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import com.amazonaws.services.s3.model.{DeleteObjectsResult, S3ObjectSummary}
import scala.concurrent.Future

/**
 * Additional functions which materialize stream to Future.
 */
class AmazonS3ClientMaterialized(
  client                        : com.amazonaws.services.s3.AmazonS3Client,
  executorService               : java.util.concurrent.ExecutorService,
  implicit val flowMaterializer : ActorMaterializer
) extends AmazonS3Client(client, executorService) {

  def deleteObjects(bucket: String, commonPrefix: String): Future[Seq[DeleteObjectsResult.DeletedObject]] = {
    for {
      objects   <- listFiles(bucket, Some(commonPrefix))
      deleted   <- deleteObjects(bucket, objects.map(_.getKey):_*)
    } yield deleted
  }

  /** List all files with a given prefix of a S3 bucket.
    *
    * @param  bucket the bucket name
    * @param  path an optional path to search in bucket
    * @return a future of seq of file keys & last modified dates (or a failure)
    */
  def listFiles(bucket: String, path: Option[String] = None): Future[Seq[S3ObjectSummary]] = {
    listFilesAsStream(bucket, path).runWith(Sink.seq)
  }

  /**
   * Get a S3 file.
   * @param bucket
   * @param key
   * @return binary file
   */
  def getFile(bucket: String, key: String): Future[ByteString] = {
    getFileAsStream(bucket, key).runFold(ByteString.empty)(_ ++ _).map(_.compact)
  }

  override def shutdown(): Unit = {
    flowMaterializer.shutdown()
    super.shutdown()
  }

}
