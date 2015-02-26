package com.mfglabs.commons.aws
package s3

import java.io.{File, ByteArrayInputStream, InputStream}
import java.util.Date
import java.util.zip.GZIPInputStream

import akka.actor.ActorSystem
import akka.stream.{ActorFlowMaterializer, FlowMaterializer, FlattenStrategy}
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import com.amazonaws.services.s3.model._
import com.mfglabs.stream.{ExecutionContextForBlockingOps, SinkExt, FlowExt, SourceExt}

import scala.concurrent.{ExecutionContext, Future}

trait S3StreamBuilder {
  val client: AmazonS3AsyncClient

  import client.ec
  implicit lazy val ecForBlockingOps = ExecutionContextForBlockingOps(client.ec)

  // Ops class contains materialized methods (returning Futures)
  class Ops(flowMaterializer: FlowMaterializer = ActorFlowMaterializer()(ActorSystem("com-mfglabs-commons-aws-s3")))
      extends AmazonS3AsyncClient(client.awsCredentialsProvider, client.clientConfiguration, client.executorService) {

    implicit val fm = flowMaterializer

    /** List files in a bucket (& optional path)
      *
      * @param  bucket the bucket name
      * @param  path an optional path to search in bucket
      * @return a future of seq of file keys & last modified dates (or a failure)
      */
    def listFiles(bucket: String, path: Option[String] = None): Future[Seq[(String, Date)]] = {
      listFilesAsStream(bucket, path).runWith(SinkExt.collect)
    }

    def getFile(bucket: String, key: String): Future[ByteString] = {
      getFileAsStream(bucket, key).runFold(ByteString.empty)(_ ++ _).map(_.compact)
    }

  } // end Ops

  def listFilesAsStream(bucket: String, path: Option[String] = None): Source[(String, Date)] = {
    import collection.JavaConversions._

    def getFirstListing: Future[ObjectListing] = path match {
      case Some(p) => client.listObjects(bucket, p)
      case None => client.listObjects(bucket)
    }

    SourceExt.seededLazyAsync(getFirstListing) { firstListing =>
      SourceExt.unfoldPullerAsync(firstListing) { listing =>
        val files = listing.getObjectSummaries.to[scala.collection.immutable.Seq]
        if (listing.isTruncated)
          client.listNextBatchOfObjects(listing).map { nextListing =>
            (Option(files), Option(nextListing))
          }
        else Future.successful(Option(files), None)
      }
    }
      .mapConcat(identity)
      .map { file =>
      (file.getKey, file.getLastModified)
    }
  }

  /** Download of file as a stream with an optional inputstream transformation.
    *
    * @param bucket the bucket name
    * @param key the key of file
    * @param inputStreamTransform transformation function (for ZIP, GZIP decompression, ...)
    * @return an enumerator (stream) of the object
    */
  def getFileAsStream(bucket: String, key: String, inputStreamTransform: InputStream => InputStream = identity): Source[ByteString] = {
    SourceExt.seededLazyAsync(client.getObject(bucket, key)) { o =>
      SourceExt.fromStream(inputStreamTransform(o.getObjectContent))
    }
  }

  def uncompressGzippedFileAsStream(bucket: String, key: String) =
    getFileAsStream(bucket, key, is => new GZIPInputStream(is))

  /** Sequential download of a multipart file as a reactive stream
    *
    * @param bucket bucket name
    * @param path the common path of the parts of the file
    * @return
    */
  def getMultipartFileAsStream(bucket: String, path: String, inputStreamTransform: InputStream => InputStream = identity): Source[ByteString] = {
    listFilesAsStream(bucket, Some(path))
      .map { case (key, _) => getFileAsStream(bucket, key, inputStreamTransform) }
      .flatten(FlattenStrategy.concat)
  }

  /**
   * Flow that upload a stream of bytes as a S3 file and returns a CompleteMultipartUploadResult (or nothing if the stream was empty).
   * Order is guaranteed even with chunkUploadConcurrency > 1.
   * An error during the S3 upload will result in a stream failure.
   * @param  bucket the bucket name
   * @param  key the key of file
   * @return a stream with only one element of type CompleteMultipartUploadResult if the upload was successful
   */
  def uploadStreamAsFile(bucket: String, key: String, chunkUploadConcurrency: Int = 1): Flow[ByteString, CompleteMultipartUploadResult] = {
    import scala.collection.JavaConversions._

    val uploadChunkSize = 8 * 1024 * 1024 // recommended by AWS

    def initiateUpload(bucket: String, key: String): Future[String] =
      client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucket, key)).map(_.getUploadId)

    Flow[ByteString]
      .via(FlowExt.rechunkByteStringBySize(uploadChunkSize))
      .via(FlowExt.zipWithConstantLazyAsync(initiateUpload(bucket, key)))
      .via(FlowExt.zipWithIndex)
      .via(
        FlowExt.mapAsyncUnorderedWithBoundedConcurrency(chunkUploadConcurrency) { case ((bytes, uploadId), partNumber) =>
          val uploadRequest = new UploadPartRequest()
            .withBucketName(bucket)
            .withKey(key)
            .withPartNumber((partNumber + 1).toInt)
            .withUploadId(uploadId)
            .withInputStream(new ByteArrayInputStream(bytes.toArray))
            .withPartSize(bytes.length)
          client.uploadPart(uploadRequest).map(r => (r.getPartETag, uploadId)).recoverWith {
            case e: Exception =>
              client.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, uploadId))
              Future.failed(e)
          }
        }
      )
      .via(FlowExt.fold(Vector.empty[(PartETag, String)])(_ :+ _))
      .mapAsync { etags =>
      etags.headOption match {
        case Some((_, uploadId)) =>
          val compRequest = new CompleteMultipartUploadRequest(bucket, key, uploadId, etags.map(_._1).toBuffer[PartETag])
          val futResult = client.completeMultipartUpload(compRequest).map(Option.apply).recoverWith { case e: Exception =>
            client.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, uploadId))
            Future.failed(e)
          }
          futResult

        case None => Future.successful(None)
      }
    }
      .mapConcat(_.to[scala.collection.immutable.Seq])
  }

  /**
   * Upload a stream as a multipart file with a given number of upstream chunk per file. Part file are uploaded sequentially but
   * chunk inside a part file can be uploaded concurrently (tuned with chunkUploadConcurrency).
   * Order is guaranteed even with chunkUploadConcurrency > 1.
   * @param bucket
   * @param prefix
   * @param nbChunkPerFile
   * @param chunkUploadConcurrency
   * @return
   */
  def uploadStreamAsMultipartFile(bucket: String, prefix: String, nbChunkPerFile: Int,
                                  chunkUploadConcurrency: Int = 1): Flow[ByteString, CompleteMultipartUploadResult] = {

    def formatKey(fileNb: Long) = {
      val pad = "%08d".format(fileNb)
      s"$prefix.part.$pad"
    }

    Flow[ByteString]
      .via(FlowExt.zipWithIndex)
      .splitWhen { case (_, i) => i != 0 && i % nbChunkPerFile == 0 }
      .map { partFileStream =>
      partFileStream.via(
        FlowExt.withHead(includeHeadInUpStream = true) { case (_, i) =>
          val key = formatKey(i / nbChunkPerFile)
          Flow[(ByteString, Long)].map(_._1).via(uploadStreamAsFile(bucket, key, chunkUploadConcurrency))
        }
      )
    }
      .flatten(FlattenStrategy.concat) // change to FlattenStrategy.merge if we want to allow concurrent upload of part files
  }

}

object S3StreamBuilder {
  def apply(s3client: AmazonS3AsyncClient) = new S3StreamBuilder {
    override val client: AmazonS3AsyncClient = s3client
  }
}

