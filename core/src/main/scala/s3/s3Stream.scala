package com.mfglabs.commons.aws
package s3

import java.io.{File, ByteArrayInputStream, InputStream}
import java.util.Date
import java.util.zip.GZIPInputStream

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import com.amazonaws.services.s3.model._
import com.mfglabs.stream.{ExecutionContextForBlockingOps, SinkExt, FlowExt, SourceExt}

import scala.concurrent.{ExecutionContext, Future}

trait S3StreamBuilder {
  val client: AmazonS3AsyncClient

  import client.ec
  implicit lazy val ecForBlockingOps = ExecutionContextForBlockingOps(client.ec)

  // Ops class contains materialized methods (returning Futures)
  class MaterializedOps(flowMaterializer: ActorMaterializer)
    extends AmazonS3AsyncClient(client.awsCredentialsProvider, client.clientConfiguration, client.executorService) {

    implicit val fm = flowMaterializer

    /** List all files with a given prefix of a S3 bucket.
      *
      * @param  bucket the bucket name
      * @param  path an optional path to search in bucket
      * @return a future of seq of file keys & last modified dates (or a failure)
      */
    def listFiles(bucket: String, path: Option[String] = None): Future[Seq[(String, Date)]] = {
      listFilesAsStream(bucket, path).runWith(SinkExt.collect)
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

  } // end Ops

  /** List all files with a given prefix of a S3 bucket.
   * @param bucket S3 bucket
   * @param maybePrefix S3 prefix. If not present, all the files of the bucket will be returned.
   */
  def listFilesAsStream(bucket: String, maybePrefix: Option[String] = None): Source[(String, Date), akka.NotUsed] = {
    import collection.JavaConversions._

    def getFirstListing: Future[ObjectListing] = maybePrefix match {
      case Some(prefix) => client.listObjects(bucket, prefix)
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

  /** Stream a S3 file.
    * @param bucket the bucket name
    * @param key the key of file
    * @param inputStreamTransform optional inputstream transformation (for example GZIP decompression, ...)
    */
  def getFileAsStream(bucket: String, key: String, inputStreamTransform: InputStream => InputStream = identity): Source[ByteString, akka.NotUsed] = {
    SourceExt.seededLazyAsync(client.getObject(bucket, key)) { o =>
      StreamConverters.fromInputStream(() => inputStreamTransform(o.getObjectContent))
    }
  }

  /**
   * Stream a gzipped S3 file and decompress it on the fly.
   * @param bucket S3 bucket
   * @param key S3 key
   * @return
   */
  def uncompressGzippedFileAsStream(bucket: String, key: String) =
    getFileAsStream(bucket, key, is => new GZIPInputStream(is))

  /**
   * Stream sequentially a SE multipart file.
   * @param bucket S3 bucket
   * @param prefix S3 prefix. Files of path prefix* will be taken into account.
   * @param inputStreamTransform optional inputstream transformation (for example GZIP decompression, ...)
   */
  def getMultipartFileAsStream(bucket: String, prefix: String, inputStreamTransform: InputStream => InputStream = identity): Source[ByteString, akka.NotUsed] = {
    listFilesAsStream(bucket, Some(prefix))
      .flatMapConcat { case (key, _) => getFileAsStream(bucket, key, inputStreamTransform) }
  }

  /**
   * Upload a stream of bytes as a S3 file and returns an element of type CompleteMultipartUploadResult (or nothing if upstream was empty).
   * @param bucket S3 bucket
   * @param key S3 key
   * @param chunkUploadConcurrency chunk upload concurrency. Order is guaranteed even with chunkUploadConcurrency > 1.
   */
  def uploadStreamAsFile(bucket: String, key: String, chunkUploadConcurrency: Int = 1): Flow[ByteString, CompleteMultipartUploadResult, akka.NotUsed] = {
    import scala.collection.JavaConversions._

    val uploadChunkSize = 8 * 1024 * 1024 // recommended by AWS

    def initiateUpload(bucket: String, key: String): Future[String] =
      client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucket, key)).map(_.getUploadId)

    Flow[ByteString]
      .via(FlowExt.rechunkByteStringBySize(uploadChunkSize))
      .via(FlowExt.zipWithConstantLazyAsync(initiateUpload(bucket, key)))
      .via(FlowExt.zipWithIndex)
      .mapAsyncUnordered(chunkUploadConcurrency) { case ((bytes, uploadId), partNumber) =>
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
      .via(FlowExt.fold(Vector.empty[(PartETag, String)])(_ :+ _))
      .mapAsync(1) { etags =>
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
   * The Flow returns a CompleteMultipartUploadResult for each part file uploaded.
   * @param bucket S3 bucket
   * @param prefix S3 prefix. The actual part files will be named prefix.part.00000001, prefix.part.00000002, ...
   * @param nbChunkPerFile the number of upstream chunks (for example lines) to include in each part file
   * @param chunkUploadConcurrency chunk upload concurrency. Order is guaranteed even with chunkUploadConcurrency > 1.
   */
  def uploadStreamAsMultipartFile(bucket: String, prefix: String, nbChunkPerFile: Int,
                                  chunkUploadConcurrency: Int = 1): Flow[ByteString, CompleteMultipartUploadResult, akka.NotUsed] = {

    def formatKey(fileNb: Long) = {
      val pad = "%08d".format(fileNb)
      s"$prefix.part.$pad"
    }

    uploadStreamAsMultipartFile(bucket, i => formatKey(i / nbChunkPerFile), nbChunkPerFile, chunkUploadConcurrency)
  }

  /**
   * Upload a stream as a multipart file with a given number of upstream chunk per file. Part file are uploaded sequentially but
   * chunk inside a part file can be uploaded concurrently (tuned with chunkUploadConcurrency).
   * The Flow returns a CompleteMultipartUploadResult for each part file uploaded.
   * @param bucket S3 bucket
   * @param getKey S3 key factory that will generate a new key for each part file based on the index of upstream chunks...
   * @param nbChunkPerFile the number of upstream chunks (for example lines) to include in each part file
   * @param chunkUploadConcurrency chunk upload concurrency. Order is guaranteed even with chunkUploadConcurrency > 1.
   */
  def uploadStreamAsMultipartFile(bucket: String, getKey: Long => String, nbChunkPerFile: Int,
      chunkUploadConcurrency : Int): Flow[ByteString, CompleteMultipartUploadResult, akka.NotUsed] = {

    Flow[ByteString]
      .via(FlowExt.zipWithIndex)
      .splitWhen { x =>
        val i = x._2
        i != 0 && i % nbChunkPerFile == 0
      }
      .via(
          FlowExt.withHead(includeHeadInUpStream = true) { case (_, i) =>
            Flow[(ByteString, Long)].map(_._1).via(uploadStreamAsFile(bucket, getKey(i), chunkUploadConcurrency))
          }
        )
      .concatSubstreams
  }
  def uploadStreamAsMultipartFile(bucket: String, getKey: Long => String, nbChunkPerFile: Int) : Flow[ByteString, CompleteMultipartUploadResult, akka.NotUsed] =
    uploadStreamAsMultipartFile(bucket,getKey,nbChunkPerFile,1)
}

object S3StreamBuilder {
  def apply(s3client: AmazonS3AsyncClient) = new S3StreamBuilder {
    override val client: AmazonS3AsyncClient = s3client
  }
}

