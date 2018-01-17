package com.mfglabs.commons.aws
package s3

import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.util.ByteString

import com.amazonaws.services.s3.model._
import com.mfglabs.stream.{ExecutionContextForBlockingOps, FlowExt, SourceExt}

import java.io.{ByteArrayInputStream, InputStream}
import java.util.concurrent.ExecutorService
import java.util.zip.GZIPInputStream

import scala.concurrent.Future

object AmazonS3Client {
  import com.amazonaws.auth._
  import com.amazonaws.ClientConfiguration
  import com.amazonaws.regions.{Region, Regions}

  import FutureHelper.defaultExecutorService

  def apply(
    region              : Regions,
    awsCredentials      : AWSCredentials,
    clientConfiguration : ClientConfiguration = new ClientConfiguration()
  )(
    executorService     : ExecutorService     = defaultExecutorService(clientConfiguration, "aws.wrap.s3")
  ): AmazonS3Client = {
   val client = new com.amazonaws.services.s3.AmazonS3Client(awsCredentials, clientConfiguration)
   client.setRegion(Region.getRegion(region))
    new AmazonS3Client(client, executorService)
  }

  def from(
    region                 : Regions,
    awsCredentialsProvider : AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain,
    clientConfiguration    : ClientConfiguration    = new ClientConfiguration()
  )(
    executorService        : ExecutorService        = defaultExecutorService(clientConfiguration, "aws.wrap.s3")
  ): AmazonS3Client = {
   val client = new com.amazonaws.services.s3.AmazonS3Client(awsCredentialsProvider, clientConfiguration)
   client.setRegion(Region.getRegion(region))
    new AmazonS3Client(client, executorService)
  }
}

class AmazonS3Client(
  val client          : com.amazonaws.services.s3.AmazonS3Client,
  val executorService : ExecutorService
) extends AmazonS3Wrapper {
  import scala.collection.immutable.Seq

  implicit lazy val ecForBlockingOps = ExecutionContextForBlockingOps(ec)

  /**
   * Return a client with additional logic to obtain a Stream logic as a Future.
   */
  def materialized(flowMaterializer : ActorMaterializer): AmazonS3ClientMaterialized =
    new AmazonS3ClientMaterialized(client, executorService, flowMaterializer)

  /** List all files with a given prefix of a S3 bucket.
   * @param bucket S3 bucket
   * @param maybePrefix S3 prefix. If not present, all the files of the bucket will be returned.
   */
  def listFilesAsStream(bucket: String, maybePrefix: Option[String] = None): Source[S3ObjectSummary, akka.NotUsed] = {
    import collection.JavaConverters._

    def getFirstListing: Future[ObjectListing] = maybePrefix match {
      case Some(prefix) => listObjects(bucket, prefix)
      case None => listObjects(bucket)
    }

    def unfold(listing: ObjectListing) =
      Some(Some(listing) -> listing.getObjectSummaries.asScala.to[Seq])

    Source.unfoldAsync[Option[ObjectListing], Seq[S3ObjectSummary]](None) {
      case None             => getFirstListing.map(unfold)
      case Some(oldListing) if oldListing.isTruncated => listNextBatchOfObjects(oldListing).map(unfold)
      case Some(_)          => Future.successful(None)
    }.mapConcat(identity)
  }

  /** Stream a S3 file.
    * @param bucket the bucket name
    * @param key the key of file
    * @param inputStreamTransform optional inputstream transformation (for example GZIP decompression, ...)
    */
  def getFileAsStream(bucket: String, key: String, inputStreamTransform: InputStream => InputStream = identity): Source[ByteString, akka.NotUsed] = {
    SourceExt.seededLazyAsync(getObject(bucket, key)) { o =>
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
      .flatMapConcat { s3Object => getFileAsStream(bucket, s3Object.getKey, inputStreamTransform) }
  }

  /**
   * Upload a stream of bytes as a S3 file and returns an element of type CompleteMultipartUploadResult (or nothing if upstream was empty).
   * @param bucket S3 bucket
   * @param key S3 key
   * @param chunkUploadConcurrency chunk upload concurrency. Order is guaranteed even with chunkUploadConcurrency > 1.
   */
  def uploadStreamAsFile(bucket: String, key: String, chunkUploadConcurrency: Int = 1): Flow[ByteString, CompleteMultipartUploadResult, akka.NotUsed] = {
    val request = new InitiateMultipartUploadRequest(bucket, key)
    uploadStreamAsFile(request, chunkUploadConcurrency)
  }

  def uploadStreamAsFile(intiate: InitiateMultipartUploadRequest, chunkUploadConcurrency: Int): Flow[ByteString, CompleteMultipartUploadResult, akka.NotUsed] = {
    import scala.collection.JavaConverters._

    val uploadChunkSize = 8 * 1024 * 1024 // recommended by AWS

    def initiateUpload: Future[String] = initiateMultipartUpload(intiate).map(_.getUploadId)

    Flow[ByteString]
      .via(FlowExt.rechunkByteStringBySize(uploadChunkSize))
      .via(FlowExt.zipWithConstantLazyAsync(initiateUpload))
      .via(FlowExt.zipWithIndex)
      .mapAsyncUnordered(chunkUploadConcurrency) { case ((bytes, uploadId), partNumber) =>
        val uploadRequest = new UploadPartRequest()
          .withBucketName(intiate.getBucketName)
          .withKey(intiate.getKey)
          .withPartNumber((partNumber + 1).toInt)
          .withUploadId(uploadId)
          .withInputStream(new ByteArrayInputStream(bytes.toArray))
          .withPartSize(bytes.length.toLong)

        uploadPart(uploadRequest).map(r => (r.getPartETag, uploadId)).recoverWith {
          case e: Exception =>
            abortMultipartUpload(new AbortMultipartUploadRequest(intiate.getBucketName, intiate.getKey, uploadId))
            Future.failed(e)
        }
      }
      .via(FlowExt.fold(Vector.empty[(PartETag, String)])(_ :+ _))
      .mapAsync(1) { etags =>
        etags.headOption match {
          case Some((_, uploadId)) =>
            // Otherwise it create a immutable java List which throw `UnsupportedOperationException`
            val javaETags = etags.map(_._1).to[scala.collection.mutable.Buffer].asJava
            val compRequest = new CompleteMultipartUploadRequest(
              intiate.getBucketName, intiate.getKey, uploadId, javaETags
            )

            val futResult = completeMultipartUpload(compRequest).map(Option.apply).recoverWith { case e: Exception =>
              abortMultipartUpload(new AbortMultipartUploadRequest(intiate.getBucketName, intiate.getKey, uploadId))
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
