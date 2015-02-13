package com.mfglabs.commons.aws

import java.text.{DateFormat, SimpleDateFormat}
import java.util.zip.GZIPInputStream

import akka.actor.Status.Failure
import akka.actor.{ActorSystem, Props, Stash, ActorLogging}
import akka.stream.{FlowMaterializer, FlattenStrategy}
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl._
import akka.stream.stage._
import akka.util.ByteString
import com.amazonaws.services.s3.model.DeleteObjectsResult.DeletedObject
import com.mfglabs.commons.aws.s3.AmazonS3Client
import com.mfglabs.commons.stream.{MFGSink, ExecutionContextForBlockingOps, MFGSource, MFGFlow}
import scala.concurrent.duration.{FiniteDuration, Duration}
import scala.concurrent.{ExecutionContext, Future, Promise}

import java.io._
import java.util.Date

import com.amazonaws.services.s3.model._
import scala.collection.JavaConversions._
import scala.util.Try

/**
 * Enhances default AWS AmazonS3Client for Scala :
 * - asynchronous/non-blocking wrapper using an external pool of threads managed internally.
 * - a few file management/streaming facilities.
 *
 * It is based on Opensource Pellucid wrapper.
 *
 * ==Overview==
 * To use rich MFGLabs AWS S3 wrapper, you just have to add the following:
 * {{{
 * import com.mfglabs.commons.aws.s3
 * import s3._ // brings implicit extensions
 *
 * // Create the client
 * val S3 = new AmazonS3Client()
 * // Use it
 * for {
 *   _   <- S3.uploadStream(bucket, "big.txt", Enumerator.fromFile(new java.io.File(s"big.txt")))
 *   l   <- S3.listFiles(bucket)
 *   _   <- S3.deleteFile(bucket, "big.txt")
 *   l2  <- S3.listFiles(bucket)
 * } yield (l, l2)
 * }}}
 *
 * Please remark that you don't need any implicit [[scala.concurrent.ExecutionContext]] as it's directly provided
 * and managed by [[AmazonS3Client]] itself.
 * There are smart [[AmazonS3Client]] constructors that can be provided with custom.
 * [[java.util.concurrent.ExecutorService]] if you want to manage your pools of threads.
 */
package object `s3` {

  implicit class RichS3Client(val client: AmazonS3Client) extends AnyVal {
    val defaultChunkSize = 5 * 1024 * 1024

    /** List files in a bucket (& optional path)
      *
      * @param  bucket the bucket name
      * @param  path an optional path to search in bucket
      * @return a future of seq of file keys & last modified dates (or a failure)
      */
    def listFiles(bucket: String, path: Option[String] = None): Future[Seq[(String, Date)]] = {
      listFilesAsStream(bucket, path).runWith(MFGSink.collect)
    }

    def listFilesAsStream(bucket: String, path: Option[String] = None): Source[(String, Date)] = {
      import collection.JavaConversions._

      val futFirstListing: Future[ObjectListing] = path match {
        case Some(p) => client.listObjects(bucket, p)
        case None => client.listObjects(bucket)
      }

      MFGSource.fromSeedAsync(futFirstListing) { firstListing =>
        MFGSource.unfoldPullerAsync(firstListing) { listing =>
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

    /** Upload file to bucket
      *
      * @param  bucket the bucket name
      * @param  key the key of file into which it is uploaded
      * @param  file the File from which to pump data
      * @return a successful future of PutObjectResult (or a failure)
      */
    def uploadFile(bucket: String, key: String, file: File): Future[PutObjectResult] = {

      val r = new PutObjectRequest(bucket, key, file)
      r.setCannedAcl(CannedAccessControlList.PublicReadWrite)
      client.putObject(r)
    }


    /** Delete file from bucket
      *
      * @param  bucket the bucket name
      * @param  key the key of file to delete
      * @return a successful future (no content) (or a failure)
      */
    def deleteFile(bucket: String, key: String): Future[Unit] = {

      val r = new DeleteObjectRequest(bucket, key)
      client.deleteObject(r)
    }


    def deleteFiles(bucket : String, commonPrefix : String) : Future[Seq[Unit]] = {
      import client.ec

      client.listObjects(bucket, commonPrefix).flatMap { objListing =>
        val allKeys = objListing.getObjectSummaries.listIterator().toList.map(_.getKey)
        Future.sequence(allKeys.map { key =>
          deleteFile(bucket, key)
          //val delObjReq = new DeleteObjectsRequest(bucket).withKeys(allKeys:_*)
          //client.deleteObjects(delObjReq)
        })

      }
    }

    /** Execute a block using the content of remote S3 file
      *
      * @param  bucket the bucket name
      * @param  key the key of file
      * @return a successful future of your something U (or a failure)
      */
    def withFile[U](bucket: String, key: String)(block: S3ObjectInputStream => U): Future[U] = {
      import client.ec

      client.getObject(bucket, key) map { o =>
        try {
          block(o.getObjectContent)
        } finally {
          if (o != null) o.close
        }
      }
    }

    /** Download of file as a stream with an optional inputstream transformation.
      *
      * @param bucket the bucket name
      * @param key the key of file
      * @param inputStreamTransform transformation function (for ZIP, GZIP decompression, ...)
      * @param chunkSize chunk size of the returned enumerator
      * @return an enumerator (stream) of the object
      */
    def getFileAsStream(bucket: String, key: String, inputStreamTransform: InputStream => InputStream = identity,
                             chunkSize: Int = defaultChunkSize): Source[ByteString] = {
      Source(client.getObject(bucket, key))
        .map(o => MFGSource.fromStream(inputStreamTransform(o.getObjectContent), chunkSize))
        .flatten(FlattenStrategy.concat)
    }

    def uncompressGzippedFileAsStream(bucket: String, key: String, chunkSize: Int = defaultChunkSize) =
      getFileAsStream(bucket, key, is => new GZIPInputStream(is), chunkSize)

    /** Sequential download of a multipart file as a reactive stream
      *
      * @param bucket bucket name
      * @param path the common path of the parts of the file
      * @param chunkSize
      * @return
      */
    def getMultipartFileAsStream(bucket: String, path: String, inputStreamTransform: InputStream => InputStream = identity,
                                 chunkSize: Int = defaultChunkSize): Source[ByteString] = {
      import scala.collection.JavaConversions._
      import client.ec

      listFilesAsStream(bucket, Some(path))
        .map { case (key, _) => getFileAsStream(bucket, key, inputStreamTransform, chunkSize) }
        .flatten(FlattenStrategy.concat)
    }

    /**
     * Streamed upload of a akka stream
     * @param  bucket the bucket name
     * @param  key the key of file
     * @param  source a source of array of bytes
     * @return a stream with only one element of type CompleteMultipartUploadResult if the upload was successful
     *         (otherwise, of an error happened during the upload, the stream fails)
     */
    def uploadFileAsStream(bucket: String, key: String, chunkUploadConcurrency: Int = 1): Flow[ByteString, CompleteMultipartUploadResult] = {
      import scala.collection.JavaConversions._
      import client.ec

      def initiateUpload(bucket: String, key: String) = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucket, key))

      Flow[ByteString]
        .via(MFGFlow.rechunkByteString(defaultChunkSize))
        .via(
          MFGFlow.customStatefulProcessor(initiateUpload(bucket, key)) {
            (futInitUpload, chunk) => (Some(futInitUpload), Vector((futInitUpload, chunk)))
          }
        )
        .mapAsync { case (futInitUpload, chunk) => futInitUpload.map(initUpload => (chunk, initUpload.getUploadId)) }
        .via(MFGFlow.zipWithIndex)
        .via(
          MFGFlow.mapAsyncUnorderedWithBoundedConcurrency(chunkUploadConcurrency) { case ((bytes, uploadId), partNumber) =>
            val uploadRequest = new UploadPartRequest()
              .withBucketName(bucket)
              .withKey(key)
              .withPartNumber((partNumber + 1).toInt)
              .withUploadId(uploadId)
              .withInputStream(new ByteArrayInputStream(bytes.toArray))
              .withPartSize(bytes.length)
            client.uploadPart(uploadRequest).map((_, uploadId))
          }
        )
        .via(
          MFGFlow.customStatefulProcessor[(UploadPartResult, String), Vector[(PartETag, String)], Future[CompleteMultipartUploadResult]](Vector.empty)(
            (etags, uploadResultAndUploadId) => {
              (Some(etags :+ (uploadResultAndUploadId._1.getPartETag, uploadResultAndUploadId._2)), Vector.empty)
            },
            lastPushIfUpstreamEnds = { etags =>
              val compRequest = new CompleteMultipartUploadRequest(bucket, key, uploadId, etags.toBuffer[PartETag])
              val futResult = client.completeMultipartUpload(compRequest).map { result =>
                result
              }.recoverWith { case e: Exception =>
                client.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, uploadId))
                Future.failed(e)
              }
              Vector(futResult)
            }
          )
        )
        .mapAsync(identity)
    }

    /**
     * periodically upload a stream to S3. Data is chuncked on a min(time,size) basis. Files are stored in a folder, named by their upload date
     * @param bucket the bucket name
     * @param prefix the folder where files will be saved
     * @param nbRecord maximum number of records to collect before dumping
     * @param duration maximum time window before dumping
     * @return
     */
    def uploadStreamMultipartFile(bucket: String, prefix: String, nbRecord: Int, duration: FiniteDuration, dateFormatter: DateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss_SSS"))(implicit fm: FlowMaterializer): Flow[Array[Byte], Int] =
      Flow[Array[Byte]].groupedWithin(nbRecord, duration)
        .via(
          MFGFlow.mapAsyncWithOrderedSideEffect { chunk => {
            val cleanPrefix = if (prefix.last.equals('/')) prefix else prefix + "/"
            val dStr = dateFormatter.format(new Date)
            uploadStream(bucket, cleanPrefix + dStr, Source(chunk))
          }
          })

    /**
     * uploadStreamMultipartFile + manage an second associated object, for callback use cases (exemple : queue acknowledgment)
     * @param bucket
     * @param prefix
     * @param nbRecord
     * @param duration
     * @tparam T
     * @return
     */
    def uploadStreamMultipartFileWithCompanion[T](bucket: String, prefix: String, nbRecord: Int, duration: FiniteDuration, dateFormatter: DateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss_SSS"))(implicit fm: FlowMaterializer): Flow[(Array[Byte], T), T] = {
      import scala.concurrent.ExecutionContext.Implicits.global
      Flow[(Array[Byte], T)].groupedWithin(nbRecord, duration)
        .via(
          MFGFlow.mapAsyncWithOrderedSideEffect { chunk => {
            val cleanPrefix = if (prefix.last.equals('/')) prefix else prefix + "/"
            val dStr = dateFormatter.format(new Date)
            val (data, companion) = chunk.unzip
            uploadStream(bucket, cleanPrefix + dStr, Source(data)).map(res => companion)
          }
          }).map(xs => Source(xs)).flatten(FlattenStrategy.concat)
    }
  }
}
