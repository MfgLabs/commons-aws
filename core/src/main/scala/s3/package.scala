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

  // TODO: separate lazy methods (returning Source / Flow) from strict methods (returning Future)

  implicit class RichS3Client(val client: AmazonS3Client) extends AnyVal {
    import client.ec
    import client.fm
    import client.ecForBlockingOps

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

      def getFirstListing: Future[ObjectListing] = path match {
        case Some(p) => client.listObjects(bucket, p)
        case None => client.listObjects(bucket)
      }

      MFGSource.seededLazyAsync(getFirstListing) { firstListing =>
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
      * @return an enumerator (stream) of the object
      */
    def getFileAsStream(bucket: String, key: String, inputStreamTransform: InputStream => InputStream = identity): Source[ByteString] = {
      MFGSource.seededLazyAsync(client.getObject(bucket, key)) { o =>
        MFGSource.fromStream(inputStreamTransform(o.getObjectContent))
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
      import client.ec

      val uploadChunkSize = 8 * 1024 * 1024 // recommended by AWS

      def initiateUpload(bucket: String, key: String): Future[String] =
        client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucket, key)).map(_.getUploadId)

      Flow[ByteString]
        .via(MFGFlow.rechunkByteStringBySize(uploadChunkSize))
        .via(MFGFlow.zipWithConstantLazyAsync(initiateUpload(bucket, key)))
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
            client.uploadPart(uploadRequest).map(r => (r.getPartETag, uploadId)).recoverWith {
              case e: Exception =>
                client.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, uploadId))
                Future.failed(e)
            }
          }
        )
        .via(MFGFlow.fold(Vector.empty[(PartETag, String)])(_ :+ _))
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
        .via(MFGFlow.zipWithIndex)
        .splitWhen { case (_, i) => i != 0 && i % nbChunkPerFile == 0 }
        .map { partFileStream =>
          partFileStream.via(
            MFGFlow.withHead(includeHeadInUpStream = true) { case (_, i) =>
              val key = formatKey(i / nbChunkPerFile)
              Flow[(ByteString, Long)].map(_._1).via(uploadStreamAsFile(bucket, key, chunkUploadConcurrency))
            }
          )
        }
        .flatten(FlattenStrategy.concat) // change to FlattenStrategy.merge if we want to allow concurrent upload of part files
    }
  }

}
