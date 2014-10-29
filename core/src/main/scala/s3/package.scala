package com.mfglabs.commons.aws

import com.mfglabs.commons.aws.s3.AmazonS3Client

import scala.concurrent.{ExecutionContext, Future, Promise}

import java.io.{ByteArrayInputStream, InputStream, File}
import java.util.Date

import com.amazonaws.services.s3.model._

import play.api.libs.iteratee._

/**
  * Enhances default AWS AmazonS3Client for Scala :
  *   - asynchronous/non-blocking wrapper using an external pool of threads managed internally.
  *   - a few file management/streaming facilities.
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

    /** List files in a bucket (& optional path)
      *
      * @param  bucket the bucket name
      * @param  path an optional path to search in bucket
      * @return a future of seq of file keys & last modified dates (or a failure)
      */
    def listFiles(bucket: String, path: Option[String] = None): Future[Seq[(String, Date)]] = {

      import collection.JavaConversions._

      // implicit exectx
      import client.executionContext

      def nextBatch(futObjectListing: Future[ObjectListing], objects: List[S3ObjectSummary]): Future[List[S3ObjectSummary]] =
        futObjectListing flatMap { l =>
          if(l.isTruncated) {
            nextBatch(client.listNextBatchOfObjects(l), objects ++ l.getObjectSummaries)
          } else {
            Future.successful(objects ++ l.getObjectSummaries)
          }
        }

      nextBatch(
        path match {
          case Some(p) => client.listObjects(bucket, p)
          case None => client.listObjects(bucket)
        },
        List.empty
      ) map { l => l map { x => (x.getKey, x.getLastModified) } }

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


    /** Execute a block using the content of remote S3 file
      *
      * @param  bucket the bucket name
      * @param  key the key of file
      * @param  a function from file content to something U
      * @return a successful future of your something U (or a failure)
      */
    def withFile[U](bucket: String, key: String)(block: S3ObjectInputStream => U): Future[U] = {
      // implicit exectx
      import client.executionContext

      client.getObject(bucket, key) map { o =>
        try {
          block(o.getObjectContent)
        } finally {
          if(o != null) o.close
        }
      }
    }

    /** Download of file as a reactive stream
     *
     * @param bucket the bucket name
     * @param key the key of file
     * @param chunkSize chunk size of the returned enumerator
     * @return an enumerator (stream) of the object
     */
    def getStream[U](bucket: String, key: String, chunkSize: Int = 5 * 1024 * 1024): Enumerator[Array[Byte]] = {
      import client.executionContext
      val futEnum = client.getObject(bucket, key).map(o => Enumerator.fromStream(o.getObjectContent, chunkSize))
      Enumerator.flatten(futEnum)
    }

    /** Sequential download of a multipart file as a reactive stream
     *
     * @param bucket bucket name
     * @param path the common path of the parts of the file
     * @param chunkSize
     * @return
     */
    def getStreamMultipartFile(bucket: String, path: String, chunkSize: Int = 5 * 1024 * 1024): Enumerator[Array[Byte]] = {
      import client.executionContext

      val futFilesKeys = listFiles(bucket, Some(path))
      val futEnum = futFilesKeys.flatMap { keys =>
        val sortedKeys = keys.map(_._1).sortWith { case (a, b) => a < b }
        sortedKeys.foldLeft(Future.successful(Enumerator.empty[Array[Byte]])) { case (futAcc, key) =>
          for {
            acc <- futAcc
            enum <- client.getObject(bucket, key).map(o => Enumerator.fromStream(o.getObjectContent, chunkSize))
          } yield acc andThen enum
        }
      }
      Enumerator.flatten(futEnum)
    }

    /** Streamed upload of a Play Enumerator
      *
      * @param  bucket the bucket name
      * @param  key the key of file
      * @param  enum an enumerator of array of bytes
      * @return a successful future of the uploaded number of chunks (or a failure)
      */
    def uploadStream(bucket: String, key: String, enum: Enumerator[Array[Byte]]): Future[Int] = {

      import scala.collection.JavaConversions._
      // implicit exectx
      import client.executionContext

      val rechunker: Enumeratee[Array[Byte], Array[Byte]] = Enumeratee.grouped {
        Traversable.takeUpTo[Array[Byte]](5 * 1024 * 1024) &>> Iteratee.consume()
      }

      def makeUploader(uploadId: String) =
        Iteratee.foldM[Array[Byte], Vector[PartETag]](Vector.empty) { case (etags, bytes) =>
          val uploadRequest = new UploadPartRequest()
                                  .withBucketName(bucket)
                                  .withKey(key)
                                  .withPartNumber(etags.length + 1)
                                  .withUploadId(uploadId)
                                  .withInputStream(new ByteArrayInputStream(bytes))
                                  .withPartSize(bytes.length)

          client.uploadPart(uploadRequest) map { res => etags :+ res.getPartETag }
        }

      client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucket, key)) flatMap { initResponse =>

        val uploadId = initResponse.getUploadId
        val maybeEtags = enum &> rechunker |>>> makeUploader(uploadId)

        maybeEtags flatMap { etags =>

          val compRequest = new CompleteMultipartUploadRequest(bucket, key, uploadId, etags.toBuffer[PartETag])

          client.completeMultipartUpload(compRequest) map { _ =>
            etags.length
          } recoverWith { case e: Exception =>
            client.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, uploadId))
            Future.failed(e)
          }
        }
      }

    }

  }
}