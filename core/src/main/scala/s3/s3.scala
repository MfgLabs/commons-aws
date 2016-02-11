package com.mfglabs.commons.aws
package s3

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

import java.util.concurrent._

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.{AmazonWebServiceRequest, ClientConfiguration}
import com.amazonaws.internal.StaticCredentialsProvider

import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._

private[s3] class S3ThreadFactory extends ThreadFactory {
  private val count = new AtomicLong(0L)
  private val backingThreadFactory: ThreadFactory = Executors.defaultThreadFactory()
  override def newThread(r: Runnable): Thread = {
    val thread = backingThreadFactory.newThread(r)
    thread.setName(s"aws.wrap.s3-${count.getAndIncrement()}")
    thread
  }
}

/**
  * Asynchronous S3 Scala client on top of Pellucid's one.
  * Manage its own thread pool for blocking ops.
  */
class AmazonS3AsyncClient(
    val awsCredentialsProvider: AWSCredentialsProvider,
    val clientConfiguration:    ClientConfiguration,
    override val executorService: ExecutorService
) extends com.github.dwhjames.awswrap.s3.AmazonS3ScalaClient (
  awsCredentialsProvider,
  clientConfiguration,
  executorService
) {

  implicit val ec = ExecutionContext.fromExecutorService(executorService)

  /**
    * make a client from a credentials provider, a config, and a default executor service.
    *
    * @param awsCredentialsProvider
    *     a provider of AWS credentials.
    * @param clientConfiguration
    *     a client configuration.
    */
  def this(awsCredentialsProvider: AWSCredentialsProvider, clientConfiguration: ClientConfiguration) = {
    this(awsCredentialsProvider, clientConfiguration, Executors.newFixedThreadPool(clientConfiguration.getMaxConnections, new S3ThreadFactory()))
  }

  /**
    * make a client from a credentials provider, a default config, and an executor service.
    *
    * @param awsCredentialsProvider
    *     a provider of AWS credentials.
    * @param executorService
    *     an executor service for synchronous calls to the underlying AmazonS3Client.
    */
  def this(awsCredentialsProvider: AWSCredentialsProvider, executorService: ExecutorService) = {
    this(awsCredentialsProvider, new ClientConfiguration(), executorService)
  }

  /**
    * make a client from a credentials provider, a default config, and a default executor service.
    *
    * @param awsCredentialsProvider
    *     a provider of AWS credentials.
    */
  def this(awsCredentialsProvider: AWSCredentialsProvider) = {
    this(awsCredentialsProvider, new ClientConfiguration())
  }

  /**
    * make a client from credentials, a config, and an executor service.
    *
    * @param awsCredentials
    *     AWS credentials.
    * @param clientConfiguration
    *     a client configuration.
    * @param executorService
    *     an executor service for synchronous calls to the underlying AmazonS3Client.
    */
  def this(awsCredentials: AWSCredentials, clientConfiguration: ClientConfiguration, executorService: ExecutorService) = {
    this(new StaticCredentialsProvider(awsCredentials), clientConfiguration, executorService)
  }

  /**
    * make a client from credentials, a default config, and an executor service.
    *
    * @param awsCredentials
    *     AWS credentials.
    * @param executorService
    *     an executor service for synchronous calls to the underlying AmazonS3Client.
    */
  def this(awsCredentials: AWSCredentials, executorService: ExecutorService) = {
    this(awsCredentials, new ClientConfiguration(), executorService)
  }

  /**
    * make a client from credentials, a default config, and a default executor service.
    *
    * @param awsCredentials
    *     AWS credentials.
    */
  def this(awsCredentials: AWSCredentials) = {
    this(new StaticCredentialsProvider(awsCredentials))
  }

  /**
    * make a client from a default credentials provider, a config, and a default executor service.
    *
    * @param clientConfiguration
    *     a client configuration.
    */
  def this(clientConfiguration: ClientConfiguration) = {
    this(new DefaultAWSCredentialsProviderChain(), clientConfiguration)
  }

  /**
    * make a client from a default credentials provider, a default config, and a default executor service.
    */
  def this() = {
    this(new DefaultAWSCredentialsProviderChain())
  }


  @inline
  def wrapMethod[Request, Result](
    f:       Request => Result,
    request: Request
  ): Future[Result] = {
    val p = Promise[Result]
    executorService.execute(new Runnable {
      override def run() =
        p complete {
          Try {
            f(request)
          }
        }
    })
    p.future
  }


  /**
   * Asynchronous methods
   */

  def uploadPart(req: UploadPartRequest): Future[UploadPartResult] =
    wrapMethod[UploadPartRequest, UploadPartResult](client.uploadPart _, req)

  def abortMultipartUpload(req: AbortMultipartUploadRequest): Future[Unit] =
    wrapMethod[AbortMultipartUploadRequest, Unit](client.abortMultipartUpload _, req)

  def listNextBatchOfObjects(req: ObjectListing): Future[ObjectListing] =
    wrapMethod[ObjectListing, ObjectListing](client.listNextBatchOfObjects _, req)

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
    putObject(r)
  }

  /** Delete file from bucket
    *
    * @param  bucket the bucket name
    * @param  key the key of file to delete
    * @return a successful future (no content) (or a failure)
    */
  def deleteFile(bucket: String, key: String): Future[Unit] = {
    val r = new DeleteObjectRequest(bucket, key)
    deleteObject(r)
  }

  def deleteFiles(bucket: String, commonPrefix: String): Future[Seq[Unit]] = {
    import collection.JavaConversions._

    listObjects(bucket, commonPrefix).flatMap { objListing =>
      val allKeys = objListing.getObjectSummaries.listIterator().toList.map(_.getKey)
      Future.sequence(allKeys.map { key =>
        deleteFile(bucket, key)
      })
    }
  }

}


