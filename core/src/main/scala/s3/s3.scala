package com.mfglabs.commons.aws
package s3

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

import java.util.concurrent.{Executors, ExecutorService, LinkedBlockingQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.{AmazonWebServiceRequest, ClientConfiguration}
import com.amazonaws.internal.StaticCredentialsProvider

import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._

class S3ThreadFactory extends ThreadFactory {
  private val count = new AtomicLong(0L)
  private val backingThreadFactory: ThreadFactory = Executors.defaultThreadFactory()
  override def newThread(r: Runnable): Thread = {
    val thread = backingThreadFactory.newThread(r)
    thread.setName(s"aws.wrap.s3-${count.getAndIncrement()}")
    thread
  }
}

/** Nastily hiding Pellucid client behind an MFG structure */
class AmazonS3Client(
    awsCredentialsProvider: AWSCredentialsProvider,
    clientConfiguration:    ClientConfiguration,
    override val executorService: ExecutorService
) extends com.pellucid.wrap.s3.AmazonS3ScalaClient (
  awsCredentialsProvider,
  clientConfiguration,
  executorService
) {

  implicit val executionContext = ExecutionContext.fromExecutorService(executorService)

  /**
    * make a client from a credentials provider, a config, and a default executor service.
    *
    * @param awsCredentialsProvider
    *     a provider of AWS credentials.
    * @param clientConfiguration
    *     a client configuration.
    */
  def this(awsCredentialsProvider: AWSCredentialsProvider, clientConfiguration: ClientConfiguration) = {
    this(awsCredentialsProvider, clientConfiguration,
      new ThreadPoolExecutor(
        0, clientConfiguration.getMaxConnections,
        60L, TimeUnit.SECONDS,
        new LinkedBlockingQueue[Runnable],
        new S3ThreadFactory()))
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

  /** More mirrored functions added just for MFG */
  def completeMultipartUpload(req: CompleteMultipartUploadRequest): Future[CompleteMultipartUploadResult] =
    wrapMethod[CompleteMultipartUploadRequest, CompleteMultipartUploadResult](client.completeMultipartUpload _, req)

  def initiateMultipartUpload(req: InitiateMultipartUploadRequest): Future[InitiateMultipartUploadResult] =
    wrapMethod[InitiateMultipartUploadRequest, InitiateMultipartUploadResult](client.initiateMultipartUpload _, req)

  def uploadPart(req: UploadPartRequest): Future[UploadPartResult] =
    wrapMethod[UploadPartRequest, UploadPartResult](client.uploadPart _, req)

  def abortMultipartUpload(req: AbortMultipartUploadRequest): Future[Unit] =
    wrapMethod[AbortMultipartUploadRequest, Unit](client.abortMultipartUpload _, req)

  def listNextBatchOfObjects(req: ObjectListing): Future[ObjectListing] =
    wrapMethod[ObjectListing, ObjectListing](client.listNextBatchOfObjects _, req)

  def putObject(req: PutObjectRequest): Future[PutObjectResult] =
    wrapMethod[PutObjectRequest, PutObjectResult](client.putObject _, req)

  def getObject(bucket: String, key: String): Future[S3Object] =
    wrapMethod[String, S3Object]({ bucket => client.getObject(bucket, key) }, bucket)

  def getObject(req: GetObjectRequest): Future[S3Object] =
    wrapMethod[GetObjectRequest, S3Object](client.getObject _, req)

  def getObject(req: GetObjectRequest, destinationFile: java.io.File): Future[ObjectMetadata] =
    wrapMethod[GetObjectRequest, ObjectMetadata]({ req => client.getObject(req, destinationFile) }, req)

}


