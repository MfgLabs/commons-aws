/*
 * Copyright 2012-2015 Pellucid Analytics
 * Copyright 2015 Daniel W. H. James
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mfglabs.commons.aws
package s3

import java.io.{InputStream, File}
import java.net.URL

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

import com.amazonaws.services.s3.model._

/**
  * A lightweight wrapper for [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3Client.html AmazonS3Client]]
  *
  * The AWS Java SDK does not provide an asynchronous S3 client,
  * so this class follows the approach of the asynchronous clients
  * that are provided by the SDK. Namely, to make the synchronous
  * calls within an executor service. The methods in this class
  * all return Scala futures.
  *
  * @param client: the underlying AmazonS3Client
  * @param clientConfiguration
  * @param executorService
  *     an executor service for synchronous calls to the underlying AmazonS3Client.
  * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3Client.html AmazonS3Client]]
  * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html AWSCredentialsProvider]]
  * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/ClientConfiguration.html ClientConfiguration]]
  * @see java.util.concurrent.ExecutorService
  */
trait AmazonS3Wrapper extends FutureHelper.MethodWrapper {
  def client          : com.amazonaws.services.s3.AmazonS3Client
  def executorService : java.util.concurrent.ExecutorService

  implicit val ec = ExecutionContext.fromExecutorService(executorService)

  /**
    * Shutdown the client and the executor service.
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/AmazonWebServiceClient.html#shutdown() AmazonWebServiceClient.shutdown()]]
    */
  def shutdown(): Unit = {
    client.shutdown()
    executorService.shutdownNow()
    ()
  }

  /**
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#abortMultipartUpload(com.amazonaws.services.s3.model.AbortMultipartUploadRequest) AWS Java SDK]]
   */
  def abortMultipartUpload(req: AbortMultipartUploadRequest): Future[Unit] =
    wrapMethod[AbortMultipartUploadRequest, Unit](client.abortMultipartUpload _, req)

  /**
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#completeMultipartUpload(com.amazonaws.services.s3.model.CompleteMultipartUploadRequest) AWS Java SDK]]
   */
  def completeMultipartUpload(
    completeMultipartUploadRequest: CompleteMultipartUploadRequest
  ): Future[CompleteMultipartUploadResult] =
    wrapMethod(client.completeMultipartUpload, completeMultipartUploadRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#copyObject(com.amazonaws.services.s3.model.CopyObjectRequest) AWS Java SDK]]
    */
  def copyObject(
    copyObjectRequest: CopyObjectRequest
  ): Future[CopyObjectResult] =
    wrapMethod(client.copyObject, copyObjectRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#copyObject(com.amazonaws.services.s3.model.CopyObjectRequest) AWS Java SDK]]
    */
  def copyObject(
    sourceBucketName:      String,
    sourceKey:             String,
    destinationBucketName: String,
    destinationKey:        String
  ): Future[CopyObjectResult] =
    copyObject(new CopyObjectRequest(sourceBucketName, sourceKey, destinationBucketName, destinationKey))

  /**
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#copyPart(com.amazonaws.services.s3.model.CopyPartRequest) AWS Java SDK]]
   */
  def copyPart(
    copyPartRequest: CopyPartRequest
  ): Future[CopyPartResult] =
    wrapMethod(client.copyPart, copyPartRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#createBucket(com.amazonaws.services.s3.model.CreateBucketRequest) AWS Java SDK]]
    */
  def createBucket(
    createBucketRequest: CreateBucketRequest
  ): Future[Bucket] =
    wrapMethod[CreateBucketRequest, Bucket](client.createBucket, createBucketRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#createBucket(com.amazonaws.services.s3.model.CreateBucketRequest) AWS Java SDK]]
    */
  def createBucket(
    bucketName: String
  ): Future[Bucket] =
    createBucket(new CreateBucketRequest(bucketName))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#createBucket(com.amazonaws.services.s3.model.CreateBucketRequest) AWS Java SDK]]
    */
  def createBucket(
    bucketName: String,
    region:     Region
  ): Future[Bucket] =
    createBucket(new CreateBucketRequest(bucketName, region))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#createBucket(com.amazonaws.services.s3.model.CreateBucketRequest) AWS Java SDK]]
    */
  def createBucket(
    bucketName: String,
    region:     String
  ): Future[Bucket] =
    createBucket(new CreateBucketRequest(bucketName, region))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#deleteBucket(com.amazonaws.services.s3.model.DeleteBucketRequest) AWS Java SDK]]
    */
  def deleteBucket(
    deleteBucketRequest: DeleteBucketRequest
  ): Future[Unit] =
    wrapMethod[DeleteBucketRequest, Unit](client.deleteBucket, deleteBucketRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#deleteBucket(com.amazonaws.services.s3.model.DeleteBucketRequest) AWS Java SDK]]
    */
  def deleteBucket(
    bucketName: String
  ): Future[Unit] =
    deleteBucket(new DeleteBucketRequest(bucketName))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#deleteObject(com.amazonaws.services.s3.model.DeleteObjectRequest) AWS Java SDK]]
    */
  def deleteObject(
    deleteObjectRequest: DeleteObjectRequest
  ): Future[Unit] =
    wrapMethod(client.deleteObject, deleteObjectRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#deleteObject(com.amazonaws.services.s3.model.DeleteObjectRequest) AWS Java SDK]]
    */
  def deleteObject(
    bucketName: String,
    key:        String
  ): Future[Unit] =
    deleteObject(new DeleteObjectRequest(bucketName, key))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#deleteObjects(com.amazonaws.services.s3.model.DeleteObjectsRequest) AWS Java SDK]]
    */
  def deleteObjects(
    deleteObjectsRequest: DeleteObjectsRequest
  ): Future[Seq[DeleteObjectsResult.DeletedObject]] =
    wrapMethod((req: DeleteObjectsRequest) => client.deleteObjects(req).getDeletedObjects.asScala.toSeq, deleteObjectsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#deleteObjects(com.amazonaws.services.s3.model.DeleteObjectsRequest) AWS Java SDK]]
    */
  def deleteObjects(
    bucketName: String,
    keys:       String*
  ): Future[Seq[DeleteObjectsResult.DeletedObject]] = {
    deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keys:_ *))
  }

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#deleteVersion(com.amazonaws.services.s3.model.DeleteVersionRequest) AWS Java SDK]]
    */
  def deleteVersion(
    deleteVersionRequest: DeleteVersionRequest
  ): Future[Unit] =
    wrapMethod(client.deleteVersion, deleteVersionRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#deleteVersion(com.amazonaws.services.s3.model.DeleteVersionRequest) AWS Java SDK]]
    */
  def deleteVersion(
    bucketName: String,
    key:        String,
    versionId:  String
  ): Future[Unit] =
    deleteVersion(new DeleteVersionRequest(bucketName, key, versionId))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#doesBucketExist(java.lang.String) AWS Java SDK]]
    */
  def doesBucketExist(
    bucketName: String
  ): Future[Boolean] =
    wrapMethod(client.doesBucketExist, bucketName)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#getBucketLocation(com.amazonaws.services.s3.model.GetBucketLocationRequest) AWS Java SDK]]
    */
  def getBucketLocation(
    getBucketLocationRequest: GetBucketLocationRequest
  ): Future[String] =
    wrapMethod[GetBucketLocationRequest, String](client.getBucketLocation, getBucketLocationRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#getBucketLocation(com.amazonaws.services.s3.model.GetBucketLocationRequest) AWS Java SDK]]
    */
  def getBucketLocation(
    bucketName: String
  ): Future[String] =
    getBucketLocation(new GetBucketLocationRequest(bucketName))

  /**
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#generatePresignedUrl(com.amazonaws.services.s3.model.GeneratePresignedUrlRequest) AWS Java SDK]]
   */
  def generatePresignedUrlRequest(
    generatePresignedUrlRequest: GeneratePresignedUrlRequest
  ): Future[URL] =
    wrapMethod(client.generatePresignedUrl, generatePresignedUrlRequest)

  /**
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#getObject(com.amazonaws.services.s3.model.GetObjectRequest) AWS Java SDK]]
   */
  def getObject(
    getObjectRequest: GetObjectRequest
  ): Future[S3Object] =
    wrapMethod[GetObjectRequest, S3Object](client.getObject, getObjectRequest)

  /**
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#getObject(com.amazonaws.services.s3.model.GetObjectRequest) AWS Java SDK]]
   */
  def getObject(
    bucketName: String,
    key: String
  ): Future[S3Object] =
    getObject(new GetObjectRequest(bucketName, key))

  /**
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#getObject(com.amazonaws.services.s3.model.GetObjectRequest) AWS Java SDK]]
   */
  def getObject(
    getObjectRequest: GetObjectRequest,
    destinationFile: File
  ): Future[ObjectMetadata] =
    wrapMethod[(GetObjectRequest, File), ObjectMetadata]({ case (r: GetObjectRequest, f: File) => client.getObject(r, f) }, (getObjectRequest, destinationFile))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#getObjectMetadata(com.amazonaws.services.s3.model.GetObjectMetadataRequest) AWS Java SDK]]
    */
  def getObjectMetadata(
    getObjectMetadataRequest: GetObjectMetadataRequest
  ): Future[ObjectMetadata] =
    wrapMethod(client.getObjectMetadata, getObjectMetadataRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#getObjectMetadata(com.amazonaws.services.s3.model.GetObjectMetadataRequest) AWS Java SDK]]
    */
  def getObjectMetadata(
    bucketName: String,
    key:        String
  ): Future[ObjectMetadata] =
    getObjectMetadata(new GetObjectMetadataRequest(bucketName, key))

  /**
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#initiateMultipartUpload(com.amazonaws.services.s3.model.InitiateMultipartUploadRequest) AWS Java SDK]]
   */
  def initiateMultipartUpload(
    initiateMultipartUploadRequest: InitiateMultipartUploadRequest
  ): Future[InitiateMultipartUploadResult] =
    wrapMethod(client.initiateMultipartUpload, initiateMultipartUploadRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#listBuckets(com.amazonaws.services.s3.model.ListBucketsRequest) AWS Java SDK]]
    */
  def listBuckets(
    listBucketsRequest: ListBucketsRequest
  ): Future[Seq[Bucket]] =
    wrapMethod((req: ListBucketsRequest) => client.listBuckets(req).asScala.toSeq, listBucketsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#listBuckets(com.amazonaws.services.s3.model.ListBucketsRequest) AWS Java SDK]]
    */
  def listBuckets(): Future[Seq[Bucket]] =
    listBuckets(new ListBucketsRequest())

  /**
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#listNextBatchOfObjects(com.amazonaws.services.s3.model.ObjectListing) AWS Java SDK]]
   */
  def listNextBatchOfObjects(req: ObjectListing): Future[ObjectListing] =
    wrapMethod[ObjectListing, ObjectListing](client.listNextBatchOfObjects _, req)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#listObjects(com.amazonaws.services.s3.model.ListObjectsRequest) AWS Java SDK]]
    */
  def listObjects(
    listObjectsRequest: ListObjectsRequest
  ): Future[ObjectListing] =
    wrapMethod[ListObjectsRequest, ObjectListing](client.listObjects, listObjectsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#listObjects(com.amazonaws.services.s3.model.ListObjectsRequest) AWS Java SDK]]
    */
  def listObjects(
    bucketName: String
  ): Future[ObjectListing] =
    listObjects(
      new ListObjectsRequest()
      .withBucketName(bucketName)
    )

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#listObjects(com.amazonaws.services.s3.model.ListObjectsRequest) AWS Java SDK]]
    */
  def listObjects(
    bucketName: String,
    prefix:     String
  ): Future[ObjectListing] =
    listObjects(
      new ListObjectsRequest()
      .withBucketName(bucketName)
      .withPrefix(prefix)
    )

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#listVersions(com.amazonaws.services.s3.model.ListVersionsRequest) AWS Java SDK]]
    */
  def listVersions(
    listVersionsRequest: ListVersionsRequest
  ): Future[VersionListing] =
      wrapMethod(client.listVersions, listVersionsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#listVersions(com.amazonaws.services.s3.model.ListVersionsRequest) AWS Java SDK]]
    */
  def listVersions(
    bucketName: String,
    prefix:     String
  ): Future[VersionListing] =
    listVersions(
      new ListVersionsRequest()
      .withBucketName(bucketName)
      .withPrefix(prefix)
    )

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#listVersions(com.amazonaws.services.s3.model.ListVersionsRequest) AWS Java SDK]]
    */
  def listVersions(
    bucketName:      String,
    prefix:          String,
    keyMarker:       String,
    versionIdMarker: String,
    delimiter:       String,
    maxKeys:         Int
  ): Future[VersionListing] =
    listVersions(new ListVersionsRequest(bucketName, prefix, keyMarker, versionIdMarker, delimiter, maxKeys))

  /**
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#putObject(com.amazonaws.services.s3.model.PutObjectRequest) AWS Java SDK]]
   */
  def putObject(
    putObjectRequest: PutObjectRequest
  ): Future[PutObjectResult] =
    wrapMethod[PutObjectRequest, PutObjectResult](client.putObject, putObjectRequest)

  /**
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#putObject(com.amazonaws.services.s3.model.PutObjectRequest) AWS Java SDK]]
   */
  def putObject(
    bucketName: String,
    key:        String,
    file:       File
  ): Future[PutObjectResult] =
    putObject(new PutObjectRequest(bucketName, key, file))

  /**
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#putObject(com.amazonaws.services.s3.model.PutObjectRequest) AWS Java SDK]]
   */
  def putObject(
    bucketName: String,
    key: String,
    input: InputStream,
    metadata: ObjectMetadata
  ): Future[PutObjectResult] =
    putObject(new PutObjectRequest(bucketName, key, input, metadata))

  /**
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#uploadPart(com.amazonaws.services.s3.model.UploadPartRequest) AWS Java SDK]]
   */
  def uploadPart(req: UploadPartRequest): Future[UploadPartResult] =
    wrapMethod[UploadPartRequest, UploadPartResult](client.uploadPart _, req)

}
