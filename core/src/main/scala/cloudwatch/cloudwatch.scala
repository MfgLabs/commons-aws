package com.mfglabs.commons.aws
package cloudwatch

import java.util.concurrent.{Executors, ExecutorService, LinkedBlockingQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}
import scala.concurrent.{Future, Promise}
import scala.util.Try

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.{AmazonWebServiceRequest, ClientConfiguration}
import com.amazonaws.internal.StaticCredentialsProvider

import com.amazonaws.services.cloudwatch.model._
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClient

class AmazonCloudwatchClient(
    client: AmazonCloudWatchAsyncClient
) extends com.github.dwhjames.awswrap.cloudwatch.AmazonCloudWatchScalaClient (
  client
) {

  /**
    * make a client from a credentials provider, a config, and a default executor service.
    *
    * @param awsCredentialsProvider
    *     a provider of AWS credentials.
    * @param clientConfiguration
    *     a client configuration.
    */
  def this(awsCredentialsProvider: AWSCredentialsProvider, clientConfiguration: ClientConfiguration) = {
    this(new AmazonCloudWatchAsyncClient(
      awsCredentialsProvider,
      clientConfiguration,
      new ThreadPoolExecutor(
        0, clientConfiguration.getMaxConnections,
        60L, TimeUnit.SECONDS,
        new LinkedBlockingQueue[Runnable],
        new AWSThreadFactory("aws.wrap.cloudwatch")))
    )
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
    this(new AmazonCloudWatchAsyncClient(
      awsCredentialsProvider, new ClientConfiguration(), executorService
    ))
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
    this(new AmazonCloudWatchAsyncClient(
      new StaticCredentialsProvider(awsCredentials), clientConfiguration, executorService
    ))
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

}