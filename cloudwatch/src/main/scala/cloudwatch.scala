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
package cloudwatch

import java.util.concurrent.ExecutorService
import scala.concurrent.Future

import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.ClientConfiguration
import com.amazonaws.client.builder.ExecutorFactory

import com.amazonaws.services.cloudwatch.{AmazonCloudWatchAsync, AmazonCloudWatchAsyncClientBuilder}
import com.amazonaws.services.cloudwatch.model._


object AmazonCloudwatchClient {
  import com.amazonaws.auth.{AWSCredentials, AWSStaticCredentialsProvider}
  import FutureHelper.defaultExecutorService
  import com.amazonaws.regions.Regions

  def apply(
    region              : Regions,
    awsCredentials      : AWSCredentials,
    clientConfiguration : ClientConfiguration = new ClientConfiguration()
  )(
    executorService     : ExecutorService     = defaultExecutorService(clientConfiguration, "aws.wrap.cloudwatch")
  ): AmazonCloudwatchClient = {
    from(region, new AWSStaticCredentialsProvider(awsCredentials), clientConfiguration)(executorService)
  }

  def from(
    region                 : Regions,
    awsCredentialsProvider : AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain,
    clientConfiguration    : ClientConfiguration    = new ClientConfiguration()
  )(
    executorService        : ExecutorService        = defaultExecutorService(clientConfiguration, "aws.wrap.cloudwatch")
  ): AmazonCloudwatchClient = new AmazonCloudwatchClient(
    AmazonCloudWatchAsyncClientBuilder
      .standard()
      .withRegion(region)
      .withCredentials(awsCredentialsProvider)
      .withClientConfiguration(clientConfiguration)
      .withExecutorFactory(new ExecutorFactory { def newExecutor() = executorService })
      .build()
  )

}

/**
 * A lightweight wrapper for [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDBAsyncClient.html AmazonCloudWatchAsyncClient]].
 *
 * @constructor construct a wrapper client from an Amazon async client.
 * @param client
  *     the underlying [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDBAsyncClient.html AmazonCloudWatchAsyncClient]].
 * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDBAsyncClient.html AmazonCloudWatchAsyncClient]]
 */
class AmazonCloudwatchClient(val client: AmazonCloudWatchAsync) {
  import FutureHelper._

  /**
  * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#deleteAlarms(com.amazonaws.services.cloudwatch.model.DeleteAlarmsRequest) AWS Java SDK]]
  */
  def deleteAlarms(
    deleteAlarmsRequest: DeleteAlarmsRequest
  ): Future[DeleteAlarmsResult] =
    wrapAsyncMethod(client.deleteAlarmsAsync, deleteAlarmsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#deleteAlarms(com.amazonaws.services.cloudwatch.model.DeleteAlarmsRequest) AWS Java SDK]]
    */
  def deleteAlarms(alarmNames: String*): Future[DeleteAlarmsResult] =
    deleteAlarms(new DeleteAlarmsRequest().withAlarmNames(alarmNames: _*))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#describeAlarmHistory(com.amazonaws.services.cloudwatch.model.DescribeAlarmHistoryRequest) AWS Java SDK]]
    */
  def describeAlarmHistory(
    describeAlarmHistoryRequest: DescribeAlarmHistoryRequest
  ): Future[DescribeAlarmHistoryResult] =
    wrapAsyncMethod(client.describeAlarmHistoryAsync, describeAlarmHistoryRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#describeAlarmHistory() AWS Java SDK]]
    */
  def describeAlarmHistory(): Future[DescribeAlarmHistoryResult] =
    describeAlarmHistory(new DescribeAlarmHistoryRequest())

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#describeAlarms(com.amazonaws.services.cloudwatch.model.DescribeAlarmsRequest) AWS Java SDK]]
    */
  def describeAlarms(
    describeAlarmRequest: DescribeAlarmsRequest
  ): Future[DescribeAlarmsResult] =
    wrapAsyncMethod(client.describeAlarmsAsync, describeAlarmRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#describeAlarms() AWS Java SDK]]
    */
  def describeAlarms(): Future[DescribeAlarmsResult] =
    describeAlarms(new DescribeAlarmsRequest())

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#describeAlarmsForMetric(com.amazonaws.services.cloudwatch.model.DescribeAlarmsForMetricRequest) AWS Java SDK]]
    */
  def describeAlarmsForMetric(
    describeAlarmsForMetricRequest: DescribeAlarmsForMetricRequest
  ): Future[DescribeAlarmsForMetricResult] =
    wrapAsyncMethod(client.describeAlarmsForMetricAsync, describeAlarmsForMetricRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#disableAlarmActions(com.amazonaws.services.cloudwatch.model.DisableAlarmActionsRequest) AWS Java SDK]]
    */
  def disableAlarmActions(
    disableAlarmActionsRequest: DisableAlarmActionsRequest
  ): Future[DisableAlarmActionsResult] =
    wrapAsyncMethod(client.disableAlarmActionsAsync, disableAlarmActionsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#disableAlarmActions(com.amazonaws.services.cloudwatch.model.DisableAlarmActionsRequest) AWS Java SDK]]
    */
  def disableAlarmActions(alarmNames: String*): Future[DisableAlarmActionsResult] =
    disableAlarmActions(new DisableAlarmActionsRequest().withAlarmNames(alarmNames: _*))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#enableAlarmActions(com.amazonaws.services.cloudwatch.model.EnableAlarmActionsRequest) AWS Java SDK]]
    */
  def enableAlarmActions(
    enableAlarmActionsRequest: EnableAlarmActionsRequest
  ): Future[EnableAlarmActionsResult] =
    wrapAsyncMethod(client.enableAlarmActionsAsync, enableAlarmActionsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#enableAlarmActions(com.amazonaws.services.cloudwatch.model.EnableAlarmActionsRequest) AWS Java SDK]]
    */
  def enableAlarmActions(alarmNames: String*): Future[EnableAlarmActionsResult] =
    enableAlarmActions(new EnableAlarmActionsRequest().withAlarmNames(alarmNames: _*))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#getMetricStatistics(com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest) AWS Java SDK]]
    */
  def getMetricStatistics(
    getMetricStatisticsRequest: GetMetricStatisticsRequest
  ): Future[GetMetricStatisticsResult] =
    wrapAsyncMethod(client.getMetricStatisticsAsync, getMetricStatisticsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#listMetrics(com.amazonaws.services.cloudwatch.model.ListMetricsRequest) AWS Java SDK]]
    */
  def listMetrics(
    listMetricsRequest: ListMetricsRequest
  ): Future[ListMetricsResult] =
    wrapAsyncMethod(client.listMetricsAsync, listMetricsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#listMetrics() AWS Java SDK]]
    */
  def listMetrics(): Future[ListMetricsResult] =
    listMetrics(new ListMetricsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#putMetricAlarm(com.amazonaws.services.cloudwatch.model.PutMetricAlarmRequest) AWS Java SDK]]
    */
  def putMetricAlarm(
    putMetricAlarmRequest: PutMetricAlarmRequest
  ): Future[PutMetricAlarmResult] =
    wrapAsyncMethod(client.putMetricAlarmAsync, putMetricAlarmRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#putMetricData(com.amazonaws.services.cloudwatch.model.PutMetricDataRequest) AWS Java SDK]]
    */
  def putMetricData(
    putMetricDataRequest: PutMetricDataRequest
  ): Future[PutMetricDataResult] =
    wrapAsyncMethod(client.putMetricDataAsync, putMetricDataRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#putMetricData(com.amazonaws.services.cloudwatch.model.PutMetricDataRequest) AWS Java SDK]]
    */
  def putMetricData(
    namespace:  String,
    metricData: Iterable[MetricDatum]
  ): Future[PutMetricDataResult] = {
    import scala.collection.JavaConversions.asJavaCollection

    putMetricData(
      new PutMetricDataRequest()
      .withNamespace(namespace)
      .withMetricData(metricData)
    )
  }

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#setAlarmState(com.amazonaws.services.cloudwatch.model.SetAlarmStateRequest) AWS Java SDK]]
    */
  def setAlarmState(
    setAlarmStateRequest: SetAlarmStateRequest
  ): Future[SetAlarmStateResult] =
    wrapAsyncMethod(client.setAlarmStateAsync, setAlarmStateRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#setAlarmState(com.amazonaws.services.cloudwatch.model.SetAlarmStateRequest) AWS Java SDK]]
    */
  def setAlarmState(
    alarmName: String,
    stateReason: String,
    stateValue: StateValue,
    stateReasonData: String = ""
  ): Future[SetAlarmStateResult] =
    setAlarmState(
      new SetAlarmStateRequest()
      .withAlarmName(alarmName)
      .withStateReason(stateReason)
      .withStateValue(stateValue)
      .withStateReasonData(stateReasonData)
    )

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#shutdown() AWS Java SDK]]
    */
  def shutdown(): Unit =
    client.shutdown()

}
