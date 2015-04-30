# Streaming / asynchronous Scala client for common AWS services

Streaming / asynchronous Scala client for common AWS services on top of [dwhjames/aws-wrap](https://github.com/dwhjames/aws-wrap)
. When possible, clients expose methods that return Akka Stream's Sources / Flows / Sinks to provide streaming facilities.

Clients use a pool of threads managed internally and optimized for blocking IO operations.

## Resolver

```scala
resolvers ++= Seq(
  Resolver.bintrayRepo("mfglabs", "maven"),
  Resolver.bintrayRepo("dwhjames", "maven")
)
```

## Dependencies

```scala
libraryDependencies += "com.mfglabs" %% "commons-aws" % "0.7"
```

## Use

### Commons

#### S3

```scala
import com.mfglabs.commons.aws.s3._

val builder = S3StreamBuilder(new AmazonS3AsyncClient()) // contains un-materialized composable Source / Flow / Sink

val fileStream: Source[ByteString] = builder.getFileAsStream(bucket, key)

val multipartfileStream: Source[ByteString] = builder.getMultipartFileAsStream(bucket, prefix)

someBinaryStream.via(
  builder.uploadStreamAsFile(bucket, key, chunkUploadConcurrency = 2)
)

someBinaryStream.via(
  builder.uploadStreamAsMultipartFile(
    bucket, 
    prefix, 
    nbChunkPerFile = 10000, 
    chunkUploadConcurrency = 2
  )
)

val ops = new builder.MaterializedOps(flowMaterializer) // contains materialized methods on top of S3Stream

val file: Future[ByteString] = ops.getFile(bucket, key)

// More methods, check the source code
```

Please remark that you don't need any implicit `scala.concurrent.ExecutionContext` as it's directly provided
and managed by [[AmazonS3Client]] itself.

There are also smart `AmazonS3Client` constructors that can be provided with custom.
`java.util.concurrent.ExecutorService` if you want to manage your pools of threads.


#### SQS

```scala
import com.amazonaws.services.sqs.AmazonSQSAsyncClient 
import com.pellucid.wrap.sqs.AmazonSQSScalaClient
import com.mfglabs.commons.aws.sqs._

val sqs = new AmazonSQSScalaClient(new AmazonSQSAsyncClient(), ec)
val builder = SQSStreamBuilder(sqs)

val sender: Flow[String, SendMessageResult] =
  Flow[String].map { body =>
    val req = new SendMessageRequest()
    req.setMessageBody(body)
    req.setQueueUrl(queueUrl)
    req
  }
  .via(builder.sendMessageAsStream)

val receiver: Source[Message] = builder.receiveMessageAsStream(queueUrl, autoAck = false)
```

#### Cloudwatch

In your code:

```scala
import com.mfglabs.commons.aws.cloudwatch
import cloudwatch._ // brings implicit extensions

// Create the client
val CW = new cloudwatch.AmazonCloudwatchClient()

// Use it
for {
  metrics  <- CW.listMetrics()
} yield (metrics)
```

Please remark that you don't need any implicit `scala.concurrent.ExecutionContext` as it's directly provided
and managed by [[AmazonCloudwatchClient]] itself.

There are also smart `AmazonCloudwatchClient` constructors that can be provided with custom.
`java.util.concurrent.ExecutorService` if you want to manage your pools of threads.

### Extensions

#### Cloudwatch heartbeat

It provides a simple mechanism that sends periodically a heartbeat metric to AWS Cloudwatch.

If the heartbeat rate on a _configurable_ period falls under a _configurable_ threshold or the metrics isn't fed with sufficient data, a Cloudwatch `ALARM` status is triggered & sent to a given SQS endpoint.

When the rate goes above threshold again, an `OK` status is triggered & sent to the same SQS endpoint.

> **IMPORTANT**: the alarm is created by the API itself but due to a limitation (or a bug) in Amazon API, the status of this alarm will stay at `INSUFFICIENT_DATA` until you manually update it in the AWS console.
For that, wait 1/2 minutes after start so that Cloudwatch receives enough heartbeats and then select the alarm, click on `modify` and then click on `save`. The alarm should pass to `OK` status.

_Cloudwatch heartbeat is based on Cloudwatch service & Akka scheduler._

##### Low-level client

```scala
import com.mfglabs.commons.aws.cloudwatch
import com.mfglabs.commons.aws.extensions.cloudwatch.CloudwatchAkkaHeartbeat

import myExecutionCtx // an implicit custom execution context

val hb = new CloudwatchAkkaHeartbeat(
  namespace = "Test/Heartbeat",         // the namespace of the cloudwatch metrics
  name = "test1",                       // the name of the cloudwatch
  beatPeriod = 2.second,                // the heart beat period in Scala.concurrent.duration.Duration string format
  alarmPeriod = 120.seconds,            // the period on which the metrics is analyzed to determine the heartbeat health
  alarmPeriodNb = 1,                    // the number of "bad health" periods after which the alarm is triggered
  alarmThreshold = 10,                  // the threshold counting the number of heartbeats on a period under which the "bad health" is detected
  system = system,                      // the Akka system to create scheduler
  client = CW,                          // the cloudwatch client
  actionEndpoint = "arn:aws:sns:eu-west-1:896733075612:Cloudwatch-HeartBeat-Test" // the actionEndpoint (SQS) to which Cloudwatch will send the alarm
)

hb.start() // to start the heartbeat

hb.stop() // to stop the heartbeat
```

> Please note that you need to provide an implicit `ExecutionContext` for `CloudwatchAkkaHeartbeat.start/stop`

##### Cakable client

`CloudwatchHeartbeatLayer` is ready to be used in a cake pattern

```scala
object MyAkkaService extends CloudwatchHeartbeatLayer {
  override val system = myAkkaSystem

  override val heartbeatClient = myCloudClient

  override val heartbeatName: String = ...
  override val heartbeatNamespace: String = ...

  override val heartbeatPeriod: FiniteDuration = ...
  override val heartbeatAlarmPeriod: FiniteDuration = ...
  override val heartbeatAlarmPeriodNb: Int = ...
  override val heartbeatAlarmThreshold: Int = ...
  override val heartbeatEndpoint: String = ...

  ...
  // start the heartbeat
  heartbeat.start()(myExeCtx)
}
```

## License

This software is licensed under the Apache 2 license, quoted below.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.