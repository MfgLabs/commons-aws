# Streaming / asynchronous Scala client for common AWS services

Streaming / asynchronous Scala client for common AWS services.
When possible, clients expose methods that return Akka Stream's Sources / Flows / Sinks to provide streaming facilities.

Clients use a pool of threads managed internally and optimized for blocking IO operations.

This library makes heavy use of our extension library for Akka Stream
[MfgLabs/akka-stream-extensions](https://github.com/MfgLabs/akka-stream-extensions).

## Resolver

```scala
resolvers ++= Seq(
  Resolver.bintrayRepo("mfglabs", "maven")
)
```

## Dependencies

Three packages are available :
```scala
libraryDependencies += "com.mfglabs" %% "commons-aws-cloudwatch" % "0.12.0"
libraryDependencies += "com.mfglabs" %% "commons-aws-s3" % "0.12.0"
libraryDependencies += "com.mfglabs" %% "commons-aws-sqs" % "0.12.0"
```

Changelog [here](CHANGELOG.md)

## Usage

> Scaladoc is available [there](http://mfglabs.github.io/commons-aws/api/current/)

### Commons

#### S3

```scala
import com.mfglabs.commons.aws.s3._

val client = AmazonS3Client()()) // client with un-materialized composable Source / Flow / Sink

val fileStream: Source[ByteString, Unit] = client.getFileAsStream(bucket, key)

val multipartfileStream: Source[ByteString, Unit] = client.getMultipartFileAsStream(bucket, prefix)

someBinaryStream.via(
  client.uploadStreamAsFile(bucket, key, chunkUploadConcurrency = 2)
)

someBinaryStream.via(
  client.uploadStreamAsMultipartFile(
    bucket,
    prefix,
    nbChunkPerFile = 10000,
    chunkUploadConcurrency = 2
  )
)

val ops = client.materialized(flowMaterializer) // client with added materialized methods

val file: Future[ByteString] = ops.getFile(bucket, key)

// More methods, check the source code
```

Please remark that you don't need any implicit `scala.concurrent.ExecutionContext` as it's directly provided
and managed by [[AmazonS3Client]] itself.

There are also smart `AmazonS3Client` constructors that can be provided with custom.
`java.util.concurrent.ExecutorService` if you want to manage your pools of threads.


#### SQS

```scala
import com.mfglabs.commons.aws.sqs._

val sqs = AmazonSQSClient()()

val sender: Flow[String, SendMessageResult, Unit] =
  Flow[String].map { body =>
    val req = new SendMessageRequest()
    req.setMessageBody(body)
    req.setQueueUrl(queueUrl)
    req
  }
  .via(sqs.sendMessageAsStream())

val receiver: Source[Message, Unit] = sqs.receiveMessageAsStream(queueUrl, autoAck = false)
```

Please remark that you don't need any implicit `scala.concurrent.ExecutionContext` as it's directly provided
and managed by [[AmazonSQSClient]] itself.

There are also smart `AmazonSQSClient` constructors that can be provided with custom.
`java.util.concurrent.ExecutorService` if you want to manage your pools of threads.

#### Cloudwatch

In your code:

```scala
import com.mfglabs.commons.aws.cloudwatch
import cloudwatch._ // brings implicit extensions

// Create the client
val CW = cloudwatch.AmazonCloudwatchClient()()

// Use it
for {
  metrics  <- CW.listMetrics()
} yield (metrics)
```

Please remark that you don't need any implicit `scala.concurrent.ExecutionContext` as it's directly provided
and managed by [[AmazonCloudwatchClient]] itself.

There are also smart `AmazonCloudwatchClient` constructors that can be provided with custom.
`java.util.concurrent.ExecutorService` if you want to manage your pools of threads.

## License

This software is licensed under the Apache 2 license, quoted below.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
