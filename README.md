## MFG Commons AWS Scala API

[Scaladoc](http://mfglabs.github.io/commons-aws/api/current/)

> Current stable version is `"com.mfglabs" %% "commons-aws" % "0.1"`


It enhances default AWS `AmazonS3Client` for Scala :
  - asynchronous/non-blocking wrapper using an external pool of threads managed internally.
  - Some custom extensions:
      - S3 to Postgres streaming facilities based on Play Iteratees
      - Cloudwatch heartbeat metric + alarm to provide simple server health monitoring.

> It is based on [Opensource Pellucid wrapper](https://github.com/pellucidanalytics/aws-wrap).

<br/>
<br/>
## Common

To use rich MFGLabs AWS wrapper, you just have to add the following to your `build.sbt` (plus the classic `~/.sbt/.s3credentials`):

```scala
resolvers ++= Seq(
  "MFG releases" at "s3://mfg-mvn-repo/releases",
  "MFG snapshots" at "s3://mfg-mvn-repo/snapshots",
  "MFG thirdparty" at "s3://mfg-mvn-repo/thirdparty",
  "Pellucid public" at "http://dl.bintray.com/content/pellucid/maven"
)


libraryDependencies ++= Seq(
...
  "com.mfglabs" %% "commons-aws" % "0.2-SNAPSHOT"
...
)

```

For now, it proposes out-of-the-box the following services:

<br/>
<br/>
## S3

In your code:

```scala
import com.mfglabs.commons.aws.s3
import s3._ // brings implicit extensions

// Create the client
val S3 = new AmazonS3Client()
// Use it
for {
  _   <- S3.uploadStream(bucket, "big.txt", Enumerator.fromFile(new java.io.File(s"big.txt")))
  l   <- S3.listFiles(bucket)
  _   <- S3.deleteFile(bucket, "big.txt")
  l2  <- S3.listFiles(bucket)
} yield (l, l2)
```

Please remark that you don't need any implicit `scala.concurrent.ExecutionContext` as it's directly provided
and managed by [[AmazonS3Client]] itself.

There are also smart `AmazonS3Client` constructors that can be provided with custom.
`java.util.concurrent.ExecutorService` if you want to manage your pools of threads.

<br/>
<br/>
## Cloudwatch

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


<br/>
<br/>
## Extensions


<br/>
<br/>
### Postgres extensions

It provides several utils to integrate AWS services with postgresql.

Add in your build.sbt:
```scala
libraryDependencies ++= Seq(
  "com.mfglabs" %% "commons-aws-postgres" % "0.1-SNAPSHOT"
)
```

You can for instance stream a multipart file from S3 directly to a postgres table:
```scala
val pgConnection = PostgresConnectionInfo("jdbc:postgresql:metadsp", "atamborrino", "password")
val S3 = new s3.AmazonS3Client()
val pg = new PostgresExtensions(pgConnection, S3)

val s3bucket = "mfg-test"
val s3path = "report.csv" // will stream sequentially report.csv.part0001, report.csv.part0002, ...

pg.streamMultipartFileFromS3(s3bucket, s3path, dbSchema = "public", dbTableName = "test_postgres_aws_s3")
```

#### Testing
You must have Docker installed (via boot2docker if you're using OSX) to run the tests (as a Postgres table is launched in a docker container
to perform the tests).

<br/>
<br/>
### Cloudwatch heartbeat

It provides a simple mechanism that sends periodically a heartbeat metric to AWS Cloudwatch.

If the heartbeat rate on a _configurable_ period falls under a _configurable_ threshold or the metrics isn't fed with sufficient data, a Cloudwatch `ALARM` status is triggered & sent to a given SQS endpoint.

When the rate goes above threshold again, an `OK` status is triggered & sent to the same SQS endpoint.


> **IMPORTANT**: the alarm is created by the API itself but due to a limitation (or a bug) in Amazon API, the status of this alarm will stay at `INSUFFICIENT_DATA` until you manually update it in the AWS console.
For that, wait 1/2 minutes after start so that Cloudwatch receives enough heartbeats and then select the alarm, click on `modify` and then click on `save`. The alarm should pass to `OK` status.

_Cloudwatch heartbeat is based on Cloudwatch service & Akka scheduler._

#### Low-level client

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

<br/>
#### Cakable client

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