import sbt._

object Dependencies {

  object V {
    val awsJavaSDK        = "1.10.77"
    val akkaStreamExt     = "0.10.0"
    val scalaTest         = "2.2.1"
    val grizzled          = "1.0.2"
    val logback           = "1.1.1"
    val akka              = "2.3.4"
    val slf4j             = "1.7.12"
  }

  object Compile {
    val awsJavaSDKcw      = "com.amazonaws"       %   "aws-java-sdk-cloudwatch"       % V.awsJavaSDK
    val awsJavaSDKs3      = "com.amazonaws"       %   "aws-java-sdk-s3"               % V.awsJavaSDK
    val awsJavaSDKsqs     = "com.amazonaws"       %   "aws-java-sdk-sqs"              % V.awsJavaSDK

    val akkaStreamExt     = "com.mfglabs"         %%  "akka-stream-extensions"        % V.akkaStreamExt
    val akka              = "com.typesafe.akka"   %%  "akka-actor"                    % V.akka
    val grizzled          = "org.clapper"         %%  "grizzled-slf4j"                % V.grizzled

    val slf4j             = "org.slf4j"           %   "slf4j-api"                     % V.slf4j
    val logback           = "ch.qos.logback"      %   "logback-classic"               % V.logback           % "runtime"
  }

  object Test {
    val scalaTest         = "org.scalatest"       %% "scalatest"            % V.scalaTest     % "test"
    val akkaTestkit       = "com.typesafe.akka"   %%  "akka-testkit"        % V.akka          % "test"
  }
}
