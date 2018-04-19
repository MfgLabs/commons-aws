import sbt._

object Dependencies {

  object V {
    val awsJavaSDK        = "1.11.199"
    val akkaStreamExt     = "0.11.2"
    val scalaTest         = "3.0.3"
    val slf4j             = "1.7.12"
  }

  object Compile {
    val awsJavaSDKcore    = "com.amazonaws"       %   "aws-java-sdk-core"             % V.awsJavaSDK
    val awsJavaSDKcw      = "com.amazonaws"       %   "aws-java-sdk-cloudwatch"       % V.awsJavaSDK
    val awsJavaSDKs3      = "com.amazonaws"       %   "aws-java-sdk-s3"               % V.awsJavaSDK
    val awsJavaSDKsqs     = "com.amazonaws"       %   "aws-java-sdk-sqs"              % V.awsJavaSDK

    val akkaStreamExt     = "com.mfglabs"         %%  "akka-stream-extensions"        % V.akkaStreamExt

    val slf4j             = "org.slf4j"           %   "slf4j-api"                     % V.slf4j
  }

  object Test {
    val scalaTest         = "org.scalatest"       %% "scalatest"            % V.scalaTest     % "test"
  }
}
