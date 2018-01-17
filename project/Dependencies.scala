import sbt._

object Dependencies {

  object V {
    val awsJavaSDK        = "1.7.4"
    val akkaStreamExt     = "0.11.2"
    val scalaTest         = "3.0.3"
    val slf4j             = "1.7.12"
  }

  object Compile {
    val awsJavaSDK        = "com.amazonaws"       %   "aws-java-sdk"             % V.awsJavaSDK
    val akkaStreamExt     = "com.mfglabs"         %%  "akka-stream-extensions"   % V.akkaStreamExt
    val slf4j             = "org.slf4j"           %   "slf4j-api"                % V.slf4j
  }

  object Test {
    val scalaTest         = "org.scalatest"       %% "scalatest"            % V.scalaTest     % "test"
  }
}
