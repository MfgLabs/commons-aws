import sbt._

object Dependencies {

  object V {
    val awsJavaSDK        = "1.8.10"
    val pellucidAwsWrap   = "0.6.1"
    val akkaStream        = "1.0-M2"
    val commonsStream     = "0.3-SNAPSHOT"
    val scalaTest         = "2.2.1"
    val postgresDriver           = "9.3-1102-jdbc4"
  }

  object Compile {
    val awsJavaSDK        = "com.amazonaws"       %   "aws-java-sdk"                  % V.awsJavaSDK
    val pellucidAwsWrap   = "com.pellucid"        %%  "aws-wrap"                      % V.pellucidAwsWrap
    val akkaStream        = "com.typesafe.akka"   %%  "akka-stream-experimental"      % V.akkaStream
    val commonsStream     = "com.mfglabs"         %%  "commons-stream"                % V.commonsStream
    val postgresDriver =     "org.postgresql"      % "postgresql"                     % V.postgresDriver
  }

  object Test {
    val scalaTest         = "org.scalatest"       %% "scalatest"            % V.scalaTest % "test"
  }
}
