import bintray.Plugin._

organization in ThisBuild := "com.mfglabs"

scalaVersion in ThisBuild := "2.11.11"

version in ThisBuild := "0.12.2-spark-2.2"

resolvers in ThisBuild ++= Seq(
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  Resolver.sonatypeRepo("releases"),
  DefaultMavenRepository,
  Resolver.bintrayRepo("mfglabs", "maven")
)

scalacOptions in ThisBuild ++= Seq(
  "-encoding", "UTF-8",
  "-target:jvm-1.8",
  "-Ydelambdafy:method",
  "-Yno-adapted-args",
  "-deprecation",
  "-feature",
  "-language:postfixOps",
  "-unchecked",
  "-Xfuture",
  "-Xlint",
  "-Xlint:-missing-interpolator",
  "-Xlint:private-shadow",
  "-Xlint:type-parameter-shadow",
  "-Ywarn-dead-code",
  "-Ywarn-unused",
  "-Ywarn-unused-import",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Xcheckinit"
)

publishMavenStyle in ThisBuild := true

lazy val commonSettings = Seq(
  scmInfo := Some(ScmInfo(
    url("https://github.com/MfgLabs/commons-aws"),
    "git@github.com:MfgLabs/commons-aws.git"
  ))
)

lazy val publishSettings = Seq(
  homepage := Some(url("https://github.com/MfgLabs/commons-aws")),
  licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")),
  autoAPIMappings := true,
  publishMavenStyle := true,
  publishArtifact in packageDoc := false,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false }
) ++ bintrayPublishSettings

lazy val noPublishSettings = Seq(
  publish := (),
  publishLocal := (),
  publishArtifact := false
)

lazy val all = (project in file("."))
  .aggregate(commons)
  .aggregate(s3)
  .aggregate(sqs)
  .settings(name := "commons-aws-all")
  .enablePlugins(ScalaUnidocPlugin)
  .settings(name := "commons-aws-all")
  .settings(noPublishSettings)

lazy val commons = project.in(file("commons"))
  .settings   (
    name := "commons-aws",
    libraryDependencies ++= Seq(
      Dependencies.Compile.awsJavaSDK,
      Dependencies.Compile.akkaStreamExt,
      Dependencies.Compile.slf4j,
      Dependencies.Test.scalaTest
    ),
    commonSettings,
    publishSettings
  )

lazy val s3 = project.in(file("s3"))
  .settings   (
    name := "commons-aws-s3",
    commonSettings,
    publishSettings
  ).dependsOn(commons % "compile->compile;test->test")

lazy val sqs = project.in(file("sqs"))
  .settings   (
    name := "commons-aws-sqs",
    commonSettings,
    publishSettings
  ).dependsOn(commons  % "compile->compile;test->test")
