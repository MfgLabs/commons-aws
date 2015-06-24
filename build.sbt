import sbtunidoc.Plugin._
import bintray.Plugin._

organization in ThisBuild := "com.mfglabs"

scalaVersion in ThisBuild := "2.11.6"

version in ThisBuild := "0.7.5-RC1"

resolvers in ThisBuild ++= Seq(
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  Resolver.sonatypeRepo("releases"),
  DefaultMavenRepository,
  Resolver.bintrayRepo("dwhjames", "maven"),
  Resolver.bintrayRepo("mfglabs", "maven")
)

scalacOptions in ThisBuild ++= Seq("-feature", "-unchecked", "-language:postfixOps")

publishMavenStyle in ThisBuild := true

lazy val commonSettings = Seq(
  scmInfo := Some(ScmInfo(url("https://github.com/MfgLabs/commons-aws"),
    "git@github.com:MfgLabs/commons-aws.git"))
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
  .aggregate  (core, cloudwatchHeartbeat)
  .settings   (name := "commons-aws-all")
  .settings   (site.settings ++ ghpages.settings: _*)
  .settings   (
    name := "commons-aws-all",
    site.addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), "api/current"),
    git.remoteRepo := "git@github.com:MfgLabs/commons-aws.git"
  )
  .settings(noPublishSettings)

lazy val core = project.in(file("core"))
  .settings   (
    name := "commons-aws",
    libraryDependencies ++= Seq(
      Dependencies.Compile.awsJavaSDK,
      Dependencies.Compile.pellucidAwsWrap,
      Dependencies.Compile.akkaStreamExt,
      Dependencies.Test.scalaTest
    ),
    commonSettings,
    publishSettings
  )

////////////////////////////////////////////////////////////////////////////////////////
// EXTENSIONS GO HERE
//

// If you want to add an extension, put it in directory "extensions"
// and add it to build as following.

lazy val cloudwatchHeartbeat = project.in(file("extensions/cloudwatch-heartbeat"))
  .dependsOn(core)
  .settings(
    name := "commons-aws-cloudwatch-heartbeat",
    libraryDependencies ++= Seq(
      Dependencies.Compile.akka,
      Dependencies.Compile.grizzled,
      Dependencies.Compile.logback,
      Dependencies.Test.scalaTest,
      Dependencies.Test.akkaTestkit
    ),
    commonSettings,
    publishSettings
  )
