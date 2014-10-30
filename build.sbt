import sbtunidoc.Plugin._

organization in ThisBuild := "com.mfglabs"

name := "commons-aws"

scalaVersion in ThisBuild := "2.11.1"

version in ThisBuild := "0.1-SNAPSHOT"

resolvers in ThisBuild ++= Seq(
	  "Pellucid Deps" at "http://dl.bintray.com/content/pellucid/maven"
  , "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
)

scalacOptions in ThisBuild ++= Seq("-feature", "-deprecation", "-unchecked")

publishTo in ThisBuild := Some("MFGLabs Snapshots" at "s3://mfg-mvn-repo/snapshots")

publishMavenStyle in ThisBuild := true



lazy val all = (project in file("."))
  .aggregate  (core)
  .aggregate  (postgresExtensions)
  .settings   (site.settings ++ ghpages.settings: _*)
  .settings   (
    site.addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), "api/current"),
    git.remoteRepo := "git@github.com:MfgLabs/commons-aws.git"
  )

lazy val core = project.in(file("core"))
  .settings   (
    name := "commons-aws",
    libraryDependencies ++= Seq(
        Dependencies.Compile.awsJavaSDK
      , Dependencies.Compile.pellucidAwsWrap
      , Dependencies.Compile.playIteratees
      , Dependencies.Test.scalaTest
    )
  )

////////////////////////////////////////////////////////////////////////////////////////
// EXTENSIONS GO HERE
//


// If you want to add an extension, put it in directory "extensions"
// and add it to build as following.

lazy val postgresExtensions = project.in(file("extensions/postgres"))
  .dependsOn(core)
  .settings(
    name := "commons-aws-postgres",
    libraryDependencies ++= Seq(
      Dependencies.Compile.postgresDriver,
      Dependencies.Test.scalaTest
    )
  )

