import scala.util.matching.Regex.Match

packageDoc in Compile <<= packageDoc in ScalaUnidoc

artifact in (ScalaUnidoc, packageDoc) := {
  val previous: Artifact = (artifact in (ScalaUnidoc, packageDoc)).value
  previous.copy(classifier = Some("javadoc"))
}

scalacOptions in (Compile, doc) ++= Seq(
  "-implicits",
  "-sourcepath", baseDirectory.value.getAbsolutePath,
  "-doc-source-url", s"https://github.com/MfgLabs/commons-aws/tree/${version.value}€{FILE_PATH}.scala"
)

autoAPIMappings := true

apiURL := Some(url("https://MfgLabs.github.io/commons-aws/api/current/"))
