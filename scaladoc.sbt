import sbtunidoc.Plugin._, UnidocKeys._
import scala.util.matching.Regex.Match


// substitue unidoc as the way to generate documentation
unidocSettings

packageDoc in Compile <<= packageDoc in ScalaUnidoc

artifact in (ScalaUnidoc, packageDoc) := {
  val previous: Artifact = (artifact in (ScalaUnidoc, packageDoc)).value
  previous.copy(classifier = Some("javadoc"))
}

scalacOptions in (Compile, doc) ++=
  Seq(
    "-implicits",
    "-sourcepath", baseDirectory.value.getAbsolutePath,
    "-doc-source-url", s"https://github.com/MfgLabs/commons-aws/tree/v${version.value}€{FILE_PATH}.scala")


autoAPIMappings := true

apiURL := Some(url("https://MfgLabs.github.io/commons-aws/api/current/"))

apiMappings ++= {
  val jarFiles = (dependencyClasspath in Compile).value.files
  val jarMap = jarFiles.find(file => file.toString.contains("com.amazonaws/aws-java-sdk")) match {
    case None => Map()
    case Some(awsJarFile) => Map(awsJarFile -> url("http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/"))
  }
  def findManagedDependency(organization: String, name: String): Option[File] = {
    (for {
      entry <- (fullClasspath in Runtime).value ++ (fullClasspath in Test).value
      module <- entry.get(moduleID.key) if module.organization == organization && module.name.startsWith(name)
    } yield entry.data).headOption
  }
  val links = Seq(
    findManagedDependency("org.scala-lang", "scala-library").map(d => d -> url(s"http://www.scala-lang.org/api/${scalaVersion.value}/"))
  )
  val linkMap = links.collect { case Some(d) => d }.toMap
  jarMap ++ linkMap ++ Map(
    file("/Library/Java/JavaVirtualMachines/jdk1.7.0_71.jdk/Contents/Home/jre/lib/rt.jar") -> 
    url("http://docs.oracle.com/javase/7/docs/api")
  )
}

lazy val transformJavaDocLinksTask = taskKey[Unit](
  "Transform JavaDoc links - replace #java.io.File with ?java/io/File.html"
)

transformJavaDocLinksTask := {
  val log = streams.value.log
  log.info("Transforming JavaDoc links")
  val t = (target in unidoc).value
  (t ** "*.html").get.filter(hasAwsJavadocApiLink).foreach { f =>
    log.info("Transforming " + f)
    val newContent = awsJavadocApiLink.replaceAllIn(IO.read(f), m =>
      "href=\"" + m.group(1) + "?" + m.group(2).replace(".", "/") + ".html")
    IO.write(f, newContent)
  }
}

val awsJavadocApiLink = """href=\"(http://docs\.aws\.amazon\.com/AWSJavaSDK/latest/javadoc/index\.html)#([^"]*)""".r
def hasAwsJavadocApiLink(f: File): Boolean = (awsJavadocApiLink findFirstIn IO.read(f)).nonEmpty

transformJavaDocLinksTask <<= transformJavaDocLinksTask triggeredBy (unidoc in Compile)
