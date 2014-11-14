package com.mfglabs.commons.aws.commons

import java.sql.{DriverManager, Connection}

import org.postgresql.PGConnection
import org.scalatest.{Suite, BeforeAndAfter, BeforeAndAfterAll}
import scala.sys.process._

trait DockerEnv extends BeforeAndAfter  {
  self : Suite => //required by BeforeAndAfter

  var dockerIdOpt: Option[String] = None

  Class.forName("org.postgresql.Driver")
  var conn : Connection = _

  def newPGDB() : Int = {
    val port: Int = 5432 + (math.random * (10000 - 5432)).toInt
    try {
      dockerIdOpt = Some(s"sudo docker run -v /etc/localtime:/etc/localtime:ro -p 127.0.0.1:$port  -d jipiboily/postgresql-9.3-for-ci".!!.trim)
      println("dockerId : " + dockerIdOpt)
      port
    } catch {
      case e: Exception =>
        newPGDB()
    }
  }
  before {

    val port = newPGDB()
    println(s"port : $port")
    Thread.sleep(2000)
    conn = DriverManager.getConnection(
      s"jdbc:postgresql://localhost:$port/postgres","ci", "ci")


    /*FakeApplication(additionalConfiguration =
      Map("db.default.url" -> s"jdbc:postgresql://localhost:$port/postgres" //, "db.default.user" -> "postgres", "db.default.password" -> ""
        , "db.default.user" -> "ci", "db.default.password" -> "ci", "db.default.acquireRetryAttempts" -> 10, "db.default.acquireRetryDelay" -> "1 seconds"))*/
    //, "db.default.connectionTimeout" -> "1 seconds"))
    //"db.default.user" -> "postgres", "db.default.password" -> ""))
  }

  after {
    conn.close()
    println(s"stop and rm docker container $dockerIdOpt")
    dockerIdOpt.foreach(dockerId => {
      val stopOUT = s"sudo docker stop $dockerId" !!;
      val rmOUT = s"sudo docker rm $dockerId" !!;
      println("docker stop : " + stopOUT + " / docker rm : " + rmOUT)
    })
  }

  //sudo docker ps -a | grep 'days ago' | awk '{print $1}' | sudo xargs docker stop && sudo docker ps -a | grep 'days ago' | awk '{print $1}' | sudo xargs docker rm
}

