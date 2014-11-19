package com.mfglabs.commons.aws.commons

import java.sql.{DriverManager, Connection}

import org.postgresql.PGConnection
import org.scalatest.{Suite, BeforeAndAfter, BeforeAndAfterAll}
import scala.sys.process._

/**
 * for each test, creates a PostgresSQL docker container and provides a connection to its database
 */
trait DockerTmpDB extends BeforeAndAfter  {
  self : Suite => //required by BeforeAndAfter

  var dockerIdOpt: Option[String] = None

  Class.forName("org.postgresql.Driver")
  implicit var conn : Connection = _

  def newPGDB() : Int = {
    val port: Int = 5432 + (math.random * (10000 - 5432)).toInt
    try {
      dockerIdOpt = Some(s"sudo docker run -v /etc/localtime:/etc/localtime:ro -p 127.0.0.1:$port:5432  -d jipiboily/postgresql-9.3-for-ci".!!.trim)
      println("dockerId : " + dockerIdOpt)
      port
    } catch {
      case e: Exception => // if the port is already allocated
        newPGDB()
    }
  }

  //ugly solution to wait for the connection to be ready
  def waitsForConnection(port : Int) : Connection = {
    try {
      DriverManager.getConnection(
        s"jdbc:postgresql://localhost:$port/postgres", "ci", "ci")
    } catch {
      case _ =>
        println("waits for connection to be ready ...")
        Thread.sleep(1000)
        waitsForConnection(port)
    }
  }

  before {
    val port = newPGDB()
    println(s"new database at port $port")
    conn = waitsForConnection(port)
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
}

