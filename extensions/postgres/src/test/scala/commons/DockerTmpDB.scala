package com.mfglabs.commons.aws.commons

import java.sql.{DriverManager, Connection}

import org.postgresql.PGConnection
import org.scalatest.{Suite, BeforeAndAfter, BeforeAndAfterAll}
import scala.sys.process._
import scala.util.Try

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
      dockerIdOpt = Some(s"docker run -p $port:5432 -e POSTGRES_PASSWORD=pwd -d postgres:9.3".!!.trim)
      println("dockerId : " + dockerIdOpt)
      port
    } catch {
      case e: Exception => // if the port is already allocated
        newPGDB()
    }
  }

  def getDockerIp: String = Try("boot2docker ip".!!.trim).getOrElse("127.0.0.1") // platform dependent

  //ugly solution to wait for the connection to be ready
  def waitsForConnection(port : Int) : Connection = {
    try {
      DriverManager.getConnection(
        s"jdbc:postgresql://$getDockerIp:$port/postgres", "postgres", "pwd")
    } catch {
      case err: Exception =>
        println(s"Error while trying to connect to db: $err")
        println("Retrying...")
        Thread.sleep(1000)
        waitsForConnection(port)
    }
  }

  before {
    val port = newPGDB()
    println(s"new database at port $port")
    Thread.sleep(2000)
    conn = waitsForConnection(port)
  }

  after {
    conn.close()
    println(s"stop and rm docker container $dockerIdOpt")
    dockerIdOpt.foreach(dockerId => {
      val stopOUT = s"docker stop $dockerId".!!
      val rmOUT = s"docker rm $dockerId".!!
      println("docker stop : " + stopOUT + " / docker rm : " + rmOUT)
    })
  }
}

