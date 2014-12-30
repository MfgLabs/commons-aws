package com.mfglabs.commons.aws
package extensions.cloudwatch

import org.scalatest._
import org.scalatest.time._
import concurrent.ScalaFutures

import scala.concurrent._
import scala.concurrent.duration._

import akka.actor.ActorSystem
import akka.testkit.TestKit

import com.amazonaws.regions.{Regions, Region}

/**
 * To run this test, launch a local postgresql instance and put the right connection info into DriverManager.getConnection
 */

class CloudwatchHeartBeatSpec extends TestKit(ActorSystem("testSystem"))
  with FlatSpecLike with Matchers with ScalaFutures with BeforeAndAfterAll {

  import cloudwatch._
  import extensions.postgres._
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(15, Minutes), interval = Span(5, Millis))

  val client = new com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClient()
  val region = Region.getRegion(Regions.valueOf("EU_WEST_1"))
  client.setRegion(region)
  val CW = new cloudwatch.AmazonCloudwatchClient(client)

  "heartbeat" should "create heartbeat metric / alarm" in {
    val hb = new CloudwatchAkkaHeartbeat(
      namespace = "Test/Heartbeat",
      name = "test1",
      beatPeriod = 2.second,
      alarmTimeout = 120.seconds,
      system = system,
      client = CW,
      actionEndpoint = "arn:aws:sns:eu-west-1:896733075612:Cloudwatch-HeartBeat-Test"
    )

    val p = Promise[Boolean]
    system.scheduler.scheduleOnce(300.seconds,
      new Runnable { override def run = {
        hb.stop() map { b => p success b }
        ()
      }}
    )

    whenReady(hb.start() flatMap ( _ => p.future )){ b => println("result:"+b)}

    // whenReady(hb.initMetricAlarm()){ _ => println("End of init") }

    // whenReady(hb.getMetrics()){ metrics => println("metrics:"+metrics)}

  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }
}
