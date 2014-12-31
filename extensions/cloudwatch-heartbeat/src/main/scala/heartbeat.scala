package com.mfglabs.commons.aws
package extensions.cloudwatch

import java.io.StringReader

import scala.concurrent._
import scala.concurrent.duration._

import akka.actor.{Actor, ActorSystem, Props, Cancellable}
import akka.pattern.ask
import akka.util.Timeout

import grizzled.slf4j.Logging

import com.amazonaws.services.cloudwatch.model._
// import com.amazonaws.services.cloudwatch.model.StandardUnit._

import cloudwatch._

trait HeartBeat {
  def namespace: String
  def name: String
  def beatPeriod: FiniteDuration
  def alarmPeriod: FiniteDuration

  def start()(implicit ec: ExecutionContext): Future[Boolean]
  def stop()(implicit ec: ExecutionContext): Future[Boolean]
}

class CloudwatchAkkaHeartbeat(
  val namespace: String,
  val name: String,
  val beatPeriod: FiniteDuration,
  val alarmPeriod: FiniteDuration,
  val alarmPeriodNb: Int,
  val alarmThreshold: Int,
  val system: ActorSystem,
  val client: AmazonCloudwatchClient,
  val actionEndpoint: String,
  val launchTimeout: FiniteDuration = 5 seconds
) extends HeartBeat with Logging {

  case object HeartBeatMsg
  case object HeartBeatCancel
  case object HeartBeatLaunch

  class HeartbeatActor extends Actor {
    import context.dispatcher

    var cancellable: Cancellable = _

    def receive = {
      case HeartBeatLaunch =>
        cancellable = system.scheduler.schedule(
          0.milliseconds,
          beatPeriod,
          self,
          HeartBeatMsg)

        sender ! true

      case HeartBeatMsg =>
        // sends heartbeat metric to cloudwatch
        logger.debug("Sending metric "+(new java.util.Date()))
        putMetric()

      case HeartBeatCancel =>
        sender ! cancellable.cancel()
    }
  }

  val actor = system.actorOf(Props(classOf[HeartbeatActor], this))
  val metricName = name + "-heartbeat-metric"
  val alarmName = name + "-heartbeat-alarm"

  val alarmDesc = "Alarm triggered when InsufficientData detected on Application Heartbeat"

  implicit val timeout = Timeout(launchTimeout)

  def dim(name: String, value: String) = new Dimension().withName(name).withValue(value)

  def getMetric()(implicit ec: ExecutionContext): Future[Option[Metric]] = {
    import scala.collection.JavaConversions._

    client.listMetrics(
      new ListMetricsRequest().withNamespace(namespace)
    ) map { res =>
      res.getMetrics().collectFirst { case m if m.getMetricName() == metricName => m }
    }
  }

  def getMetrics()(implicit ec: ExecutionContext): Future[List[Metric]] = {
    import scala.collection.JavaConversions._
    client.listMetrics(new ListMetricsRequest().withNamespace("Wdmtg/Backend")) map (_.getMetrics().toList)
  }

  def putMetric()(implicit ec: ExecutionContext): Future[Unit] = {
    import scala.collection.JavaConversions._

    val data = new MetricDatum()
      .withTimestamp(new java.util.Date())
      .withMetricName(metricName)
      .withValue(1.0)
      .withDimensions(
        dim("Category", "Backend")
      )

    val metric = new PutMetricDataRequest()
      .withNamespace(namespace)
      .withMetricData(data)

    client.putMetricData(metric)
  }


  def getAlarm()(implicit ec: ExecutionContext): Future[Option[MetricAlarm]] = {
    import scala.collection.JavaConversions._

    client.describeAlarmsForMetric(
      new DescribeAlarmsForMetricRequest()
        .withNamespace(namespace)
        .withMetricName(metricName)
        // .withStatistic(Statistic.SampleCount)
        // .withUnit(StandardUnit.Count)
        // .withPeriod(alarmPeriod.toSeconds.toInt)
        // .withDimensions(dim("Category", "Backend")
    ) map { res =>
      res.getMetricAlarms().toList.headOption match {
        case Some(ma) =>
          logger.info("Alarm $namespace-$alarmName found...")
          if(
            ma.getAlarmName() == s"$namespace-$alarmName" &&
            ma.getAlarmDescription() == alarmDesc &&
            ma.getNamespace() == namespace &&
            ma.getMetricName() == metricName &&
            ma.getThreshold() == alarmThreshold &&
            ma.getEvaluationPeriods() == alarmPeriodNb &&
            ma.getPeriod() == alarmPeriod.toSeconds.toInt &&
            ma.getAlarmActions().toList == List(actionEndpoint) &&
            ma.getStatistic() == Statistic.SampleCount &&
            ma.getUnit() == StandardUnit.Count &&
            ma.getComparisonOperator() == ComparisonOperator.LessThanThreshold &&
            ma.getDimensions().toList == List(dim("Category", "Backend"))
          ) {
            logger.info("Alarm $namespace-$alarmName unchanged...")
            Some(ma)
          } else {
            logger.info("Alarm $namespace-$alarmName changed...")
            None
          }
        case None =>
          logger.info("Alarm $namespace-$alarmName not found...")
          None
      }
    }
  }


  def putAlarm()(implicit ec: ExecutionContext): Future[Unit] = {
    import scala.collection.JavaConversions._

    val alarm = new PutMetricAlarmRequest()
      .withAlarmName(s"$namespace-$alarmName")
      .withAlarmDescription(alarmDesc)
      .withNamespace(namespace)
      .withMetricName(metricName)
      .withThreshold(alarmThreshold) // 70s timeout / 1.5s => 46 count
      .withEvaluationPeriods(alarmPeriodNb) // if for x periods, there are less data than half of this count, it means there is a pb
      // TODO conversion to Int risky???
      .withPeriod(alarmPeriod.toSeconds.toInt)
      .withInsufficientDataActions(actionEndpoint)
      .withAlarmActions(actionEndpoint)
      .withOKActions(actionEndpoint)
      .withStatistic(Statistic.SampleCount)
      .withUnit(StandardUnit.Count)
      .withComparisonOperator(ComparisonOperator.LessThanThreshold)
      .withDimensions(dim("Category", "Backend"))

    client.putMetricAlarm(alarm) //map { _ =>
    //   system.scheduler.scheduleOnce(
    //     30.seconds
    //   ) {
    //     //set the alarm to OK state
    //     client.setAlarmState(
    //       new SetAlarmStateRequest()
    //         .withAlarmName(s"$namespace-$alarmName")
    //         .withStateValue(StateValue.OK)
    //         .withStateReason("Set to OK... Now, Listening heartbeats...")
    //     )
    //   }
    // }
  }

  def initMetricAlarm()(implicit ec: ExecutionContext): Future[Unit] = {
    logger.info(s"Updating Metric $namespace/$metricName & Alarm $alarmName @Cloudwatch")
    // putMetric() flatMap { _ =>
    //   logger.info(s"Metric $namespace/$metricName updated")
    //   putAlarm() map (_ => logger.info(s"Alarm $alarmName updated"))
    // }

    getMetric() flatMap {
      case None =>
        logger.info(s"Metric $namespace/$metricName not found, creating it")
        putMetric() map (_ => logger.info(s"Metric $namespace/$metricName created"))
      case Some(m) =>
        //Future.successful(logger.info(s"Metric $namespace/$metricName found"))
        logger.info(s"Metric $namespace/$metricName found, updating it")
        putMetric() map (_ => logger.info(s"Metric $namespace/$metricName updated"))
    } flatMap { _ =>
      getAlarm() flatMap {
        case None =>
          logger.info(s"Alarm $alarmName not found, creating it")
          putAlarm() map (_ => logger.info(s"Alarm $alarmName created"))
        case Some(a) =>
          logger.info(s"Alarm $alarmName found, updating it")
          putAlarm() map (_ => logger.info(s"Alarm $alarmName updated"))
          //Future.successful(())
      }
    }
  }

  override def start()(implicit ec: ExecutionContext): Future[Boolean] = {
    // check/create metric + alarm on cloudwatch
    initMetricAlarm() flatMap { _ =>
      // launch akka scheduler
      (actor ? HeartBeatLaunch).mapTo[Boolean]
    }

  }

  override def stop()(implicit ec: ExecutionContext): Future[Boolean] = {
    (actor ? HeartBeatCancel).mapTo[Boolean]
  }
}


trait CloudwatchHeartbeatLayer {
  def system: ActorSystem

  def heartbeatClient: AmazonCloudwatchClient

  def heartbeatNamespace: String
  def heartbeatName: String

  // duration between each heartbeat
  def heartbeatPeriod: FiniteDuration // = 2.seconds
  // duration of a period of analysis for alarm
  def heartbeatAlarmPeriod: FiniteDuration // = 120.seconds
  // number of alarm periods not satisfying threshold after which alarm is triggered
  def heartbeatAlarmPeriodNb: Int // = 1
  // threshold data sample count under which the alarm is triggered on an alarm period
  def heartbeatAlarmThreshold: Int // = 20
  // endpoint to send alarms to...
  def heartbeatEndpoint: String

  lazy val heartbeat = new CloudwatchAkkaHeartbeat(
    namespace = heartbeatNamespace,
    name = heartbeatName,
    beatPeriod = heartbeatPeriod,
    alarmPeriod = heartbeatAlarmPeriod,
    alarmPeriodNb = heartbeatAlarmPeriodNb,
    alarmThreshold = heartbeatAlarmThreshold,
    system = system,
    client = heartbeatClient,
    actionEndpoint = heartbeatEndpoint
  )
}