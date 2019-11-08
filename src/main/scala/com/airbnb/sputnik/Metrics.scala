package com.airbnb.sputnik

import java.util.concurrent.TimeUnit

import com.airbnb.sputnik.Metrics.Tag
import com.airbnb.sputnik.RunConfiguration.{JobRunConfig, SputnikRunnerConfig}
import com.codahale.metrics.MetricRegistry
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object Metrics {

  case class Tag(key: String, value: String) {
    override def toString: String = s"$key:$value"
  }

  def tagTableName(tableName: String) = {
    Seq(Tag(key = "table_name", value = tableName))
  }

}

class Metrics extends Logging {

  val metrics = new MetricRegistry

  var sputnikRunnerConfig: Option[SputnikRunnerConfig] = None
  var jobRunConfig: Option[JobRunConfig] = None
  var jobName: Option[String] = None

  def withSputnikRunnerConfig(newJobRunnerConfig: SputnikRunnerConfig) = {
    sputnikRunnerConfig = Some(newJobRunnerConfig)
    this
  }

  def withJobRunConfig(newRunConfig: JobRunConfig) = {
    jobRunConfig = Some(newRunConfig)
    this
  }

  def withJobName(newJobName: String) = {
    jobName = Some(newJobName)
    this
  }

  def getResultTags(tags: Seq[Tag]): Seq[Tag] = {
    val tagsBuffer = new ListBuffer[Tag]
    tagsBuffer ++= tags
    val userName = System.getProperty("user.name")
    tagsBuffer += Tag(key = "user_name", value = userName)

    sputnikRunnerConfig match {
      case Some(config) => {
        tagsBuffer += Tag(key = "stepSize", value = config.stepSize.isDefined.toString)
        tagsBuffer += Tag(key = "autoMode", value = config.autoMode.toString)
        tagsBuffer += Tag(key = "compareResults", value = config.compareResults.toString)
      }
      case None =>
    }

    jobRunConfig match {
      case Some(config) => {
        tagsBuffer += Tag(key = "ds", value = {
          config.ds match {
            case Left(_) => "oneDay"
            case Right(_) => "range"
          }
        })

        tagsBuffer += Tag(key = "readEnv", value = config.readEnvironment.toString)
        tagsBuffer += Tag(key = "sample", value = config.sample.isDefined.toString)
        tagsBuffer += Tag(key = "writeEnv", value = config.writeConfig.writeEnvironment.toString)
        tagsBuffer += Tag(key = "repartition", value = config.writeConfig.repartition.toString)
        tagsBuffer += Tag(key = "addUserNameToTable", value = config.writeConfig.addUserNameToTable.toString)
        tagsBuffer += Tag(key = "dropResultTables", value = config.writeConfig.dropResultTables.toString)
      }
      case None =>
    }

    jobName match {
      case Some(name) => {
        tagsBuffer += Tag(key = "sputnik_job_name", value = name)
      }
      case None =>
    }
    tagsBuffer
  }

  def measureTime[T](
                      metricName: String,
                      timedAction: () => T,
                      timeUnit: TimeUnit = TimeUnit.MINUTES,
                      tags: Seq[Tag] = Seq.empty
                    ): T = {
    val result_tags = getResultTags(tags)
    val start = System.currentTimeMillis()
    try {
      timedAction()
    } finally {
      val millis = System.currentTimeMillis() - start
      val duration = timeUnit.convert(millis, MILLISECONDS)
      logger.info(s"Metric = $metricName, millis = $millis," +
        s" duration = $duration ${timeUnit.name()}," +
        s" tags = ${result_tags.map(_.toString).mkString(",")}")
      reportTime(metricName, result_tags, millis, duration)
    }
  }

  def reportTime(metricName: String, tags: Seq[Tag], millis: Long, duration: Long) = {
    metrics.counter(metricName).inc(millis)
  }

  def count(metricName: String,
            count: Long,
            tags: Seq[Tag] = Seq.empty): Unit = {
    val result_tags = getResultTags(tags)
    logger.info(s"Metric = $metricName, count = $count," +
      s" tags = ${result_tags.map(_.toString).mkString(",")}")
    reportCount(metricName, result_tags, count)
  }

  def reportCount(metricName: String, tags: Seq[Tag], value: Long) = {
    metrics.counter(metricName).inc(value)
  }

  def jobStatus(result: Try[Unit]) = {
    result match {
      case Success(_) => {
        logger.info("Job finished successfully")
      }
      case Failure(exception) => {
        logger.error("Job failed", exception)
      }
    }
  }

  def close(ss: SparkSession) = {

  }

}
