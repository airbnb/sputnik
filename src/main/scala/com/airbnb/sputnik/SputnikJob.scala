package com.airbnb.sputnik

import com.airbnb.sputnik.RunConfiguration.JobRunConfig
import com.airbnb.sputnik.SputnikJob.SputnikSession
import com.airbnb.sputnik.hive.{HiveTableReader, HiveTableWriter, Insert}
import com.airbnb.sputnik.tools.{DefaultEnvironmentTableResolver, EnvironmentTableResolver}
import org.apache.spark.sql.SparkSession

object SputnikJob {

  case class Date(ds: String)

  case class SputnikSession(runConfig: JobRunConfig, ss: SparkSession, metrics: Metrics)

}

/**
  * The job in Sputnik is Object, which implement this SparkJob trait.
  * Developer needs to define {@link com.airbnb.sputnik.SparkJob#run} and
  * operate with data with {@link com.airbnb.sputnik.hive.HiveTableReader HiveTableReader} and
  * {@link com.airbnb.sputnik.hive.HiveTableWriter HiveTableWriter}.
  */
trait SputnikJob {

  @transient lazy val log = org.apache.log4j.LogManager.getLogger(this.getClass)

  /**
    * Configuration for this specific run. Contains info about
    * date for which we are running the job. Shouldn't be accessed directly by developer,
    * please use {@link com.airbnb.sputnik.SparkJob#getJobArguments getJobArguments} to get
    * access to job specific console parameters and {@link com.airbnb.sputnik.SparkJob#daysToProcessAsDS daysToProcessAsDS}
    * to get info about running dates. For reading from Hive rely on {@link com.airbnb.sputnik.hive.HiveTableReader HiveTableReader}
    * to work with runConfig.
    */
  implicit var sputnikSession: SputnikSession = _

  /**
    * Spark configs specific for this job.
    */
  lazy val sparkConfigs: Map[String, String] = Map.empty

  /**
    * When we try to run the job with big range of dates, let's say 2 years,
    * the job might fail because of heavy volume of data. Solution might be
    * just to break down the date range into smaller ranges and run them sequentially.
    * To avoid performing this operation manually by pipeline developer Sputnik allow
    * developer specify step size in the job and {@link com.airbnb.sputnik.runner.SparkJobRunner SparkJobRunner}
    * would run only this number of days in one batch. Can be overwritten from console
    * with "--stepSize" flag.
    */
  lazy val defaultStepSize: Option[Int] = None

  /**
    * Main method of SparkJob. All logic for operating with the data
    * should go to the implementation of this method. Please use
    * {@link com.airbnb.sputnik.hive.HiveTableReader HiveTableReader} and
    * {@link com.airbnb.sputnik.hive.HiveTableWriter HiveTableWriter} to read
    * and write from the Hive.
    */
  def run(): Unit

  def setup(sputnikSession: SputnikSession
           ) = {
    this.sputnikSession = sputnikSession
    this.hiveTableReader = createReader(sputnikSession)
    this.hiveTableWriter = createWriter(sputnikSession)

  }

  def createReader(sputnikSession: SputnikSession): HiveTableReader = {
    new HiveTableReader(sputnikSession,
      environmentTableResolver = environmentTableResolver)
  }

  def createWriter(sputnikSession: SputnikSession): HiveTableWriter = {
    new HiveTableWriter(
      insert = new Insert(sputnikSession.metrics),
      metrics = sputnikSession.metrics,
      runConfig = sputnikSession.runConfig,
      environmentTableResolver = environmentTableResolver)
  }

  val environmentTableResolver: EnvironmentTableResolver = DefaultEnvironmentTableResolver

  var hiveTableWriter: HiveTableWriter = _

  var hiveTableReader: HiveTableReader = _

  /**
    * Allows pipeline developer to define job name in the YARN and metrics reporting
    */
  val appName = {
    this.getClass.getCanonicalName
  }

}
