package com.airbnb.sputnik

import java.time.LocalDate

import com.airbnb.sputnik.SputnikJob.{Date, SputnikSession}
import com.airbnb.sputnik.tools.DateConverter
import org.apache.spark.sql.Dataset

object SputnikJobUtils {


  /**
    * Gives access to job arguments, which were passed from console with
    * "--jobArguments" flag. In theory average user wouldn't need it, because
    * everything run specific should be handled by Sputnik.
    *
    * @return arguments passed to a job from console
    */
  def getJobArguments()(implicit sputnikSession: SputnikSession): String = {
    sputnikSession.runConfig.jobArguments match {
      case Some(arg) => arg
      case None => throw new RuntimeException("Not job arguments were passed from console")
    }
  }

  /**
    * Allows user to get access to the dates for which this job currently run.
    * Please use with caution - you shouldn't use it to directly read from hive,
    * you should use {@link com.airbnb.sputnik.hive.HiveTableReader HiveTableReader}.
    * Example of valid use is: {@link com.airbnb.sputnik.example.VisitsPerCountry VisitsPerCountry}
    *
    * @return dataset with dates we are processing in current run
    */
  def daysToProcessAsDS()(implicit sputnikSession: SputnikSession): Dataset[Date] = {
    val sparkSession = sputnikSession.ss
    import sparkSession.implicits._
    daysToProcess()
      .map(DateConverter.dateToString(_))
      .map(Date(_))
      .toDS
  }

  /**
    * See {@link com.airbnb.sputnik.SparkJob#daysToProcessAsDS daysToProcessAsDS}
    */
  def daysToProcess()(implicit sputnikSession: SputnikSession): List[LocalDate] = {
    RunConfiguration.daysToProcess(sputnikSession.runConfig.ds)
  }

}
