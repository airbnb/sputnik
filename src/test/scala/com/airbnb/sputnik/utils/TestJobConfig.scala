package com.airbnb.sputnik.utils

import java.time.{LocalDate, Month}

import com.airbnb.sputnik.Metrics
import com.airbnb.sputnik.RunConfiguration.{Environment, JobRunConfig, SputnikRunnerConfig, WriteConfig}
import com.airbnb.sputnik.SputnikJob.SputnikSession
import org.apache.spark.sql.SparkSession

object TestRunConfig {

  def sputnikSession(ss: SparkSession) = {
    SputnikSession(
      runConfig = testJobRunConfig,
      ss = ss,
      new Metrics
    )
  }

  val testSputnikRunnerConfig = SputnikRunnerConfig()

  val testJobRunConfig = JobRunConfig(
    ds = Left(LocalDate.of(2017, Month.AUGUST, 2)),
    writeConfig = WriteConfig(writeEnvironment = Environment.PROD,
      addUserNameToTable = false,
      dropResultTables = None,
      repartition = false)
  )

}
