package com.airbnb.sputnik.example

import com.airbnb.sputnik.RunConfiguration
import com.airbnb.sputnik.RunConfiguration.JobRunConfig
import com.airbnb.sputnik.example.NewUsersCountDesktopJob.executeResourceSQLFile
import com.airbnb.sputnik.hive.HivePartitionSpec
import com.airbnb.sputnik.tools.DateConverter
import com.airbnb.sputnik.utils.{HiveTestDataWriter, SputnikJobBaseTest}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NewUsersCountDesktopJobTest extends SputnikJobBaseTest {

  test("Test NewUsersCountDesktopJob") {
    ss.sql("create database if not exists user_data")
    drop(NewUsersJob.outputTable)
    drop("user_data.new_user_count")
    import spark.implicits._
    val input = Data.newUsers.toDF()
    HiveTestDataWriter.writeInputDataForTesting(
      dataset = input,
      dbTableName = NewUsersJob.outputTable,
      partitionSpec = HivePartitionSpec.DS_PARTITIONING
    )
    val runConfig = JobRunConfig(ds = Right(RunConfiguration.Range(DateConverter.stringToDate("2018-12-01"), DateConverter.stringToDate("2018-12-03"))))
    runJob(NewUsersCountDesktopJob, runConfig)
    val expectedOutput = Data.newUsersCountDesktop.toDF()

    val result = ss.table("user_data.new_user_count")
    assertDataFrameAlmostEquals(expectedOutput, result)
  }

  test("Test NewUsersCountDesktopJob transformation") {
    import spark.implicits._
    Data.newUsers.toDF().createOrReplaceTempView("new_users")
    val result = executeResourceSQLFile("example/new_users_count_desktop.sql")
    val expectedOutput = Data.newUsersCountDesktop.toDF()
    assertDataFrameAlmostEquals(expectedOutput, result)
  }

}
