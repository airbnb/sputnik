package com.airbnb.sputnik.example

import com.airbnb.sputnik.RunConfiguration.JobRunConfig
import com.airbnb.sputnik.hive.HivePartitionSpec
import com.airbnb.sputnik.tools.DateConverter
import com.airbnb.sputnik.utils.{HiveTestDataWriter, SputnikJobBaseTest}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NewUsersJobTest extends SputnikJobBaseTest {

  test("Test NewUsersJob") {
    ss.sql("create database if not exists user_data")
    drop("user_data.new_users")
    drop("user_data.visits_aggregation")
    import spark.implicits._
    val input = Data.visits_aggregated.toDF()
    HiveTestDataWriter.writeInputDataForTesting(
      dataset = input,
      dbTableName = "user_data.visits_aggregation",
      partitionSpec = HivePartitionSpec.DS_PARTITIONING
    )
    val runConfig = JobRunConfig(ds = Left(DateConverter.stringToDate("2018-12-03")))
    runJob(NewUsersJob, runConfig)
    val expectedOutput = Data.newUsers.toDF()

    val result = ss.table("user_data.new_users")
    assertDataFrameAlmostEquals(expectedOutput, result)

  }

}
