package com.airbnb.sputnik.example

import com.airbnb.sputnik.RunConfiguration
import com.airbnb.sputnik.RunConfiguration.JobRunConfig
import com.airbnb.sputnik.hive.HivePartitionSpec
import com.airbnb.sputnik.tools.DateConverter
import com.airbnb.sputnik.utils.{HiveTestDataWriter, SputnikJobBaseTest}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NewUsersCountMobileJobTest extends SputnikJobBaseTest {

  test("Test NewUsersCountMobileJob") {
    ss.sql("create database if not exists user_data")
    drop("user_data.mobile_row_data")
    drop("user_data.new_user_count")
    import spark.implicits._
    val input = Data.mobileUsersRowDatas.toDF()

    HiveTestDataWriter.writeInputDataForTesting(
      dataset = input,
      dbTableName = "user_data.mobile_row_data",
      partitionSpec = HivePartitionSpec.DS_PARTITIONING
    )
    val runConfig = JobRunConfig(ds = Right(RunConfiguration.Range(DateConverter.stringToDate("2018-12-01"), DateConverter.stringToDate("2018-12-03"))))

    runJob(NewUsersCountMobileJob, runConfig)
    val expectedOutput = Data.newUsersCountMobile.toDF()

    val result = ss.table("user_data.new_user_count")
    assertDataFrameAlmostEquals(expectedOutput, result)
  }

}
