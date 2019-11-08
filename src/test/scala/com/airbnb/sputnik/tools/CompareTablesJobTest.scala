package com.airbnb.sputnik.tools

import com.airbnb.sputnik.RunConfiguration.{JobRunConfig, Range}
import com.airbnb.sputnik.example.Data.visits
import com.airbnb.sputnik.hive.HivePartitionSpec
import com.airbnb.sputnik.tools.DateConverter.stringToDate
import com.airbnb.sputnik.utils.{HiveTestDataWriter, SputnikJobBaseTest}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CompareTablesJobTest extends SputnikJobBaseTest {
  test("Test CompareTablesJob") {

    import spark.implicits._

    ss.sql("create database if not exists user_data")
    drop("user_data.visits")
    drop("user_data.visits_2")
    val df = visits.toDF

    HiveTestDataWriter.writeInputDataForTesting(
      dataset = df,
      dbTableName = "user_data.visits",
      partitionSpec = HivePartitionSpec.DS_PARTITIONING
    )

    HiveTestDataWriter.writeInputDataForTesting(
      dataset = df,
      dbTableName = "user_data.visits_2",
      partitionSpec = HivePartitionSpec.DS_PARTITIONING
    )

    val runConfig = JobRunConfig(
      ds = Right(Range(stringToDate("2018-12-01"),
        stringToDate("2018-12-03"))),
      jobArguments = Some("--table1 user_data.visits --table2 user_data.visits_2")
    )

    runJob(CompareTablesJob, runConfig)

    drop("user_data.visits_2")
    val newDf = visits.map(_.copy(url = "")).toDF
    HiveTestDataWriter.writeInputDataForTesting(
      dataset = newDf,
      dbTableName = "user_data.visits_2",
      partitionSpec = HivePartitionSpec.DS_PARTITIONING
    )

    assertThrows[RuntimeException] {
      runJob(CompareTablesJob, runConfig)
    }
  }
}
