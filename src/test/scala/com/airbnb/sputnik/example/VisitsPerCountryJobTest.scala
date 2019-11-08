package com.airbnb.sputnik.example

import java.time.{LocalDate, Month}

import com.airbnb.sputnik.RunConfiguration.JobRunConfig
import com.airbnb.sputnik.SputnikJob.Date
import com.airbnb.sputnik.example.Schemas.VisitAggregated
import com.airbnb.sputnik.hive.HivePartitionSpec
import com.airbnb.sputnik.utils.{HiveTestDataWriter, SputnikJobBaseTest}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class VisitsPerCountryJobTest extends SputnikJobBaseTest {

  test("test VisitsPerCountry") {
    ss.sql("create database if not exists user_data")

    import spark.implicits._
    drop("user_data.visits_aggregation")
    drop("user_data.users_to_country")

    val visits_aggregated = Data.visits_aggregated.toDS
    HiveTestDataWriter.writeInputDataForTesting(
      dataset = visits_aggregated,
      dbTableName = "user_data.visits_aggregation",
      itemClass = Some(classOf[VisitAggregated]),
      partitionSpec = HivePartitionSpec.DS_PARTITIONING
    )

    val users_to_country = Data.usersToCountries.toDF()
    HiveTestDataWriter.writeInputDataForTesting(
      dataset = users_to_country,
      dbTableName = "user_data.users_to_country"
    )

    val runConfig = JobRunConfig(ds = Left(LocalDate.of(2018, Month.DECEMBER, 9)))
    runJob(VisitsPerCountryJob, runConfig)
    val resultExpected = Data.countryStats.toDF()

    val result = ss
      .table("user_data.country_stats")

    assertDataFrameAlmostEquals(result, resultExpected)
  }

  test("test check fails") {
    ss.sql("create database if not exists user_data")

    import spark.implicits._
    drop("user_data.visits_aggregation")
    drop("user_data.users_to_country")

    val visits_aggregated = Data
      .visits_aggregated
      .map(visitAggregated => visitAggregated.copy(distinctUrlCount = 0))
      .toDS()
    HiveTestDataWriter.writeInputDataForTesting(
      dataset = visits_aggregated,
      dbTableName = "user_data.visits_aggregation",
      itemClass = Some(classOf[VisitAggregated]),
      partitionSpec = HivePartitionSpec.DS_PARTITIONING
    )

    val users_to_country = Data.usersToCountries.toDF()
    HiveTestDataWriter.writeInputDataForTesting(
      dataset = users_to_country,
      dbTableName = "user_data.users_to_country"
    )

    val runConfig = JobRunConfig(ds = Left(LocalDate.of(2018, Month.DECEMBER, 9)))
    val exception = intercept[RuntimeException] {
      runJob(VisitsPerCountryJob, runConfig)
    }
    assert(exception.getMessage.contains("with distinct_url_number or countryStats equals to 0"))
  }

  test("test createSevenDaysGroups") {
    val spark = ss
    import spark.implicits._

    val daysToProcess = List(
      Date("2018-12-02"),
      Date("2018-12-23"),
      Date("2018-12-24"),
      Date("2018-12-25")
    )
      .toDS()
      .alias("daysToProcess")

    val visitsAggregated = List(
      VisitAggregated("1", 1, "2018-12-03"),
      VisitAggregated("2", 1, "2018-12-02"),
      VisitAggregated("3", 1, "2018-12-01"),
      VisitAggregated("4", 1, "2018-11-30"),
      VisitAggregated("5", 1, "2018-11-20")
    ).toDS()
      .alias("visits_aggregation")
    val result = VisitsPerCountryJob
      .createSevenDaysGroups(daysToProcess, visitsAggregated)

    val expectedResult = List(
      VisitAggregated("2", 1, "2018-12-02"),
      VisitAggregated("3", 1, "2018-12-02"),
      VisitAggregated("4", 1, "2018-12-02")
    ).toDS

    assertDataFrameAlmostEquals(result.toDF(), expectedResult.toDF())
  }

}
