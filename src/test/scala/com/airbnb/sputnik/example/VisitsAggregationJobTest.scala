package com.airbnb.sputnik.example

import com.airbnb.sputnik.RunConfiguration.{JobRunConfig, Range}
import com.airbnb.sputnik.example.Data._
import com.airbnb.sputnik.example.Schemas.{Visit, VisitAggregated}
import com.airbnb.sputnik.tools.DateConverter.stringToDate
import com.airbnb.sputnik.utils.SputnikJobBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class VisitsAggregationJobTest extends SputnikJobBaseTest {

  test("Test VisitsAggregationJob") {
    ss.sql("create database if not exists user_data")

    import spark.implicits._

    drop("user_data.visits_aggregation")

    hiveTableFromCSV(resourcePath = "/visits.csv",
      tableName = "user_data.visits"
    )

    val runConfig = JobRunConfig(
      ds = Right(Range(stringToDate("2015-12-01"),
        stringToDate("2018-12-03")))
    )

    runJob(VisitsAggregationJob, runConfig)

    val result = hiveTableReader(runConfig)
      .getDataset(classOf[VisitAggregated])

    val resultExpected = visits_aggregated.toDF

    assertDataFrameAlmostEquals(resultExpected, result)

  }

}
