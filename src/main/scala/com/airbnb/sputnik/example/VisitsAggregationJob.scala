package com.airbnb.sputnik.example

import java.time.LocalDate

import com.airbnb.sputnik.example.Schemas.VisitAggregated
import com.airbnb.sputnik.hive._
import com.airbnb.sputnik.tools.DateConverter
import com.airbnb.sputnik.{AutoModeSputnikJob, DS_FIELD, SputnikJob}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.countDistinct

object VisitsAggregationJob extends SputnikJob with AutoModeSputnikJob {

  override val startDate: LocalDate = DateConverter.stringToDate("2015-01-01")
  override val outputTables: Seq[String] = List("user_data.visits_aggregation")

  def run(): Unit = {

    val inputTable = "user_data.visits"

    val spark = sputnikSession.ss
    import spark.implicits._

    val input: DataFrame = hiveTableReader
      .getDataframe(tableName = inputTable)

    val result = input
      .groupBy("userId", DS_FIELD)
      .agg(countDistinct("url").as("distinctUrlCount"))
      .as[VisitAggregated]

    hiveTableWriter.saveDatasetAsHiveTable(
      dataset = result,
      itemClass = classOf[VisitAggregated]
    )
  }

}
