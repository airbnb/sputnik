package com.airbnb.sputnik.example

import java.time.LocalDate

import com.airbnb.sputnik.example.Schemas.{NewUser, VisitAggregated}
import com.airbnb.sputnik.hive.FixedLowerBound
import com.airbnb.sputnik.{DS_FIELD, HoconConfigSputnikJob, SputnikJob}
import org.apache.spark.sql.functions._

object NewUsersJob extends SputnikJob with HoconConfigSputnikJob {

  override val configPath = Some("example/new_users_job.conf")

  val outputTable = "user_data.new_users"
  val inputTable = "user_data.visits_aggregation"

  def run(): Unit = {

    val spark = sputnikSession.ss
    import spark.implicits._

    val result = if (sputnikSession.ss.catalog.tableExists(outputTable)) {

      val alreadySeen =
        sputnikSession.ss
          .table(outputTable)
          .as[NewUser]
          .alias("already_seen")

      hiveTableReader.getDataframe(
        inputTable
      ).as[VisitAggregated]
        .groupBy("userId")
        .agg(
          min(col(DS_FIELD)).as(DS_FIELD)
        )
        .as[NewUser]
        .alias("processing")
        .join(
          alreadySeen,
          col("processing.userId").equalTo(col("already_seen.userId")),
          "left"
        )
        .where(col("already_seen.userId").isNull)
        .select(
          col("processing.userId").as("userId"),
          col(s"processing.$DS_FIELD").as(DS_FIELD)
        )
        .as[NewUser]

    } else {
      val input = hiveTableReader.getDataframe(
        tableName = inputTable,
        dateBoundsOffset = Some(FixedLowerBound(LocalDate.of(2016,1,1)))
      ).as[VisitAggregated]
      input
        .groupBy("userId")
        .agg(
          min(col(DS_FIELD)).as(DS_FIELD)
        )
        .as[NewUser]
    }

    val tableProperties = getHiveTablePropertiesFromConfig("user_data_new_users")

    hiveTableWriter.saveAsHiveTable(
      dataFrame = result.toDF(),
      dbTableName = outputTable,
      hiveTableProperties = tableProperties
    )
  }
}
