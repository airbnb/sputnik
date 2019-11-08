package com.airbnb.sputnik.example

import com.airbnb.sputnik.SputnikJob.Date
import com.airbnb.sputnik.SputnikJobUtils._
import com.airbnb.sputnik.checks.{Check, NotEmptyCheck}
import com.airbnb.sputnik.example.Schemas.{CountryStats, UserToCountry, VisitAggregated}
import com.airbnb.sputnik.hive.DateOffset._
import com.airbnb.sputnik.hive._
import com.airbnb.sputnik.{DS_FIELD, SputnikJob}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

object VisitsNonZeroCheck extends Check {

  override val checkDescription = "Checking that we have only records with more than 0 visits"

  def check(df: DataFrame): Option[ErrorMessage] = {
    val spark = df.sparkSession
    import spark.implicits._
    df
      .as[CountryStats]
      .filter(countryStats => {
        (countryStats.distinct_url_number == 0) || (countryStats == 0)
      })
      .take(1)
      .headOption match {
      case Some(badRecord) => {
        Some(s"There is at least one record " +
          s"with distinct_url_number or countryStats equals to 0: ${badRecord}")
      }
      case None => None
    }
  }

}

object VisitsPerCountryJob extends SputnikJob {

  override def run(): Unit = {

    val spark = sputnikSession.ss
    import spark.implicits._

    val visitsAggregated = hiveTableReader
      .getDataset(
        itemClass = classOf[Schemas.VisitAggregated],
        dateBoundsOffset = Some(DateBoundsOffset(lowerOffset = -7))
      ).as[Schemas.VisitAggregated]

    val daysProcessing: Dataset[Date] = daysToProcessAsDS()

    val groupedByDay =
      createSevenDaysGroups(daysProcessing, visitsAggregated)
        .alias("grouped")

    val userToCountry = hiveTableReader
      .getDataframe("user_data.users_to_country")
      .as[UserToCountry]
      .alias("country")

    val result = groupedByDay.join(userToCountry,
      col("grouped.userId").equalTo(col("country.userId")),
      "inner"
    )
      .select(
        col("grouped.userId").as("userId"),
        col(s"grouped.$DS_FIELD").as(DS_FIELD),
        col("country.country").as("country"),
        col("grouped.distinctUrlCount").as("distinct_url")
      )
      .groupBy(col("country"), col(DS_FIELD))
      .agg(
        sum(col("distinct_url")).as("distinct_url_number"),
        approx_count_distinct(col("userId")).as("user_count")
      )
      .as[CountryStats]

    val tableProperties = SimpleHiveTableProperties(
      description = "Information for number of distinct users " +
        " and urls visited for the last 7 days for a given country"
    )

    hiveTableWriter.saveAsHiveTable(
      dataFrame = result.toDF(),
      dbTableName = "user_data.country_stats",
      hiveTableProperties = tableProperties,
      checks = List(NotEmptyCheck, VisitsNonZeroCheck)
    )
  }

  def createSevenDaysGroups(daysProcessing: Dataset[Date],
                            visitsAgregated: Dataset[VisitAggregated]
                           ) = {
    val daysToProcess = daysProcessing.alias("daysToProcess")
    val visits_aggregation = visitsAgregated.alias("visits_aggregation")

    visits_aggregation
      .join(
        daysToProcess,
        datediff(
          col(s"daysToProcess.$DS_FIELD").cast("date"),
          col(s"visits_aggregation.$DS_FIELD").cast("date"))
          .between(0, 6),
        "inner")
      .select(
        col(s"daysToProcess.$DS_FIELD").as(DS_FIELD),
        col(s"visits_aggregation.userId").as("userId"),
        col(s"visits_aggregation.distinctUrlCount").as("distinctUrlCount")
      )

  }
}
