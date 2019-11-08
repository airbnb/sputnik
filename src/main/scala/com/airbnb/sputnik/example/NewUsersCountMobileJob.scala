package com.airbnb.sputnik.example

import com.airbnb.sputnik.checks.SQLCheck
import com.airbnb.sputnik.example.Schemas.{MobileUsersRowData, NewUserCount}
import com.airbnb.sputnik.hive.HivePartitionSpec
import com.airbnb.sputnik.{DS_FIELD, SQLSputnikJob, SputnikJob}

object CountGreaterZero extends SQLCheck {

  def sql(temporaryTableName: String): String = sqlFromResourceFile("example/count_greater_zero_check.sql", temporaryTableName)

}

object NewUsersCountMobileJob extends SputnikJob with SQLSputnikJob {

  def run(): Unit = {
    hiveTableReader
      .getDataset(classOf[MobileUsersRowData])
      .createOrReplaceTempView("mobile_row_data")

    val spark = sputnikSession.ss
    import spark.implicits._

    val newUsersMobileCounts = executeSQLQuery(
      """
        | select count(distinct(userId)) as user_count, 'mobile' as platform,  ds
        | from mobile_row_data
        | where event="createdAccount"
        | group by ds
      """.stripMargin)
      .as[NewUserCount]

    hiveTableWriter.saveDatasetAsHiveTable(
      newUsersMobileCounts,
      classOf[NewUserCount],
      checks = Seq(CountGreaterZero)
    )
  }

}
