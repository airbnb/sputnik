package com.airbnb.sputnik.example

import com.airbnb.sputnik.SputnikJob
import com.airbnb.sputnik.checks.{NotEmptyCheck, SQLCheck}
import com.airbnb.sputnik.hive._

object NullCheck extends SQLCheck {

  def sql(temporaryTableName: String): String = {
    s"""
       | SELECT
       | IF(country is null OR country = "", false, true) as countryExist,
       | IF(userId is null OR userId = "", false, true) as userExists
       | from $temporaryTableName
     """.stripMargin
  }

}

object UsersToCountryJob extends SputnikJob {

  def run(): Unit = {
    val userTable = "user_data.users"
    val areaCodesTable = "user_data.area_codes"
    val outputTable = "user_data.users_to_country"
    val users = hiveTableReader.getDataframe(userTable)
    val areaCodes = hiveTableReader.getDataframe(areaCodesTable)

    val joined = users.joinWith(areaCodes,
      users.col("areaCode")
        .equalTo(areaCodes.col("areaCode")), "inner")

    val result = joined.select(
      joined.col("_1.userId").as("userId"),
      joined.col("_2.country").as("country")
    )

    val tableProperties = SimpleHiveTableProperties(
      description = "Temporary table for user to country mapping",
      partitionSpec = None
    )

    hiveTableWriter.saveAsHiveTable(
      dataFrame = result,
      dbTableName = outputTable,
      hiveTableProperties = tableProperties,
      checks = List(NullCheck, NotEmptyCheck)
    )
  }
}
