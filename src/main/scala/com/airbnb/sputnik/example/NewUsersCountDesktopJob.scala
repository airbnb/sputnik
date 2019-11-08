package com.airbnb.sputnik.example

import com.airbnb.sputnik.example.Schemas.{NewUser, NewUserCount}
import com.airbnb.sputnik.hive.HivePartitionSpec
import com.airbnb.sputnik.{DS_FIELD, HoconConfigSputnikJob, SQLSputnikJob, SputnikJob}

object NewUsersCountDesktopJob extends SputnikJob
  with HoconConfigSputnikJob
  with SQLSputnikJob {

  override val configPath = Some("example/new_users_count_job.conf")

  def run(): Unit = {
    hiveTableReader
      .getDataset(classOf[NewUser], Some(NewUsersJob.outputTable))
      .createOrReplaceTempView("new_users")

    val spark = sputnikSession.ss
    import spark.implicits._

    val newUsersCounts = executeResourceSQLFile("example/new_users_count_desktop.sql")
      .as[NewUserCount]

    hiveTableWriter.saveDatasetAsHiveTable(
      newUsersCounts,
      classOf[NewUserCount]
    )

  }

}
