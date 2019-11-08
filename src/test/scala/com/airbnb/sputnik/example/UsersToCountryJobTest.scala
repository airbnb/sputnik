package com.airbnb.sputnik.example

import com.airbnb.sputnik.utils.{HiveTestDataWriter, SputnikJobBaseTest}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class UsersToCountryJobTest extends SputnikJobBaseTest {

  test("Test UsersToCountryJob") {
    ss.sql("create database if not exists user_data")

    import spark.implicits._
    drop("user_data.users_to_country")
    drop("user_data.users")
    drop("user_data.area_codes")
    val users = Data.users.toDF()

    HiveTestDataWriter.writeInputDataForTesting(
      dataset = users,
      dbTableName = "user_data.users"
    )

    hiveTableFromJson(resourcePath = "/area_codes.json",
      tableName = "user_data.area_codes",
      partitionSpec = None
    )

    runJob(UsersToCountryJob)

    val result = ss.table("user_data.users_to_country")
    val resultExpected = Data.usersToCountries.toDF

    assertDataFrameAlmostEquals(result, resultExpected)
  }

  test("Test check fails on UsersToCountryJob") {
    ss.sql("create database if not exists user_data")

    import spark.implicits._
    drop("user_data.users_to_country")
    drop("user_data.users")
    drop("user_data.area_codes")
    val users = Data.users.map(user => user.copy(userId = "")).toDF()

    HiveTestDataWriter.writeInputDataForTesting(
      dataset = users,
      dbTableName = "user_data.users"
    )

    hiveTableFromJson(resourcePath = "/area_codes.json",
      tableName = "user_data.area_codes",
      partitionSpec = None
    )

    assertThrows[RuntimeException](runJob(UsersToCountryJob))
  }
}
