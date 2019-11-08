package com.airbnb.sputnik.example

import com.airbnb.sputnik.example.Schemas._
import org.apache.spark.sql.{DataFrame, SparkSession}


object Data {

  val visits = List(
    Visit("1", "airbnb/plus", "2018-12-01"),
    Visit("1", "airbnb/experience", "2018-12-01"),
    Visit("1", "airbnb/lux", "2018-12-01"),
    Visit("2", "airbnb/experience", "2018-12-01"),
    Visit("2", "airbnb/experience", "2018-12-01"),
    Visit("3", "airbnb/plus", "2018-12-01"),
    Visit("1", "airbnb/plus", "2018-12-02"),
    Visit("1", "airbnb/samara", "2018-12-02"),
    Visit("4", "airbnb/lux", "2018-12-02"),
    Visit("1", "airbnb/lux", "2018-12-03"),
    Visit("5", "airbnb/lux", "2018-12-03"),
    Visit("6", "airbnb/lux", "2018-12-03")
  )

  val visits_aggregated = List(
    VisitAggregated("1", 1, "2015-12-01"),
    VisitAggregated("1", 3, "2018-12-01"),
    VisitAggregated("2", 1, "2018-12-01"),
    VisitAggregated("3", 1, "2018-12-01"),
    VisitAggregated("1", 2, "2018-12-02"),
    VisitAggregated("4", 1, "2018-12-02"),
    VisitAggregated("1", 1, "2018-12-03"),
    VisitAggregated("6", 1, "2018-12-03"),
    VisitAggregated("5", 1, "2018-12-03")
  )

  val newUsers = List(
    NewUser("1", "2018-12-01"),
    NewUser("2", "2018-12-01"),
    NewUser("3", "2018-12-01"),
    NewUser("4", "2018-12-02"),
    NewUser("5", "2018-12-03"),
    NewUser("6", "2018-12-03")
  )

  val newUsersCountDesktop = List(
    NewUserCount(3, "desktop", "2018-12-01"),
    NewUserCount(1, "desktop", "2018-12-02"),
    NewUserCount(2, "desktop", "2018-12-03")
  )

  val newUsersCountMobile = List(
    NewUserCount(3, "mobile", "2018-12-01"),
    NewUserCount(2, "mobile", "2018-12-02"),
    NewUserCount(2, "mobile", "2018-12-03")
  )

  val mobileUsersRowDatas = List(
    MobileUsersRowData("453", "createdAccount", "2018-12-01"),
    MobileUsersRowData("453", "deletedAccount", "2018-12-01"),
    MobileUsersRowData("342345353", "createdAccount", "2018-12-01"),
    MobileUsersRowData("345342", "createdAccount", "2018-12-01"),
    MobileUsersRowData("345342", "deletedAccount", "2018-12-01"),
    MobileUsersRowData("2345", "createdAccount", "2018-12-02"),
    MobileUsersRowData("45342", "createdAccount", "2018-12-02"),
    MobileUsersRowData("34345", "createdAccount", "2018-12-03"),
    MobileUsersRowData("34345", "deletedAccount", "2018-12-03"),
    MobileUsersRowData("32543", "createdAccount", "2018-12-03")
  )

  def getUsersDataFrame(sparkSession: SparkSession): DataFrame = {
    val spark = sparkSession
    import spark.implicits._
    users.toDF()
  }

  def getUsersToCountries(sparkSession: SparkSession): DataFrame = {
    val spark = sparkSession
    import spark.implicits._
    usersToCountries.toDF()
  }

  val users = List(
    User("1", "94502"),
    User("2", "67457"),
    User("3", "54765"),
    User("4", "57687"),
    User("5", "34567"),
    User("6", "34567")
  )

  val usersToCountries = List(
    UserToCountry("1", "Russia"),
    UserToCountry("2", "Russia"),
    UserToCountry("3", "China"),
    UserToCountry("4", "USA"),
    UserToCountry("5", "USA"),
    UserToCountry("6", "USA")
  )

  val countryStats = List(
    CountryStats(1, 1, "Russia", "2018-12-09"),
    CountryStats(2, 2, "USA", "2018-12-09")
  )

}
