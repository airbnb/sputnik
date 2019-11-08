package com.airbnb.sputnik.checks

import com.airbnb.sputnik.example.Data
import com.airbnb.sputnik.utils.SputnikJobBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SQLCheckTest extends SputnikJobBaseTest {

  test("Test boolean result") {
    val checkSuccess = new SQLCheck() {
      def sql(temporaryTableName: String): String = {
        """select true as someCheck"""
      }
    }
    assert(checkSuccess.check(ss.emptyDataFrame) == None)

    val checkFailure = new SQLCheck() {
      def sql(temporaryTableName: String): String = {
        """select false as someCheck"""
      }
    }
    assert(checkFailure.check(ss.emptyDataFrame).get.contains("false for "))
  }

  test("Test string result") {
    val checkSuccess = new SQLCheck() {
      def sql(temporaryTableName: String): String = {
        """select 'true' as someCheck"""
      }
    }
    assert(checkSuccess.check(ss.emptyDataFrame) == None)

    val checkFailure = new SQLCheck() {
      def sql(temporaryTableName: String): String = {
        """select 'false' as someCheck"""
      }
    }
    assert(checkFailure.check(ss.emptyDataFrame).get.contains("false for "))
  }

  test("Test integer result") {
    val checkSuccess = new SQLCheck() {
      def sql(temporaryTableName: String): String = {
        """select 1 as someCheck"""
      }
    }
    assert(checkSuccess.check(ss.emptyDataFrame) == None)

    val checkFailure = new SQLCheck() {
      def sql(temporaryTableName: String): String = {
        """select 0 as someCheck"""
      }
    }
    assert(checkFailure.check(ss.emptyDataFrame).get.contains("0 for "))
  }

  test("Test long result") {
    val checkSuccess = new SQLCheck() {
      def sql(temporaryTableName: String): String = {
        """select cast(1 as bigint) as someCheck"""
      }
    }
    assert(checkSuccess.check(ss.emptyDataFrame) == None)

    val checkFailure = new SQLCheck() {
      def sql(temporaryTableName: String): String = {
        """select cast(0 as bigint) as someCheck"""
      }
    }
    assert(checkFailure.check(ss.emptyDataFrame).get.contains("0 for "))
  }

  test("Test string not parsed") {
    val checkSuccess = new SQLCheck() {
      def sql(temporaryTableName: String): String = {
        """select 'hi' as someCheck"""
      }
    }
    assert(checkSuccess.check(ss.emptyDataFrame).get.contains("java.lang.IllegalArgumentException: For input string"))
  }

  test("Test incorrect datatype") {
    val checkSuccess = new SQLCheck() {
      def sql(temporaryTableName: String): String = {
        """select cast("2018_12_12" as date) as someCheck"""
      }
    }
    assert(checkSuccess.check(ss.emptyDataFrame).get.contains("Incorrect data type for"))
  }

  test("Test multiple fields datatype") {
    val checkSuccess = new SQLCheck() {
      def sql(temporaryTableName: String): String = {
        """select "true" as someCheck, 1 as someOtherCheck"""
      }
    }
    assert(checkSuccess.check(ss.emptyDataFrame) === None)

    val checkFailure = new SQLCheck() {
      def sql(temporaryTableName: String): String = {
        """select "true" as someCheck, 0 as someOtherCheck"""
      }
    }
    assert(checkFailure.check(ss.emptyDataFrame).get.contains("0 for "))
  }

  test("Test access to table") {
    val checkSuccess = new SQLCheck() {
      def sql(temporaryTableName: String): String = {
        s"""select count(*) from ${temporaryTableName} where country = "Russia" """
      }
    }
    import spark.implicits._
    val df = Data.usersToCountries.toDF()
    assert(checkSuccess.check(df) === None)
  }

  test("Test per record check") {
    val checkSuccess = new SQLCheck() {
      def sql(temporaryTableName: String): String = {
        s"""select true from ${temporaryTableName}"""
      }
    }

    import spark.implicits._
    val df = Data.usersToCountries.toDF()
    assert(checkSuccess.check(df) === None)

    val checkFailure = new SQLCheck() {
      def sql(temporaryTableName: String): String = {
        s"""select false from ${temporaryTableName}"""
      }
    }
    assert(checkFailure.check(df).isDefined)
  }

}
