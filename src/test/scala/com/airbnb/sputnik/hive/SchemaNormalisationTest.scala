package com.airbnb.sputnik.hive

import com.airbnb.sputnik.hive.SchemaNormalisationTest.{example_table_1_missing_field, example_table_2_missing_field, example_table_3_missing_field, reorder_fields}
import com.airbnb.sputnik.utils.SparkBaseTest
import org.apache.spark.sql.DataFrame
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

object SchemaNormalisationTest {

  case class example_table_1_missing_field(
                                            field1: Int,
                                            field2: String
                                          )

  case class example_table_2_missing_field(
                                            field1: Int,
                                            field2: String,
                                            field3: String
                                          )

  case class reorder_fields(
                             field1: Int,
                             field3: String,
                             field2: String
                           )

  case class example_table_3_missing_field(
                                            field1: Int,
                                            field2: String,
                                            field3: String
                                          )

}

@RunWith(classOf[JUnitRunner])
class SchemaNormalisationTest extends SparkBaseTest {


  test("Assert, that we throw exception when result table has field, which dataframe doesn't") {
    drop("default.example_table_1")
    ss.sql(
      """
        |create table default.example_table_1 (
        |field1 int,
        |field2 string,
        |field3 string
        |)
      """.stripMargin)
    val sparkSession = ss
    import sparkSession.implicits._
    val df = List(example_table_1_missing_field(1, "")).toDF()
    assertThrows[RuntimeException] {
      SchemaNormalisation.normaliseSchema(df, "example_table_1")
    }
    assertThrows[RuntimeException] {
      SchemaNormalisation.normaliseSchema(df, "example_table_1", subsetIsOk = true)
    }
  }

  test("Assert, that we throw exception when dataframe has field, which table doesn't") {
    drop("default.example_table_2")
    ss.sql(
      """
        |create table default.example_table_2 (
        |field1 int,
        |field2 string
        |)
      """.stripMargin)
    val sparkSession = ss
    import sparkSession.implicits._
    val df: DataFrame = List(example_table_2_missing_field(1, "", "")).toDF()
    assert(df.columns === Array("field1", "field2", "field3"))
    val normalised = SchemaNormalisation.normaliseSchema(df, classOf[reorder_fields])
    assert(normalised.columns === Array("field1", "field3", "field2"))
  }

  test("Assert, that we rearrange fields according to case class arrangement") {
    val sparkSession = ss
    import sparkSession.implicits._
    val df = List(example_table_2_missing_field(1, "", "")).toDF()
    assertThrows[RuntimeException] {
      SchemaNormalisation.normaliseSchema(df, "example_table_2")
    }
    SchemaNormalisation.normaliseSchema(df, "example_table_2", subsetIsOk = true)
  }

  test("Assert, that we reorder fields") {
    drop("default.example_table_3")
    ss.sql(
      """
        |create table default.example_table_3 (
        |field3 string,
        |field2 string,
        |field1 int
        |)
      """.stripMargin)
    val sparkSession = ss
    import sparkSession.implicits._
    val df = List(example_table_3_missing_field(1, "field2_value", "field3_value")).toDF()

    val row = SchemaNormalisation
      .normaliseSchema(df, "example_table_3")
      .collect()
      .head

    assert(row.getAs[String](0) == "field3_value")
    assert(row.getAs[Int](2) == 1)
    assert(row.getAs[String](1) == "field2_value")
  }

}
