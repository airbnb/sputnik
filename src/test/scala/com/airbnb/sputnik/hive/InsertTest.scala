package com.airbnb.sputnik.hive

import java.lang
import java.time.LocalDate

import com.airbnb.sputnik.Metrics
import com.airbnb.sputnik.RunConfiguration.Range
import com.airbnb.sputnik.checks.{Check, NotEmptyCheck}
import com.airbnb.sputnik.hive.HivePartitionSpec.DS_PARTITIONING
import com.airbnb.sputnik.utils.SomeTestingData.OneMoreSimpleRow
import com.airbnb.sputnik.utils.{SomeTestingData, SparkBaseTest}
import org.apache.spark.sql.DataFrame
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

case class MultiColumnPartData(first_field: String, platform: String, ds: String)

@RunWith(classOf[JUnitRunner])
class InsertTest extends SparkBaseTest with SomeTestingData {

  test("Test main insert logic") {
    var simpleInsertRun = 0
    var processMultipleDaysInsertRun = 0
    var runChecksRun = 0

    val sparkSession = ss
    import sparkSession.implicits._

    def testingInsert(simplifiedInsertLogic: Boolean) = {
      new Insert(metrics = new Metrics, simplifiedInsertLogic) {

        override def simpleInsert(tableName: String,
                                  partitionSpec: Option[HivePartitionSpec],
                                  dataFrame: DataFrame,
                                  checks: Seq[Check]): Unit = {
          simpleInsertRun += 1
          val cases = Set("case1", "case2", "case3")
          assert(cases.contains(tableName))
          if (tableName.equals("case3")) {
            assert(dataFrame.as[Data].collect() === Array(Data("value1", "2018-01-01")))
          }
        }

        override def processMultipleDaysInsert(dataFrame: DataFrame,
                                               tableName: String,
                                               checks: Seq[Check],
                                               resultDates: Array[String]): Unit = {
          processMultipleDaysInsertRun += 1
          val cases = Set("case4")
          assert(cases.contains(tableName))
          if (tableName.equals("case4")) {
            assert(resultDates === Array("2018-01-01", "2018-01-02"))
          }
        }

        override def runChecks(checks: Seq[Check], dataFrame: DataFrame, tableName: String): Unit = {
          runChecksRun += 1
        }
      }
    }

    var insert = testingInsert(true)

    insert.insert(ss.emptyDataFrame,
      tableName = "case1",
      partitionSpec = None
    )

    insert.insert(ss.emptyDataFrame,
      tableName = "case2",
      partitionSpec = DS_PARTITIONING
    )

    assert(simpleInsertRun === 2)


    var data = List(
      Data("value1", "2018-01-01")
    ).toDF()

    insert = testingInsert(false)

    insert.insert(data,
      tableName = "case3",
      partitionSpec = DS_PARTITIONING,
      ds = Left(LocalDate.of(2018, 1, 1))
    )

    assert(simpleInsertRun === 3)

    data = List(
      Data("value1", "2018-01-01"),
      Data("value1", "2018-01-02")
    ).toDF()

    ss.sql(
      s"""
         |  create table case4 (
         | first_field string
         |) PARTITIONED BY (ds String)
      """.stripMargin)
    sparkSession.sql(s"ALTER TABLE case4 ADD PARTITION (ds='2018-01-04')")
    insert.insert(data,
      tableName = "case4",
      partitionSpec = DS_PARTITIONING,
      ds = Right(Range(LocalDate.of(2018, 1, 1), LocalDate.of(2018, 1, 5)))
    )

    assert(simpleInsertRun === 3)
    assert(processMultipleDaysInsertRun === 1)
    assert(HiveUtils.getExistingPartitions(ss, "case4") === Array("2018-01-03", "2018-01-04", "2018-01-05"))

    ss.sql(
      s"""
         |  create table case5 (
         | first_field string
         |) PARTITIONED BY (ds String)
      """.stripMargin)

    assert(HiveUtils.getExistingPartitions(ss, "case5") === Array())
    insert.insert(ss.emptyDataset[Data].toDF(),
      tableName = "case5",
      partitionSpec = DS_PARTITIONING,
      ds = Left(LocalDate.of(2018, 1, 1))
    )

    assert(simpleInsertRun === 3)
    assert(processMultipleDaysInsertRun === 1)
    assert(runChecksRun === 1)
    assert(HiveUtils.getExistingPartitions(ss, "case5") === Array("2018-01-01"))

  }

  test("Test processMultipleDaysInsert") {
    val table = s"default.MultipleDaysInsert_test"
    drop(table)
    ss.sql(
      s"""
         |  create table $table(
         | first_field string
         |) PARTITIONED BY (ds String)
      """.stripMargin)


    val sparkSession = ss
    import sparkSession.implicits._
    val data = List(
      Data("value1", "2018-01-01"),
      Data("value2", "2018-01-02"),
      Data("value3", "2018-01-03")
    ).toDF()

    val check = new Check() {
      override def check(df: DataFrame): Option[ErrorMessage] = {
        if (df.head().getAs[String]("first_field").equals("value2")) {
          Some("Bad record")
        } else {
          None
        }
      }
    }
    assert(HiveUtils.getExistingPartitions(ss, table) === Array())
    val caught = intercept[RuntimeException] {
      DefaultInsert.processMultipleDaysInsert(data, table, List(check), Array("2018-01-01", "2018-01-02", "2018-01-03"))
    }
    assert(caught.getMessage === "Failed to insert to 2018-01-02")
    assert(HiveUtils.getExistingPartitions(ss, table) === Array("2018-01-01", "2018-01-03"))
    assert(sparkSession.table(table).as[Data].collect().sortBy(_.ds) === Array(Data("value1", "2018-01-01"), Data("value3", "2018-01-03")).sortBy(_.ds))
  }

  test("Test insert on non partitioned table") {
    val table = s"default.insert_test"
    drop(table)
    ss.sql(
      s"""
         |  create table $table(
         | field1 string ,
         | field2 string
         |)
      """.stripMargin)

    assert(ss.table(table).count() === 0)

    DefaultInsert.insert(
      df = oneMoreSimpleRowDF,
      tableName = table
    )
    assertDataFrameEquals(oneMoreSimpleRowDF, ss.table(table))

    DefaultInsert.insert(
      df = oneMoreSimpleRowDF,
      tableName = table
    )

    assertDataFrameEquals(oneMoreSimpleRowDF.union(oneMoreSimpleRowDF), ss.table(table))
  }

  test("Test checks") {
    val table = s"default.insert_test_check"
    drop(table)
    ss.sql(
      s"""
         |  create table $table(
         | field1 string ,
         | field2 string
         |)
      """.stripMargin)

    assert(ss.table(table).count() === 0)

    DefaultInsert.insert(
      df = oneMoreSimpleRowDF,
      tableName = table,
      checks = List(NotEmptyCheck)
    )
    assertDataFrameEquals(oneMoreSimpleRowDF, ss.table(table))

    val sparkSession = ss
    import sparkSession.implicits._

    val caught = intercept[RuntimeException] {
      DefaultInsert.insert(
        df = ss.emptyDataset[OneMoreSimpleRow].toDF(),
        tableName = table,
        checks = List(NotEmptyCheck)
      )
    }
    assert(caught.getMessage === NotEmptyCheck.errorMessage)
  }

  test("storing result data on failed check") {
    ss.sql("create database if not exists tmp")
    val sparkSession = ss
    import sparkSession.implicits._


    val data = List(
      Data("value1", "2018-01-01")
    ).toDF()

    var check = new Check {
      override def check(df: DataFrame): Option[lang.String] = {None}
    }

    DefaultInsert.runChecks(Seq(check), data, "")

    check = new Check {
      override def check(df: DataFrame): Option[lang.String] = {Some("Bad data")}
    }

    assertThrows[RuntimeException]{
      DefaultInsert.runChecks(Seq(check), data, "target_table")
    }

    val tableName = ss
      .sql("show tables in tmp")
      .map(x => x.getAs[String](0) + "." + x.getAs[String](1))
      .collect()
      .find(_.startsWith("tmp.sputnik_failed_check_persisted_result_"))
      .get

    assertDataFrameAlmostEquals(ss.read.table(tableName), data)

  }

  test("Test insert on partitioned table") {
    val table = s"default.insert_test_dynamic_partioning"
    drop(table)
    ss.sql(
      s"""
         |  create table $table(
         | field1 string
         |) PARTITIONED BY (field2 string)
      """.stripMargin)

    assert(ss.table(table).count() === 0)

    DefaultInsert.insert(
      df = oneMoreSimpleRowDF,
      tableName = table,
      partitionSpec = Some(HivePartitionSpec(List("field2")))
    )

    assertDataFrameAlmostEquals(oneMoreSimpleRowDF, ss.table(table))

    DefaultInsert.insert(
      df = oneMoreSimpleRowDF,
      tableName = table,
      partitionSpec = Some(HivePartitionSpec(List("field2")))
    )

    assertDataFrameAlmostEquals(oneMoreSimpleRowDF, ss.table(table))

    val sparkSession = ss
    import sparkSession.implicits._
    val moreData = List(
      OneMoreSimpleRow("field1_some_value_3", "field2_some_value_3"),
      OneMoreSimpleRow("field1_some_value_4", "field2_some_value_4")
    ).toDF()

    DefaultInsert.simpleInsert(
      dataFrame = moreData,
      tableName = table,
      partitionSpec = Some(HivePartitionSpec(List("field2")))
    )

    assertDataFrameAlmostEquals(oneMoreSimpleRowDF.union(moreData), ss.table(table))

    val rewriteData = List(
      OneMoreSimpleRow("sfsdfsf", "field2_some_value_1"),
      OneMoreSimpleRow("asdgsecrtgesr", "field2_some_value_2"),
      OneMoreSimpleRow("vsdhhsg", "field2_some_value_3"),
      OneMoreSimpleRow("sdvhdrht", "field2_some_value_4")
    ).toDF()

    DefaultInsert.insert(
      df = rewriteData,
      tableName = table,
      partitionSpec = Some(HivePartitionSpec(List("field2")))
    )

    assertDataFrameAlmostEquals(rewriteData, ss.table(table))
  }


  test("Test insert on table partitioned with many columns") {
    val table = s"default.insert_test_multiple_fields_partitioning"
    drop(table)
    ss.sql(
      s"""
         |  create table $table(
         | field1 string
         |) PARTITIONED BY (partitionedfield1 string, partitionedfield2 string)
      """.stripMargin)
    assert(ss.table(table).count() === 0)

    DefaultInsert.insert(
      df = multipleFieldsPartitioningRowDF,
      tableName = table,
      partitionSpec = Some(HivePartitionSpec(List("partitionedfield1", "partitionedfield2")))
    )
    assertDataFrameAlmostEquals(multipleFieldsPartitioningRowDF, ss.table(table))

  }

}
