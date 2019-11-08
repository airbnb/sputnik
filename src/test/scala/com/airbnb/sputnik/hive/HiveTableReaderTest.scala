package com.airbnb.sputnik.hive

import java.time.LocalDate

import com.airbnb.sputnik.Metrics
import com.airbnb.sputnik.RunConfiguration.{Environment, JobRunConfig, Range}
import com.airbnb.sputnik.SputnikJob.SputnikSession
import com.airbnb.sputnik.annotations.FieldsFormatting
import com.airbnb.sputnik.annotations.TableName
import com.airbnb.sputnik.hive.DateOffset._
import com.airbnb.sputnik.tools.beans.ParsedData
import com.airbnb.sputnik.utils.{HiveTestDataWriter, SputnikJobBaseTest}
import com.google.common.base.CaseFormat
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

case class Data(first_field: String, ds: String)

case class DataExtraField(first_field: String, extraField: String, ds: String)

case class InputData(field_one: String, json_field_one: String)

case class DataInSQL(first_field: String, ds: String)

@TableName("default.someTablePartitionedData")
case class partitionData(field_one: String, partition_one: String, ds: String)

@FieldsFormatting(CaseFormat.LOWER_UNDERSCORE)
case class DataInJava(first_field: String, ds: String)

@RunWith(classOf[JUnitRunner])
class HiveTableReaderTest extends SputnikJobBaseTest {

  test("test getTableName method") {
    assert(HiveTableReader.getTableName(classOf[ParsedData]) === "default.someTableParsedData")
  }

  test("test getDataframe method") {
    val table = s"default.test_simpleGetDataFrame"
    val sparkSession = ss
    import sparkSession.implicits._
    drop(table)
    val df = List(
      Data("1", "2018-01-01"),
      Data("2", "2018-01-02"),
      Data("3", "2018-01-03"),
      Data("4", "2018-01-04"),
      Data("5", "2018-01-05"),
      Data("6", "2018-01-06")
    ).toDF

    HiveTestDataWriter.writeInputDataForTesting(df,
      table,
      partitionSpec = HivePartitionSpec.DS_PARTITIONING)

    var runConfig = JobRunConfig()
      .copy(ds = Left(LocalDate.of(2018, 1, 3)))

    var hiveTableReader = new HiveTableReader(new SputnikSession(
      runConfig = runConfig,
      ss = ss,
      new Metrics()))

    var result = hiveTableReader.getDataframe(table).as[Data]
    assert(result.collect() === Array(Data("3", "2018-01-03")))

    result = hiveTableReader.getDataframe(table, DS_AND_DATE_BEFORE_THAT).as[Data]
    assert(result.collect() === Array(Data("2", "2018-01-02"), Data("3", "2018-01-03")))

    result = hiveTableReader.getDataframe(table, Some(DateBoundsOffset(lowerOffset = -10))).as[Data]
    assert(result.collect() === Array(Data("1", "2018-01-01"), Data("2", "2018-01-02"), Data("3", "2018-01-03")))

    result = hiveTableReader.getDataframe(table, Some(DateBoundsOffset(lowerOffset = -10, upperOffset = -1))).as[Data]
    assert(result.collect() === Array(Data("1", "2018-01-01"), Data("2", "2018-01-02")))

    result = hiveTableReader.getDataframe(table, FROM_BEGINNING_TO_DS).as[Data]
    assert(result.collect() === Array(Data("1", "2018-01-01"), Data("2", "2018-01-02"), Data("3", "2018-01-03")))

    runConfig = runConfig
      .copy(ds = Right(Range(LocalDate.of(2018, 1, 3),
        LocalDate.of(2018, 1, 5))))

    hiveTableReader = new HiveTableReader(new SputnikSession(
      runConfig = runConfig,
      ss = ss,
      new Metrics()))

    result = hiveTableReader.getDataframe(table).as[Data]
    assert(result.collect() === Array(
      Data("3", "2018-01-03"),
      Data("4", "2018-01-04"),
      Data("5", "2018-01-05")
    ))

    result = hiveTableReader.getDataframe(table, DS_AND_DATE_BEFORE_THAT).as[Data]
    assert(result.collect() === Array(
      Data("2", "2018-01-02"),
      Data("3", "2018-01-03"),
      Data("4", "2018-01-04"),
      Data("5", "2018-01-05")
    ))

    result = hiveTableReader.getDataframe(table, FROM_BEGINNING_TO_DS).as[Data]
    assert(result.collect().sortBy(_.ds) === Array(
      Data("1", "2018-01-01"),
      Data("2", "2018-01-02"),
      Data("3", "2018-01-03"),
      Data("4", "2018-01-04"),
      Data("5", "2018-01-05")
    ))

    // Test getting data from different environments

    runConfig = runConfig.copy(readEnvironment = Environment.DEV)

    hiveTableReader = new HiveTableReader(new SputnikSession(
      runConfig = runConfig,
      ss = ss,
      new Metrics()))

    assertThrows[AnalysisException] {
      hiveTableReader.getDataframe(table)
    }

    HiveTestDataWriter.writeInputDataForTesting(df,
      table + "_dev",
      partitionSpec = HivePartitionSpec.DS_PARTITIONING)

    assert(hiveTableReader.getDataframe(table).count() === 3L)
  }

  test("test reading dataset") {
    val spark = ss
    import spark.implicits._
    val input = List(InputData("sfsd", "{\"innerFieldOne\":\"hi\"}")).toDF()
    val table = "default.someTableParsedData"
    HiveTestDataWriter.writeInputDataForTesting(
      dataset = input,
      dbTableName = table
    )
    implicit val runConfig = JobRunConfig()
    val output = hiveTableReader()
      .getDataset(classOf[ParsedData])
      .head()
    assert(output.getAs[String]("fieldOne") === "sfsd")
    assert(output.getAs[StructType]("jsonFieldOne").asInstanceOf[GenericRowWithSchema].getAs[String]("innerFieldOne") === "hi")
  }

  test("test reading dataset with subset of fields") {
    val spark = ss
    import spark.implicits._
    val input = List(DataExtraField("sfsd", "sdasdffs", "2006-01-01")).toDF()
    val table = "default.subset_fields"
    HiveTestDataWriter.writeInputDataForTesting(
      dataset = input,
      dbTableName = table,
      partitionSpec = HivePartitionSpec.DS_PARTITIONING
    )
    val runConfig = JobRunConfig()
      .copy(ds = Left(LocalDate.of(2018, 1, 3)))

    val hiveTableReader = new HiveTableReader(new SputnikSession(
      runConfig = runConfig,
      ss = ss,
      new Metrics()))
    val output = hiveTableReader
      .getDataset(classOf[Data], tableName = Some(table), FROM_BEGINNING_TO_DS)
      .head()
    assert(output.getAs[String]("first_field") === "sfsd")
    assert(output.getAs[String]("ds") === "2006-01-01")
  }

  test("test reading dataset with partition") {
    val spark = ss
    import spark.implicits._
    val input = List(
      partitionData("453", "createdAccount", "2018-12-01"),
      partitionData("453", "deletedAccount", "2018-12-01"),
      partitionData("342345353", "createdAccount", "2018-12-01"),
      partitionData("345342", "createdAccount", "2018-12-01"),
      partitionData("345342", "deletedAccount", "2018-12-01"),
      partitionData("2345", "createdAccount", "2018-12-02"),
      partitionData("45342", "createdAccount", "2018-12-02"),
      partitionData("34345", "createdAccount", "2018-12-03"),
      partitionData("34345", "deletedAccount", "2018-12-03"),
      partitionData("32543", "createdAccount", "2018-12-03")
    ).toDF()

    val table = "default.someTablePartitionedData"
    HiveTestDataWriter.writeInputDataForTesting(
      dataset = input,
      dbTableName = table,
      partitionSpec = Some(HivePartitionSpec(List("ds", "partition_one")))
    )

    val runConfig = JobRunConfig()
      .copy(ds = Left(LocalDate.of(2018, 12, 3)))

    val hiveTableReader = new HiveTableReader(new SputnikSession(
      runConfig = runConfig,
      ss = ss,
      new Metrics()))

    val output = hiveTableReader
      .getDataset(classOf[partitionData], partition = Some("partition_one=deletedAccount"))
      .head()
    assert(output.getAs[String]("partition_one") === "deletedAccount")
    assert(output.getAs[String]("field_one") === "34345")
  }
}
