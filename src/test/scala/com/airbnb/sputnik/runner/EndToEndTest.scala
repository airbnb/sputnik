package com.airbnb.sputnik.runner

import com.airbnb.sputnik.SputnikJob
import com.airbnb.sputnik.hive.HivePartitionSpec
import com.airbnb.sputnik.runner.TestJob.Data
import com.airbnb.sputnik.utils.{HiveTestDataWriter, SparkBaseTest}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

object TestJob extends SputnikJob {

  val INPUT_TABLE = "users.input"
  val OUTPUT_TABLE = "users.output"

  override lazy val sparkConfigs: Map[String, String] = Map("spark.driver.memory" -> "5g")

  def run(): Unit = {
    val input = hiveTableReader.getDataframe(INPUT_TABLE)
    val resultTable = hiveTableWriter.saveAsHiveTable(
      dataFrame = input,
      dbTableName = OUTPUT_TABLE
    )
    log.info(s"Wrote data to $resultTable")
  }

  case class Data(someField: String, ds: String)

}

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends SparkBaseTest {

  test("Print driver args") {
    val consoleCommand =
      """com.airbnb.sputnik.runner.TestJob --dropResultTables true --startDate 2018-01-01 --endDate 2018-01-03 --writeEnv DEV"""
    DriverArgsPrinter.run(consoleCommand.split(" "), false, true)
  }

  test("Test runSingleJob") {
    ss.sql("create database if not exists users")

    import spark.implicits._
    drop(TestJob.INPUT_TABLE)
    drop(TestJob.OUTPUT_TABLE)
    val inputData = List(
      Data("01", "2018-01-01"),
      Data("02", "2018-01-02"),
      Data("03", "2018-01-03"),
      Data("04", "2018-01-04"),
      Data("05", "2018-01-05"),
      Data("06", "2018-01-06"),
      Data("07", "2018-01-07"),
      Data("08", "2018-01-08"),
      Data("09", "2018-01-09"),
      Data("10", "2018-01-10")
    ).toDF()
    HiveTestDataWriter.writeInputDataForTesting(inputData,
      TestJob.INPUT_TABLE,
      partitionSpec = HivePartitionSpec.DS_PARTITIONING
    )

    assert(ss.catalog.tableExists(TestJob.OUTPUT_TABLE) === false)

    var consoleCommand =
      """com.airbnb.sputnik.runner.TestJob --ds 2018-01-05 --writeEnv PROD"""
    SputnikJobRunner.run(consoleCommand.split(" "), false)

    assert(ss.table(TestJob.OUTPUT_TABLE).as[Data].collect() === Array(Data("05", "2018-01-05")))

    consoleCommand =
      """com.airbnb.sputnik.runner.TestJob --ds 2018-01-06 --writeEnv PROD"""
    SputnikJobRunner.run(consoleCommand.split(" "), false)

    assert(ss.table(TestJob.OUTPUT_TABLE).as[Data].collect() === Array(
      Data("05", "2018-01-05"),
      Data("06", "2018-01-06")
    ))

    consoleCommand =
      """com.airbnb.sputnik.runner.TestJob --startDate 2018-01-07 --endDate 2018-01-09 --writeEnv PROD --logLevel ERROR"""
    SputnikJobRunner.run(consoleCommand.split(" "), false)

    assert(ss.table(TestJob.OUTPUT_TABLE).as[Data].collect().sortBy(_.ds) === Array(
      Data("05", "2018-01-05"),
      Data("06", "2018-01-06"),
      Data("07", "2018-01-07"),
      Data("08", "2018-01-08"),
      Data("09", "2018-01-09")
    ))

    consoleCommand =
      """com.airbnb.sputnik.runner.TestJob --startDate 2018-01-07 --endDate 2018-01-09 --writeEnv PROD --logLevel ERROR --backfill true"""
    SputnikJobRunner.run(consoleCommand.split(" "), false)

    assert(ss.table("users.backfill_in_progress__output").as[Data].collect().sortBy(_.ds) === Array(
      Data("07", "2018-01-07"),
      Data("08", "2018-01-08"),
      Data("09", "2018-01-09")
    ))

    consoleCommand =
      """com.airbnb.sputnik.runner.TestJob --dropResultTables true --startDate 2018-01-01 --endDate 2018-01-03 --writeEnv PROD"""
    SputnikJobRunner.run(consoleCommand.split(" "), false)

    assert(ss.table(TestJob.OUTPUT_TABLE).as[Data].collect().sortBy(_.ds) === Array(
      Data("01", "2018-01-01"),
      Data("02", "2018-01-02"),
      Data("03", "2018-01-03")
    ))

    // Test env behavior with different environments

    consoleCommand =
      """com.airbnb.sputnik.runner.TestJob --dropResultTables true --startDate 2018-01-01 --endDate 2018-01-03 --writeEnv DEV"""
    SputnikJobRunner.run(consoleCommand.split(" "), false)

    assert(ss.table(TestJob.OUTPUT_TABLE + "_dev").as[Data].collect().sortBy(_.ds) === Array(
      Data("01", "2018-01-01"),
      Data("02", "2018-01-02"),
      Data("03", "2018-01-03")
    ))

  }

}

