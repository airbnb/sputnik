package com.airbnb.sputnik.tools

import com.airbnb.sputnik.SputnikJobUtils._
import com.airbnb.sputnik.{Logging, SputnikJob}
import org.apache.commons.cli.{BasicParser, Options}

import scala.util.{Failure, Success, Try}

object CompareTablesJob extends DataFrameSuiteBase with SputnikJob with Logging {

  val TABLE_1_PARAMETER = "table1"
  val TABLE_2_PARAMETER = "table2"

  def run(): Unit = {

    val jobArgs = getJobArguments()
    compare(jobArgs)
  }

  def compare(jobArgs: String) = {
    val args = jobArgs.split(" ")
    val options = new Options
    options.addOption(TABLE_1_PARAMETER, true, "")
    options.addOption(TABLE_2_PARAMETER, true, "")

    val parser = new BasicParser
    val result = parser.parse(options, args)
    val table1 = result.getOptionValue(TABLE_1_PARAMETER)
    val table2 = result.getOptionValue(TABLE_2_PARAMETER)
    logger.info(s"Comparing table ${table1} with ${table2} for ${daysToProcess.map(DateConverter.dateToString(_)).mkString(",")}")
    val table1Dataframe = hiveTableReader.getDataframe(table1)
    val table2Dataframe = hiveTableReader.getDataframe(table2)
    Try {
      assertDataFrameAlmostEquals(table1Dataframe, table2Dataframe)
    } match {
      case Success(_) => logger.info("Tables are equal")
      case Failure(exception) => {
        logger.error("Table are not equal", exception)
        throw exception
      }
    }
  }
}
