package com.airbnb.sputnik

import java.time.LocalDate

import com.airbnb.sputnik.RunConfiguration.Environment.Environment
import com.airbnb.sputnik.tools.DateConverter
import org.apache.commons.cli.{BasicParser, Options}
import org.apache.log4j.Level

import scala.collection.mutable.ListBuffer

object RunConfiguration {

  @transient lazy val logger = org.apache.log4j.LogManager.getLogger(this.getClass)

  case class Range(start: LocalDate, end: LocalDate) {
    override def toString: String = s"($start, $end)"
  }

  object Environment extends Enumeration {
    type Environment = Value
    val DEV, STAGE, PROD = Value
  }


  case class WriteConfig(
                          writeEnvironment: Environment = Environment.PROD,
                          addUserNameToTable: Boolean = false,
                          dropResultTables: Option[Boolean] = None,
                          backfill : Boolean = false,
                          repartition: Boolean = false
                        )

  case class JobRunConfig(
                           ds: Either[LocalDate, Range] = Left(LocalDate.MIN),
                           jobArguments: Option[String] = None,
                           configPath: Option[String] = None,
                           writeConfig: WriteConfig = WriteConfig(),
                           readEnvironment: Environment = Environment.PROD,
                           sample: Option[Double] = None
                         )

  case class SputnikRunnerConfig(
                                  stepSize: Option[Int] = None,
                                  autoMode: Boolean = false,
                                  compareResults: Option[String] = None,
                                  logLevel: Option[Level] = None
                                )

  val DS_COMMAND = "ds"
  val START_DATE_COMMAND = "startDate"
  val END_DATE_COMMAND = "endDate"
  val READ_ENVIRONMENT_COMMAND = "readEnv"
  val WRITE_ENVIRONMENT_COMMAND = "writeEnv"
  val DROP_RESULT_TABLES_COMMAND = "dropResultTables"
  val REPARTITION_COMMAND = "repartition"
  val AUTO_MODE = "autoMode"
  val STEP_SIZE = "stepSize"
  val JOB_ARGUMENTS = "jobArguments"
  val ADD_USERNAME_SUFFIX = "addUserNameToTable"
  val COMPARE_RESULTS = "compareResults"
  val LOG_LEVEL = "logLevel"
  val SAMPLE = "sample"
  val CONFIG_PATH = "configPath"
  val BACKFILL = "backfill"

  type StepSize = Int

  def parseCommandLine(args: Array[String]): (JobRunConfig, SputnikRunnerConfig) = {
    logger.info(s"Command line arguments are: ${args.mkString(" ")}")
    val options = new Options
    options.addOption(DS_COMMAND, true, "Date to process")
    options.addOption(START_DATE_COMMAND, true, "For date range processing. Start of the date range")
    options.addOption(END_DATE_COMMAND, true, "For date range processing. End of the date range")
    options.addOption(DROP_RESULT_TABLES_COMMAND, true, "Set to true, if you want to drop table and create new")
    options.addOption(REPARTITION_COMMAND, true, "Do not do repartitioning before writing down the result")
    options.addOption(STEP_SIZE, true, "Number of days in a single batch to process")
    options.addOption(AUTO_MODE, true, "")
    options.addOption(JOB_ARGUMENTS, true, "")
    options.addOption(ADD_USERNAME_SUFFIX, true, "")
    options.addOption(COMPARE_RESULTS, true, "")
    options.addOption(LOG_LEVEL, true, "")
    options.addOption(READ_ENVIRONMENT_COMMAND, true, "")
    options.addOption(WRITE_ENVIRONMENT_COMMAND, true, "")
    options.addOption(SAMPLE, true, "")
    options.addOption(CONFIG_PATH, true, "")
    options.addOption(BACKFILL, true, "")

    val parser = new BasicParser
    val result = parser.parse(options, args)

    def getOption(parameter: String): Option[String] = {
      if (result.hasOption(parameter)) {
        Some(result.getOptionValue(parameter))
      } else {
        None
      }
    }

    val simpleDs = getOption(DS_COMMAND).map(DateConverter.stringToDate(_))
    val repartition = getOption(REPARTITION_COMMAND).getOrElse("true").toBoolean
    val dropResultTables = getOption(DROP_RESULT_TABLES_COMMAND).map(_.toBoolean)
    val startDate = getOption(START_DATE_COMMAND).map(DateConverter.stringToDate(_))
    val endDate = getOption(END_DATE_COMMAND).map(DateConverter.stringToDate(_))
    val stepSize = getOption(STEP_SIZE).map(_.toInt)
    val autoMode = getOption(AUTO_MODE).getOrElse("false").toBoolean
    val testTimestampSuffix = getOption(ADD_USERNAME_SUFFIX).getOrElse("false").toBoolean
    val jobArguments = getOption(JOB_ARGUMENTS)
    val compareResults = getOption(COMPARE_RESULTS)
    val logLevel = getOption(LOG_LEVEL).map(Level.toLevel(_))
    val writeEnv = getOption(WRITE_ENVIRONMENT_COMMAND).map(Environment.withName).getOrElse(Environment.DEV)
    val readEnv = getOption(READ_ENVIRONMENT_COMMAND).map(Environment.withName).getOrElse(Environment.PROD)
    val sample = getOption(SAMPLE).map(_.toDouble)
    val configPath = getOption(CONFIG_PATH)
    val backfill = getOption(BACKFILL).map(_.toBoolean).getOrElse(false)

    if ((simpleDs.isDefined && (startDate.isDefined || endDate.isDefined)) ||
      (simpleDs.isEmpty && (startDate.isEmpty || endDate.isEmpty))
    ) {
      throw new RuntimeException(s"Either $DS_COMMAND or range($START_DATE_COMMAND and $END_DATE_COMMAND)" +
        s" should be defined, not both.")
    }

    val ds: Either[LocalDate, Range] = simpleDs match {
      case Some(date) => Left(date)
      case None => Right(Range(startDate.get, endDate.get))
    }

    val sputnikRunnerConfig =
      SputnikRunnerConfig(
        stepSize = stepSize,
        compareResults = compareResults,
        autoMode = autoMode,
        logLevel = logLevel
      )

    val jobRunConfig = JobRunConfig(
      ds = ds,
      jobArguments = jobArguments,
      configPath = configPath,
      writeConfig = WriteConfig(
        dropResultTables = dropResultTables,
        addUserNameToTable = testTimestampSuffix,
        repartition = repartition,
        writeEnvironment = writeEnv,
        backfill = backfill
      ),
      readEnvironment = readEnv,
      sample = sample
    )

    logger.info(s"SputnikRunnerConfig is $sputnikRunnerConfig")
    logger.info(s"JobRunConfig is $jobRunConfig")
    (jobRunConfig, sputnikRunnerConfig)
  }

  def daysToProcess(ds: Either[LocalDate, Range]): List[LocalDate] = {
    ds match {
      case Left(ds) => List(ds)
      case Right(range) => {
        var current = range.start
        val end = range.end
        val result = new ListBuffer[LocalDate]
        while (current.isBefore(end) || current.isEqual(end)) {
          result += current
          current = current.plusDays(1)
        }
        result.toList
      }
    }
  }

}
