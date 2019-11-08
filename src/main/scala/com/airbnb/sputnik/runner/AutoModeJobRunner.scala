package com.airbnb.sputnik.runner

import java.time.LocalDate

import com.airbnb.sputnik.RunConfiguration.{Range, SputnikRunnerConfig}
import com.airbnb.sputnik.SputnikJob.SputnikSession
import com.airbnb.sputnik.hive.HiveUtils
import com.airbnb.sputnik.runner.AutoModeJobRunner._
import com.airbnb.sputnik.tools.DateConverter.dateToString
import com.airbnb.sputnik.tools.EnvironmentSpecificLogic
import com.airbnb.sputnik.{AutoModeSputnikJob, SputnikJob}

import scala.collection.mutable.ListBuffer

object AutoModeJobRunner {

  def getRanges(startDate: LocalDate,
                endDate: LocalDate,
                existingPartitions: Set[String]
               ): Array[Either[LocalDate, Range]] = {
    var iterator = startDate
    val end = endDate
    val result = new ListBuffer[Either[LocalDate, Range]]
    var current: Option[Either[LocalDate, Range]] = None
    while (iterator.isBefore(end) || iterator.isEqual(end)) {
      if (!existingPartitions.contains(dateToString(iterator))) {
        current = current match {
          case None => Some(Left(iterator))
          case Some(Left(ds)) => {
            if (ds.plusDays(1).isEqual(iterator)) {
              Some(Right(Range(ds, iterator)))
            } else {
              result += Left(ds)
              Some(Left(iterator))
            }
          }
          case Some(Right(Range(start, end))) => {
            if (end.plusDays(1).isEqual(iterator)) {
              Some(Right(Range(start, iterator)))
            } else {
              result += Right(Range(start, end))
              Some(Left(iterator))
            }
          }
        }
      }
      iterator = iterator.plusDays(1)
    }
    if (current.isDefined) {
      result += current.get
    }
    result.toArray
  }

}

class AutoModeJobRunner(stepsJobRunner: JobRunner) extends JobRunner {


  override def run(sputnikJob: SputnikJob,
                   sputnikRunnerConfig: SputnikRunnerConfig,
                   sputnikSession: SputnikSession): Unit = {
    assert(sputnikSession.runConfig.ds.isLeft)

    val autoModeSparkJob = if (sputnikJob.isInstanceOf[AutoModeSputnikJob]) {
      sputnikJob.asInstanceOf[AutoModeSputnikJob]
    } else {
      throw new RuntimeException(s"Spark job ${sputnikJob.appName} is not an instance of AutoModeSparkJob")
    }

    assert(sputnikRunnerConfig.autoMode == true, "SputnikRunnerConfig:autoMode must be true to use AutoModeJobRunner")

    logger.info("Running job in auto mode." +
      s" Getting info about available partitions " +
      s" from tables: ${autoModeSparkJob.outputTables.mkString(",")}")

    val existingPartitions: Set[String] =
      autoModeSparkJob
        .outputTables
        .map(table => {
          val resolver = sputnikJob.environmentTableResolver
          EnvironmentSpecificLogic.getOutputTableName(table, resolver, sputnikSession.runConfig)
        })
        .map(table => {
          sputnikSession.ss.catalog.tableExists(table) match {
            case true => {
              HiveUtils.getExistingPartitions(sputnikSession.ss, table)
            }
            case false => {
              Array.empty[String]
            }
          }
        }).reduce((p1, p2) => {
        p1.intersect(p2)
      }
      ).toSet

    logger.info(s"Partitions, which exists in all tables are: ${existingPartitions.mkString("\n")}")

    val ranges = getRanges(startDate = autoModeSparkJob.startDate,
      endDate = sputnikSession.runConfig.ds.left.get,
      existingPartitions = existingPartitions
    )
    logger.info(s"Based on job start date: ${autoModeSparkJob.startDate} " +
      s" and current ds: ${sputnikSession.runConfig.ds.left.get} " +
      s" and existing partitions I'm planning to run job on next ranges: ${ranges.mkString("\n")}")
    val removedAutoConfig = sputnikRunnerConfig.copy(autoMode = false)

    def putRange(range: Either[LocalDate, Range]): SputnikSession = sputnikSession
      .copy(runConfig = sputnikSession.runConfig.copy(ds = range))

    ranges.foreach(range => {
      stepsJobRunner.run(sputnikSession = putRange(range),
        sputnikRunnerConfig = removedAutoConfig,
        sputnikJob = sputnikJob
      )
    })
  }

}
