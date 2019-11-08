package com.airbnb.sputnik.runner

import java.time.LocalDate

import com.airbnb.sputnik.RunConfiguration.{JobRunConfig, Range, SputnikRunnerConfig}
import com.airbnb.sputnik.SputnikJob.SputnikSession
import com.airbnb.sputnik.runner.StepsJobRunner.getRanges
import com.airbnb.sputnik.{Logging, SputnikJob}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

object StepsJobRunner extends Logging {

  def getRanges(runConfig: JobRunConfig,
                stepSize: Int): List[Either[LocalDate, Range]] = {
    val result = new ListBuffer[Either[LocalDate, Range]]
    runConfig.ds match {
      case Left(ds) => {
        result += Left(ds)
      }
      case Right(range) => {
        var current = range.start
        val end = range.end

        while (current.isBefore(end) || current.isEqual(end)) {
          val startRange = current
          current = current.plusDays(stepSize - 1)
          val endRange = if (current.isBefore(end) || current.isEqual(end)) {
            current
          } else {
            end
          }
          current = current.plusDays(1)
          endRange.isEqual(startRange) match {
            case true => result += Left(startRange)
            case false => result += Right(Range(startRange, endRange))
          }
        }
      }
    }

    logger.info(s"Got new ranges to run based on step size. " +
      s"Number of ranges = ${result.size}. " +
      s"Ranges are: ${result.mkString(",")}")
    result.toList
  }

}

class StepsJobRunner(singleJobRunner: JobRunner) extends JobRunner {

  def run(sputnikJob: SputnikJob,
          sputnikRunnerConfig: SputnikRunnerConfig,
          sputnikSession: SputnikSession
         ): Unit = {
    if (sputnikSession.runConfig.ds.isLeft) {
      logger.info("Console ds argument was specified, so running just single day job")
      singleJobRunner.run(
        sputnikJob,
        sputnikRunnerConfig,
        sputnikSession
      )
    } else if (sputnikJob.defaultStepSize.equals(Some(1))) {
      logger.info("SparkJob require to run one day at a time," +
        " so running with steps, where step size is 1")
      runSingleJobWithSteps(
        sputnikJob,
        sputnikRunnerConfig,
        sputnikSession,
        1
      )
    } else {

      val (stepSize, explanation) = if (sputnikRunnerConfig.stepSize.isDefined) {
        (sputnikRunnerConfig.stepSize, "Step size defined in console")
      } else {
        (sputnikJob.defaultStepSize, "Taking step size from default from a job")
      }

      stepSize match {
        case Some(step) => {
          logger.info(s"Running job with steps. Step size = $step. $explanation")
          runSingleJobWithSteps(
            sputnikJob,
            sputnikRunnerConfig,
            sputnikSession,
            step
          )
        }
        case None => {
          logger.info("Running job without steps, just from startDate to endDate.")
          singleJobRunner.run(
            sputnikJob,
            sputnikRunnerConfig,
            sputnikSession
          )
        }
      }
    }
  }

  def runSingleJobWithSteps(job: SputnikJob,
                            sputnikRunnerConfig: SputnikRunnerConfig,
                            sputnikSession: SputnikSession,
                            stepSize: Int
                           ): Unit = {
    val ranges: List[Either[LocalDate, Range]] = getRanges(sputnikSession.runConfig, stepSize)
    logger.info(s"Start running jobs based on new date ranges. Ranges are: ${ranges.mkString("\n")}")
    ranges.foreach(range => {
      logger.info(s"Running job for range $range")

      val newSputnikSession = sputnikSession.copy(runConfig = sputnikSession.runConfig.copy(ds = range))
      val startedAtNanos = System.nanoTime()
      singleJobRunner.run(job, sputnikRunnerConfig, newSputnikSession)
      val duration = (System.nanoTime() - startedAtNanos).nanos
      logger.info(s"Finished $range in ${duration.toMinutes.minutes}")
    })
    logger.info("Done with running job for ranges")
  }


}
