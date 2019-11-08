package com.airbnb.sputnik.runner

import com.airbnb.sputnik.RunConfiguration.{JobRunConfig, SputnikRunnerConfig}
import com.airbnb.sputnik.SputnikJob.SputnikSession
import com.airbnb.sputnik._
import com.airbnb.sputnik.tools.SparkSessionUtils
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Level

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class SputnikJobRunner extends Logging with SputnikJobRunnerUtils {

  def setUpMetrics(sputnikJob: SputnikJob,
                   jobRunConfig: JobRunConfig,
                   sputnikRunnerConfig: SputnikRunnerConfig
                  ): Metrics = {
    getMetrics()
      .withJobName(sputnikJob.appName)
      .withJobRunConfig(jobRunConfig)
      .withSputnikRunnerConfig(sputnikRunnerConfig)
  }

  def getMetrics() = {
    new Metrics()
  }

  def run(args: Array[String], exitAfter: Boolean, printDriverArgs: Boolean = false): Unit = {

    val (jobRunConfig, sputnikRunnerConfig) = RunConfiguration.parseCommandLine(args.tail)
    val className = args.head
    val sputnikJob = createSputnikJob(className)
    val sparkConf = createSparkConf(sputnikJob, sputnikRunnerConfig, jobRunConfig)
    if (printDriverArgs) {
      println(getDriverArgs(sparkConf))
    } else {
      sputnikRunnerConfig.logLevel match {
        case Some(logLevel) => {
          ConfigFactory.load().getStringList("suppress_logs_packages").asScala.foreach(packageName => {
            org.apache.log4j.Logger.getLogger(packageName).setLevel(logLevel)
          })
        }
        case None =>
      }
      val sparkSession = SparkSessionUtils.getSparkSession(sparkConf)
      val metrics = setUpMetrics(sputnikJob, jobRunConfig, sputnikRunnerConfig)
      val sputnikSession = SputnikSession(jobRunConfig, sparkSession, metrics)
      run(
        sputnikJob = sputnikJob,
        sputnikRunnerConfig = sputnikRunnerConfig,
        sputnikSession = sputnikSession,
        exitAfter = exitAfter
      )
    }
  }

  def run(sputnikJob: SputnikJob,
          sputnikRunnerConfig: SputnikRunnerConfig,
          sputnikSession: SputnikSession,
          exitAfter: Boolean
         ) = {
    val result = sputnikSession.metrics.measureTime("total_job_run", () => {
      Try {
        val singleJobRunner = new SingleJobRunner()
        val stepsJobRunner = new StepsJobRunner(singleJobRunner)
        sputnikRunnerConfig.autoMode match {
          case true => {
            val autoModeRunner = new AutoModeJobRunner(stepsJobRunner)
            autoModeRunner.run(sputnikJob, sputnikRunnerConfig, sputnikSession)
          }
          case false => {
            stepsJobRunner.run(sputnikJob, sputnikRunnerConfig, sputnikSession)
          }
        }
      }
    })


    sputnikSession.metrics.jobStatus(result)
    sputnikSession.metrics.close(sputnikSession.ss)

    if (exitAfter) {
      sputnikSession.ss.stop()
    }

    result match {
      case Success(_) => {
        logger.info(s"Job succeeded.")
        if (exitAfter) {
          System.exit(0)
        }
      }
      case Failure(e) => {
        logger.error(s"Job failed.")
        e.printStackTrace()
        if (exitAfter) {
          System.exit(-1)
        } else {
          throw e
        }
      }
    }
  }

}

object DriverArgsPrinter extends SputnikJobRunner {

  def main(args: Array[String]): Unit = {
    org.apache.log4j.Logger.getRootLogger.setLevel(Level.ERROR)
    run(args, true, printDriverArgs = true)
  }

}

object SputnikJobRunner extends SputnikJobRunner {

  def main(args: Array[String]): Unit = {
    run(args, true)
  }

}