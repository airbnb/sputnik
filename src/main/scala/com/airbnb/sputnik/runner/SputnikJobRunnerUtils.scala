package com.airbnb.sputnik.runner

import com.airbnb.sputnik.HoconConfigSputnikJob.getMap
import com.airbnb.sputnik.RunConfiguration.{JobRunConfig, SputnikRunnerConfig}
import com.airbnb.sputnik.SputnikJob.SputnikSession
import com.airbnb.sputnik.tools.DateConverter.dateToString
import com.airbnb.sputnik.{HoconConfigSputnikJob, Logging, SputnikJob}
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe
import scala.util.{Failure, Success, Try}

trait SputnikJobRunnerUtils extends Logging {

  def YARNAppName(appName: String,
                  sputnikRunnerConfig: SputnikRunnerConfig,
                  jobRunConfig: JobRunConfig
                 ) = {
    val prefixBuffer = new ListBuffer[String]
    prefixBuffer += "Write env:" + jobRunConfig.writeConfig.writeEnvironment.toString
    prefixBuffer += "Read env:" + jobRunConfig.readEnvironment.toString

    if (sputnikRunnerConfig.autoMode) {
      prefixBuffer += "AutoMode"
    }

    jobRunConfig.ds match {
      case Left(ds) => {
        prefixBuffer += s"DS: ${dateToString(ds)}"
      }
      case Right(range) => {
        prefixBuffer += s"Start date: ${dateToString(range.start)}"
        prefixBuffer += s"End date: ${dateToString(range.end)}"
      }
    }

    prefixBuffer += sputnikRunnerConfig.stepSize.map(stepSize => s"Step Size: $stepSize").getOrElse("")
    val yarnAppName = prefixBuffer.mkString(", ") + appName
    logger.info(s"YARN app name is $yarnAppName")
    yarnAppName
  }

  def createSputnikJob(className: String): SputnikJob = {
    Try(createInstance(className).asInstanceOf[SputnikJob])
      .transform(
        s => Success(s),
        e => Failure(new RuntimeException("Probably your job is 'class' and not 'object'. Your job should be an object", e)))
      .get
  }

  def createInstance(jobName: String, packageName: Option[String] = None) = {
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val path = packageName match {
      case Some(pn) => s"$pn.$jobName"
      case None => jobName
    }
    val module = runtimeMirror.staticModule(path)
    Try(runtimeMirror.reflectModule(module).instance).getOrElse({
      // Seems to be Java
      val classObject = Class.forName(path)
      val constructor = classObject.getConstructor()
      constructor.newInstance()
    })
  }

  def createSparkConf(sputnikJob: SputnikJob,
                      sputnikRunnerConfig: SputnikRunnerConfig,
                      jobRunConfig: JobRunConfig
                     ): SparkConf = {
    var sparkConf = new SparkConf()
    val applicationConfig = ConfigFactory.load()
    getMap(applicationConfig.getConfig("sparkConfigs"))
      .foreach(x => sparkConf = sparkConf.set(x._1, x._2))
    if (sputnikJob.isInstanceOf[HoconConfigSputnikJob]) {
      sputnikJob.setup(SputnikSession(jobRunConfig, null, null))
    }
    sputnikJob
      .sparkConfigs
      .foreach(x => sparkConf = sparkConf.set(x._1, x._2))
    val YarnAppName = YARNAppName(sputnikJob.appName, sputnikRunnerConfig, jobRunConfig)
    sparkConf = sparkConf.setAppName(YarnAppName)
    sparkConf
  }

  def getDriverArgs(conf: SparkConf): String = {
    val default = new SparkConf()
    conf.getAll.filter({ case (key, value) => {
      if (key.startsWith("spark.driver")) {
        !default.contains(key) || !default.get(key).equals(value)
      } else {
        false
      }
    }
    }).map({ case (key, value) => {
      s"--conf $key=$value"
    }
    }).mkString(" ")
  }

}

object SputnikJobRunnerUtils extends SputnikJobRunnerUtils
