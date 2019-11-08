package com.airbnb.sputnik.runner

import java.time.LocalDate

import com.airbnb.sputnik.RunConfiguration._
import com.airbnb.sputnik.{HoconConfigSputnikJob, SputnikJob}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SputnikJobRunnerUtilsTest extends FunSuite {

  object TestSputnikJobWithHocon extends HoconConfigSputnikJob {

    override val configPath: Option[String] = Some("spark_job_runner_test.conf")

    def run() = {
    }
  }

  object TestJobWithWrongHoconPath extends HoconConfigSputnikJob {

    override val configPath: Option[String] = Some("path_which_do_not_exist.conf")

    def run() = {
    }
  }


  object TestSputnikJobWithSparkConf extends SputnikJob {

    override lazy val sparkConfigs: Map[String, String] = Map("spark.executor.memory" -> "100g")

    def run() = {
    }
  }

  test("Test creating SparkConf") {

    val runConfig = JobRunConfig(ds = Left(LocalDate.of(2018, 12, 5)), writeConfig =
      WriteConfig(writeEnvironment = Environment.PROD))
    var sputnikRunnerConfig = SputnikRunnerConfig(autoMode = false)
    var conf = SputnikJobRunner.createSparkConf(TestSputnikJob, sputnikRunnerConfig, runConfig)

    assert(conf.get("hive.exec.dynamic.partition.mode") === "nonstrict") // from reference.conf

    // test setReducersNumber
    sputnikRunnerConfig = sputnikRunnerConfig.copy(autoMode = true)
    conf = SputnikJobRunner.createSparkConf(TestSputnikJob, sputnikRunnerConfig, runConfig)

    assert(conf.get("hive.exec.dynamic.partition.mode") === "nonstrict") // from reference.conf

    conf = SputnikJobRunner.createSparkConf(TestSputnikJobWithSparkConf, sputnikRunnerConfig, runConfig)

    assert(conf.get("hive.exec.dynamic.partition.mode") === "nonstrict") // from reference.conf
    assert(conf.get("spark.executor.memory") === "100g") // set in the job

    conf = SputnikJobRunner.createSparkConf(TestSputnikJobWithHocon, sputnikRunnerConfig, runConfig)

    assert(conf.get("hive.exec.dynamic.partition.mode") === "nonstrict") // from reference.conf
    assert(conf.get("spark.executor.memory") === "200g") // set in the spark_job_runner_test.conf
    assertThrows[NullPointerException] {
      TestJobWithWrongHoconPath.config.hasPath("something")
    }
  }

}
