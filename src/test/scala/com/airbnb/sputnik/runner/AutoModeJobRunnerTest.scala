package com.airbnb.sputnik.runner

import java.time.LocalDate

import com.airbnb.sputnik.RunConfiguration._
import com.airbnb.sputnik.SputnikJob.SputnikSession
import com.airbnb.sputnik.example.Data
import com.airbnb.sputnik.hive.HivePartitionSpec
import com.airbnb.sputnik.runner.AutoModeJobRunner._
import com.airbnb.sputnik.tools.DateConverter.dateToString
import com.airbnb.sputnik.utils.{HiveTestDataWriter, SparkBaseTest}
import com.airbnb.sputnik.{AutoModeSputnikJob, Metrics, SputnikJob}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AutoModeJobRunnerTest extends SparkBaseTest {

  test("Test auto run") {

    val sputnikJob = new SputnikJob with AutoModeSputnikJob {
      override val outputTables: Seq[String] = List("default.table1")
      override val startDate: LocalDate = LocalDate.of(2018, 11, 25)

      override def run(): Unit = {}
    }

    import spark.implicits._

    val existingData = Data.newUsers.toDF
    HiveTestDataWriter
      .writeInputDataForTesting(
        existingData,
        "default.table1",
        partitionSpec = HivePartitionSpec.DS_PARTITIONING)

    var invocationNumber = 0

    val mockJobRunner = new JobRunner {
      override def run(sputnikJob: SputnikJob,
                       sputnikRunnerConfig: SputnikRunnerConfig,
                       sputnikSession: SputnikJob.SputnikSession): Unit = {
        assert(sputnikRunnerConfig.autoMode === false)
        invocationNumber += 1
        invocationNumber match {
          case 1 => assert(sputnikSession.runConfig.ds === Right(Range(LocalDate.of(2018, 11, 25), LocalDate.of(2018, 11, 30))))
          case 2 => assert(sputnikSession.runConfig.ds === Right(Range(LocalDate.of(2018, 12, 4), LocalDate.of(2018, 12, 5))))
        }
      }
    }

    val autoModeJobRunner = new AutoModeJobRunner(mockJobRunner)
    val sputnikRunnerConfig = SputnikRunnerConfig(autoMode = true)

    val runConfig = JobRunConfig(ds = Left(LocalDate.of(2018, 12, 5)), writeConfig =
      WriteConfig(writeEnvironment = Environment.PROD))

    autoModeJobRunner.run(sputnikJob,
      sputnikRunnerConfig,
      SputnikSession(runConfig, ss, new Metrics)
    )

    assert(invocationNumber === 2)
  }

  test("Test get ranges based on existing partitions") {
    var startDate = LocalDate.of(2018, 2, 2)
    var endDate = LocalDate.of(2018, 2, 3)
    var result = getRanges(startDate, endDate, Set.empty)
    assert(result === Array(Right(Range(startDate, endDate))))

    endDate = LocalDate.of(2018, 2, 2)
    result = getRanges(startDate, endDate, Set.empty)
    assert(result === Array(Left(endDate)))

    result = getRanges(startDate, endDate, Set(dateToString(endDate)))
    assert(result === Array())

    startDate = LocalDate.of(2018, 2, 2)
    endDate = LocalDate.of(2018, 2, 4)
    result = getRanges(startDate, endDate, Set(dateToString(LocalDate.of(2018, 2, 3))))
    assert(result === Array(Left(startDate), Left(endDate)))

    startDate = LocalDate.of(2018, 2, 2)
    endDate = LocalDate.of(2018, 2, 5)
    result = getRanges(startDate, endDate, Set(dateToString(LocalDate.of(2018, 2, 3))))
    assert(result === Array(Left(startDate), Right(Range(LocalDate.of(2018, 2, 4), LocalDate.of(2018, 2, 5)))))

    startDate = LocalDate.of(2018, 2, 2)
    endDate = LocalDate.of(2018, 2, 5)
    result = getRanges(startDate, endDate, Set(dateToString(LocalDate.of(2018, 2, 4)),
      dateToString(LocalDate.of(2018, 2, 5))
    ))
    assert(result === Array(Right(Range(LocalDate.of(2018, 2, 2), LocalDate.of(2018, 2, 3)))))

    startDate = LocalDate.of(2018, 11, 25)
    endDate = LocalDate.of(2018, 12, 5)
    result = getRanges(startDate, endDate, Set(dateToString(LocalDate.of(2018, 12, 1)),
      dateToString(LocalDate.of(2018, 12, 2)),
      dateToString(LocalDate.of(2018, 12, 3))
    ))
    assert(result === Array(
      Right(Range(LocalDate.of(2018, 11, 25), LocalDate.of(2018, 11, 30))),
      Right(Range(LocalDate.of(2018, 12, 4), LocalDate.of(2018, 12, 5)))
    ))
  }

}
