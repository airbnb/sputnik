package com.airbnb.sputnik.runner

import java.time.LocalDate

import com.airbnb.sputnik.RunConfiguration.{JobRunConfig, Range, SputnikRunnerConfig}
import com.airbnb.sputnik.SputnikJob.SputnikSession
import com.airbnb.sputnik.utils.{SparkBaseTest, TestRunConfig}
import com.airbnb.sputnik.{Metrics, SputnikJob}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable.ListBuffer

@RunWith(classOf[JUnitRunner])
class StepsJobRunnerTest extends SparkBaseTest {

  test("Test runSingleJobWithSteps") {

    val bufferRanges = new ListBuffer[Range]
    val bufferDS = new ListBuffer[LocalDate]

    val mockJobRunner = new JobRunner {
      override def run(sputnikJob: SputnikJob,
                       sputnikRunnerConfig: SputnikRunnerConfig,
                       sputnikSession: SputnikSession
                      ) = {
        sputnikSession.runConfig.ds match {
          case Left(ds) => {
            bufferDS += ds
          }
          case Right(range) => {
            bufferRanges += range
          }
        }
      }
    }

    val stepsJobRunner = new StepsJobRunner(mockJobRunner)

    var runConfig = JobRunConfig(
      ds = Right(Range(LocalDate.of(2018, 2, 3),
        LocalDate.of(2018, 2, 3)))
    )

    stepsJobRunner.runSingleJobWithSteps(
      TestSputnikJob,
      TestRunConfig.testSputnikRunnerConfig,
      SputnikSession(runConfig, ss, new Metrics),
      5
    )

    assert(bufferDS.toList === List(LocalDate.of(2018, 2, 3)))
    bufferDS.clear()
    runConfig = JobRunConfig(
      ds = Right(Range(LocalDate.of(2018, 2, 3),
        LocalDate.of(2018, 2, 4)))
    )
    stepsJobRunner.runSingleJobWithSteps(
      TestSputnikJob,
      TestRunConfig.testSputnikRunnerConfig,
      SputnikSession(runConfig, ss, new Metrics),
      1
    )
    assert(bufferDS.toList === List(
      LocalDate.of(2018, 2, 3),
      LocalDate.of(2018, 2, 4)
    ))
    bufferDS.clear()
    stepsJobRunner.runSingleJobWithSteps(
      TestSputnikJob,
      TestRunConfig.testSputnikRunnerConfig,
      SputnikSession(runConfig, ss, new Metrics),
      2
    )
    assert(bufferDS.toList === List.empty)
    assert(bufferRanges.toList === List(Range(LocalDate.of(2018, 2, 3),
      LocalDate.of(2018, 2, 4))))
    bufferDS.clear()
    bufferRanges.clear()
    runConfig = JobRunConfig(
      ds = Right(Range(LocalDate.of(2018, 2, 3),
        LocalDate.of(2018, 2, 5)))
    )
    stepsJobRunner.runSingleJobWithSteps(
      TestSputnikJob,
      TestRunConfig.testSputnikRunnerConfig,
      SputnikSession(runConfig, ss, new Metrics),
      2
    )
    assert(bufferDS.toList === List(LocalDate.of(2018, 2, 5)))
    assert(bufferRanges.toList === List(Range(LocalDate.of(2018, 2, 3),
      LocalDate.of(2018, 2, 4))))
  }


  test("Test get ranges with steps") {
    var runConfig = JobRunConfig(
      ds = Right(Range(LocalDate.of(2018, 2, 3),
        LocalDate.of(2018, 2, 3)))
    )

    var ranges = StepsJobRunner.getRanges(runConfig, 1)
    assert(ranges === List(Left(LocalDate.of(2018, 2, 3))))
    ranges = StepsJobRunner.getRanges(runConfig, 10)
    assert(ranges === List(Left(LocalDate.of(2018, 2, 3))))
    runConfig = JobRunConfig(
      ds = Right(Range(LocalDate.of(2018, 2, 3),
        LocalDate.of(2018, 2, 4)))
    )
    ranges = StepsJobRunner.getRanges(runConfig, 1)
    assert(ranges === List(
      Left(LocalDate.of(2018, 2, 3)),
      Left(LocalDate.of(2018, 2, 4))
    ))
    ranges = StepsJobRunner.getRanges(runConfig, 10)
    assert(ranges === List(Right(Range(LocalDate.of(2018, 2, 3), LocalDate.of(2018, 2, 4)))))
    runConfig = JobRunConfig(
      ds = Right(Range(LocalDate.of(2018, 2, 3),
        LocalDate.of(2018, 2, 10)))
    )
    ranges = StepsJobRunner.getRanges(runConfig, 3)
    assert(ranges === List(
      Right(Range(LocalDate.of(2018, 2, 3), LocalDate.of(2018, 2, 5))),
      Right(Range(LocalDate.of(2018, 2, 6), LocalDate.of(2018, 2, 8))),
      Right(Range(LocalDate.of(2018, 2, 9), LocalDate.of(2018, 2, 10)))
    ))
  }

}
