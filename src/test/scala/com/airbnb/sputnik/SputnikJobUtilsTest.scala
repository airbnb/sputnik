package com.airbnb.sputnik

import java.time.LocalDate

import com.airbnb.sputnik.RunConfiguration.{JobRunConfig, Range}
import com.airbnb.sputnik.SputnikJob.SputnikSession
import com.airbnb.sputnik.SputnikJobUtils.daysToProcess
import com.airbnb.sputnik.utils.SparkBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SputnikJobUtilsTest extends SparkBaseTest {

  test("Test daysToProcess") {

    implicit var sputnikSession = SputnikSession(JobRunConfig(ds = Left(LocalDate.MIN)), ss, new Metrics)

    assert(daysToProcess() === List(LocalDate.MIN))
    sputnikSession = sputnikSession.copy(runConfig = JobRunConfig(
      ds = Right(Range(LocalDate.of(2018, 2, 3),
        LocalDate.of(2018, 2, 3)))
    ))

    assert(daysToProcess() === List(LocalDate.of(2018, 2, 3)))
    sputnikSession = sputnikSession.copy(runConfig = JobRunConfig(
      ds = Right(Range(LocalDate.of(2018, 2, 3),
        LocalDate.of(2018, 2, 5)))
    ))

    assert(daysToProcess() === List(
      LocalDate.of(2018, 2, 3),
      LocalDate.of(2018, 2, 4),
      LocalDate.of(2018, 2, 5)
    ))
  }

}
