package com.airbnb.sputnik.runner

import com.airbnb.sputnik.utils.{SparkBaseTest, TestRunConfig}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SingleJobRunnerTest extends SparkBaseTest {

  test("Test runSingleJob") {
    assert(TestSputnikJob.wasRun.get() === false)
    new SingleJobRunner().run(
      TestSputnikJob,
      TestRunConfig.testSputnikRunnerConfig,
      TestRunConfig.sputnikSession(ss)
    )
    assert(TestSputnikJob.wasRun.get() === true)
  }

}
