package com.airbnb.sputnik.runner

import java.time.LocalDate
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import com.airbnb.sputnik.RunConfiguration.{JobRunConfig, SputnikRunnerConfig}
import com.airbnb.sputnik.SputnikJob.SputnikSession
import com.airbnb.sputnik.utils.SparkBaseTest
import com.airbnb.sputnik.{Metrics, SputnikJob}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.airbnb.sputnik.RunConfiguration.Range


object TestSputnikJob extends SputnikJob {

  val wasRun = new AtomicBoolean(false)

  override def run(): Unit = {
    wasRun.set(true)
  }

}

@RunWith(classOf[JUnitRunner])
class SputnikJobRunnerTest extends SparkBaseTest {


  test("Test choosing mode to run") {

    val singleJobRun = new AtomicInteger(0)
    val stepsJobRun = new AtomicInteger(0)

    val singleJobRunner = new JobRunner {
      override def run(sputnikJob: SputnikJob,
                       sputnikRunnerConfig: SputnikRunnerConfig,
                       sputnikSession: SputnikJob.SputnikSession): Unit = {
        val runNumber = singleJobRun.incrementAndGet()
        runNumber match {
          case 1 => assert(sputnikSession.runConfig.ds.left.get === LocalDate.MIN)
          case 2 => {
            assert(sputnikSession.runConfig.ds.right.get.start === LocalDate.MIN)
            assert(sputnikSession.runConfig.ds.right.get.end === LocalDate.MAX)
          }
        }
      }
    }

    val stepsRunner = new StepsJobRunner(singleJobRunner) {

      override def runSingleJobWithSteps(job: SputnikJob,
                                         sputnikRunnerConfig: SputnikRunnerConfig,
                                         sputnikSession: SputnikSession,
                                         stepSize: Int
                                        ): Unit = {

        val runNumber = stepsJobRun.incrementAndGet()
        assert(sputnikSession.runConfig.ds.right.get.start === LocalDate.MIN)
        assert(sputnikSession.runConfig.ds.right.get.end === LocalDate.MAX)
        runNumber match {
          case 1 => {
            assert(stepSize === 1)
          }
          case 2 => {
            assert(stepSize === 5)
          }
          case 3 => {
            assert(stepSize === 10)
          }
        }

      }
    }

    val autoRunner = new AutoModeJobRunner(stepsRunner)

    var metaRunConfig = SputnikRunnerConfig()

    var runConfig = JobRunConfig(ds = Left(LocalDate.MIN))

    stepsRunner.run(TestSputnikJob,
      metaRunConfig,
      SputnikSession(runConfig, ss, new Metrics)
    )

    assert(singleJobRun.get() === 1)
    runConfig = JobRunConfig(ds = Right(Range(LocalDate.MIN, LocalDate.MAX)))

    metaRunConfig = metaRunConfig.copy(stepSize = Some(1))

    stepsRunner.run(TestSputnikJob,
      metaRunConfig,
      SputnikSession(runConfig, ss, new Metrics)
    )

    assert(stepsJobRun.get() === 1)

    var newSputnikJob = new SputnikJob {
      override lazy val defaultStepSize: Option[Int] = None

      override def run(): Unit = {
      }
    }

    metaRunConfig = metaRunConfig.copy(stepSize = Some(5))
    stepsRunner.run(newSputnikJob,
      metaRunConfig,
      SputnikSession(runConfig, ss, new Metrics)
    )
    assert(stepsJobRun.get() === 2)
    newSputnikJob = new SputnikJob {

      override lazy val defaultStepSize = Some(10)

      override def run(): Unit = {
      }
    }
    metaRunConfig = metaRunConfig.copy(stepSize = None)
    stepsRunner.run(newSputnikJob,
      metaRunConfig,
      SputnikSession(runConfig, ss, new Metrics)
    )
    assert(stepsJobRun.get() === 3)

    val blya: Option[Int] = None

    newSputnikJob = new SputnikJob {

      override lazy val defaultStepSize: Option[Int] = None

      override def run(): Unit = {

      }
    }

    metaRunConfig = metaRunConfig.copy(stepSize = None)
    stepsRunner.run(newSputnikJob,
      metaRunConfig,
      SputnikSession(runConfig, ss, new Metrics)
    )
    assert(singleJobRun.get() === 2)
  }

}
