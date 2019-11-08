package com.airbnb.sputnik.runner

import com.airbnb.sputnik.RunConfiguration.SputnikRunnerConfig
import com.airbnb.sputnik.SputnikJob
import com.airbnb.sputnik.SputnikJob.SputnikSession

class SingleJobRunner extends JobRunner {

  var ranBefore = false

  def run(sputnikJob: SputnikJob,
          sputnikRunnerConfig: SputnikRunnerConfig,
          sputnikSession: SputnikSession
         ) = {
    val resultSputnikSession = if (ranBefore) {
      sputnikSession.copy(runConfig =
        sputnikSession.runConfig.copy(writeConfig =
          sputnikSession.runConfig.writeConfig.copy(dropResultTables = None)))
    } else {
      sputnikSession
    }
    sputnikSession.metrics.measureTime(
      metricName = "runSingleJob",
      () => {
        sputnikJob.setup(resultSputnikSession)
        sputnikJob.run()
        sputnikJob.hiveTableReader.close()
        sputnikJob.hiveTableWriter.close()
      })
    ranBefore = true
  }

}
