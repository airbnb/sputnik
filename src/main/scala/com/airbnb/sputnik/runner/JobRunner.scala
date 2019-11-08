package com.airbnb.sputnik.runner

import com.airbnb.sputnik.RunConfiguration.SputnikRunnerConfig
import com.airbnb.sputnik.SputnikJob.SputnikSession
import com.airbnb.sputnik.{Logging, SputnikJob}

trait JobRunner extends Logging {

  def run(sputnikJob: SputnikJob,
          sputnikRunnerConfig: SputnikRunnerConfig,
          sputnikSession: SputnikSession
         ): Unit

}
