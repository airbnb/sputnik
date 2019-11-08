package com.airbnb.sputnik

import java.time.LocalDate

trait AutoModeSputnikJob extends SputnikJob {
  /**
    * The earliest date for which this job should run. Required only when
    * auto mode used to run the job. {@link com.airbnb.sputnik.runner.SparkJobRunner SparkJobRunner}
    * would need to this to understand where to start, when auto filling
    */
  val startDate: LocalDate

  /**
    * List of tables, to which the job writes. Required only when
    * auto mode used to run the job. {@link com.airbnb.sputnik.runner.SparkJobRunner SparkJobRunner}
    * would need to this list to extract the list of existing partitions.
    */
  val outputTables: Seq[String]

}
