package com.airbnb.sputnik.checks

import org.apache.spark.sql.DataFrame

/**
  * Implement a check to verify the result of the job
  * before writing it to result table. It's an implementation
  * of check-exchange model inside sputnik framework. Motivation for
  * making the check before writing the result is not to trigger downstream
  * jobs with incorrect data.
  */
trait Check {

  type ErrorMessage = String

  /**
    * Description of the check for logging and exception throwing.
    * User is encouraged to redefine this.
    */
  val checkDescription = this.getClass.getCanonicalName

  /**
    * Method, which user should define to check the result data.
    *
    * @param df result data from the job. Contains data for single partition
    * @return result of the check. None - if the check has passed and there is not error.
    *         Some(error) is check failed.
    */
  def check(df: DataFrame): Option[ErrorMessage]

}
