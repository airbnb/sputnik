package com.airbnb.sputnik.checks

import java.lang.annotation.Annotation

import com.airbnb.sputnik.Logging
import org.apache.spark.sql.DataFrame

/**
  * Checks, that result of the job is not empty. The check is not free.
  */
object NotEmptyCheck extends Check with ClassAnnotation with Logging {

  override val checkDescription = "Checking that dataframe is not empty"
  val errorMessage = "Dataframe is empty"

  override def fromAnnotation(annotation: Annotation): Check = {
    this
  }

  override def getAnnotationType: Class[_ <: java.lang.annotation.Annotation
  ] = classOf[com.airbnb.sputnik.annotations.checks.NotEmptyCheck]

  def check(df: DataFrame): Option[ErrorMessage] = {
    val recordsCount = df.count()
    (recordsCount == 0) match {
      case true => Some(errorMessage)
      case false => {
        logger.info(s"Result contains $recordsCount records")
        None
      }
    }
  }

}
