package com.airbnb.sputnik.checks

import java.lang.annotation.Annotation

import com.airbnb.sputnik.Logging
import org.apache.spark.sql.DataFrame

object RecordMinCount extends ClassAnnotation {

  override def fromAnnotation(annotation: Annotation): Check = {
    val minCount = annotation
      .asInstanceOf[com.airbnb.sputnik.annotations.checks.RecordMinCount]
      .minCount()
    new RecordMinCount(minCount)
  }

  override def getAnnotationType: Class[_ <: java.lang.annotation.Annotation]
  = classOf[com.airbnb.sputnik.annotations.checks.RecordMinCount]

}

class RecordMinCount(minCount: Int) extends Check with Logging {

  override val checkDescription = s"Checking that dataframe has at least $minCount records"
  val errorMessage = s"Dataframe has less than $minCount records. Actual count is: "

  def getMinCount() = minCount

  def check(df: DataFrame): Option[ErrorMessage] = {
    val recordsCount = df.count()
    (recordsCount < minCount) match {
      case true => Some(errorMessage + recordsCount)
      case false => {
        logger.info(s"Result contains $recordsCount records")
        None
      }
    }
  }

}
