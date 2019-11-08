package com.airbnb.sputnik.checks

import java.lang.annotation.Annotation

import com.airbnb.sputnik.Logging
import org.apache.spark.sql.DataFrame

object NotNull extends FieldAnnotation {

  override def fromAnnotation(annotation: Annotation, fieldName: String): Check = {
    new NotNull(fieldName)
  }

  override def getAnnotationType: Class[_] = classOf[com.airbnb.sputnik.annotations.checks.NotNull]

}

class NotNull(columnName: String) extends Check with Logging {

  override val checkDescription = s"Checking that column $columnName is not null"
  val errorMessage = s"Column $columnName is null. Number of records with null is: "

  def getColumnName() = columnName

  def check(df: DataFrame): Option[ErrorMessage] = {
    val badRecords = df
      .filter(df.col(columnName).isNull)
    val recordsCount = badRecords.count()

    (recordsCount != 0) match {
      case true => {
        val error = errorMessage + recordsCount
        Some(error)
      }
      case false => {
        logger.info(s"Result contains $recordsCount records")
        None
      }
    }
  }

}