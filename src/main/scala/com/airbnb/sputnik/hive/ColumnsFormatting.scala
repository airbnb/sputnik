package com.airbnb.sputnik.hive

import com.airbnb.sputnik.annotations.FieldsFormatting
import com.google.common.base.CaseFormat
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object ColumnsFormatting {

  private def toJava(caseFormat: CaseFormat, underscore: String): String = {
    caseFormat.to(CaseFormat.LOWER_CAMEL, underscore)
  }

  private def toSQL(caseFormat: CaseFormat, camel: String): String = {
    CaseFormat.LOWER_CAMEL.to(caseFormat, camel)
  }


  def dfToSQL(df: DataFrame, itemClass: Class[_]) = formatIfNeeded(df, itemClass, toSQL)

  def dfToJava(df: DataFrame, itemClass: Class[_]) = formatIfNeeded(df, itemClass, toJava)

  private def formatIfNeeded(df: DataFrame, itemClass: Class[_], formatting: (CaseFormat, String) => String): DataFrame = {
    val caseFormat = getFieldFormatting(itemClass)
    caseFormat match {
      case Some(format) => {
        val renamed = df.columns
          .map(column => {
            col(column) as formatting(format, column)
          })
        df.select(renamed: _*)
      }
      case None => {
        df
      }
    }
  }

  def formatComments(comments: Map[String, String], itemClass: Class[_]): Map[String, String] = {
    getFieldFormatting(itemClass) match {
      case Some(format) =>
        comments.map({ case (field, comment) => {
          (toSQL(format, field), comment)
        }
        })
      case None => comments
    }
  }

  private def getFieldFormatting[T](itemClass: Class[_]): Option[CaseFormat] = {
    if (itemClass.isAnnotationPresent(classOf[FieldsFormatting])) {
      Some(itemClass.getAnnotation(classOf[FieldsFormatting]).value())
    } else {
      None
    }
  }

}
