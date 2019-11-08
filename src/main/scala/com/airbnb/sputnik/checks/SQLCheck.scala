package com.airbnb.sputnik.checks

import com.airbnb.sputnik.GetResourceFile
import com.airbnb.sputnik.hive.HiveUtils.tableRandomSuffix
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataTypes

import scala.util.{Failure, Success, Try}

/**
  * Abstract SQL check, which helps user define his check through
  * SQL expression.
  */
trait SQLCheck extends Check with Serializable with GetResourceFile {

  val DATAFRAME_TABLE_NAME = "df"

  /**
    *
    * SQL expression for the check.  Select statement should return
    * Integer, Long, Boolean or String values. It can return just
    * one record or a record per every row in temporaryTableName.
    * true, “true”, “True”, 1, 6 is passed. false, “false”, 0 is failed.
    * “2018-05-05” isn't allowed and would throw the Exception.
    *
    * @param temporaryTableName name of the table, which user need to query for
    *                           the result of the job
    * @return sql expression which would be executed in scope of this check
    */
  def sql(temporaryTableName: String): String

  def sqlFromResourceFile(path: String, temporaryTableName: String): String = {
    val fileContent = getFileContent(path)
    fileContent.replace("temporaryTableName", temporaryTableName)
  }

  def check(df: DataFrame): Option[ErrorMessage] = {
    val tableName = s"${DATAFRAME_TABLE_NAME}_${tableRandomSuffix()}"
    df.createOrReplaceTempView(tableName)
    val sqlStatement = sql(temporaryTableName = tableName)
    val result = df.sparkSession.sql(sqlStatement)
    Try(result.foreach(row => {
      row.schema.foreach(field => {
        field.dataType match {
          case DataTypes.StringType | DataTypes.BooleanType => {
            val value = field.dataType match {
              case DataTypes.StringType => row.getAs[String](field.name).toBoolean
              case DataTypes.BooleanType => row.getAs[Boolean](field.name)
            }
            if (!value) {
              throw new RuntimeException(s"Check '$checkDescription' failed." +
                s"false for ${field.name}. row = ${row}")
            }
          }
          case DataTypes.IntegerType | DataTypes.LongType => {
            val value = field.dataType match {
              case DataTypes.IntegerType => row.getAs[Integer](field.name).toLong
              case DataTypes.LongType => row.getAs[Long](field.name)
            }
            if (value == 0) {
              throw new RuntimeException(s"Check '$checkDescription' failed. 0 for ${field.name}. row = ${row}")
            }
          }
          case _ => {
            throw new RuntimeException(s"Check '$checkDescription' can not be parsed. " +
              s" Incorrect data type for field ${field.name}")
          }
        }
      })
    })) match {
      case Success(_) => None
      case Failure(exception) => Some(exception.getMessage)
    }
  }

}
