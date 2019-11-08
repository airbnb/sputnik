package com.airbnb.sputnik.hive

import com.airbnb.sputnik.tools.FieldAnnotationUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}

object SchemaNormalisation {

  @transient lazy val logger = org.apache.log4j.LogManager.getLogger(this.getClass)

  def ignoreNullable(schema: Seq[StructField]) = schema.map(_.copy(nullable = true))


  def compareSchema(structType1: StructType,
                    structType1Name: String,
                    structType2: StructType,
                    structType2Name: String,
                    firstMightBeSubset: Boolean = false
                   ) = {

    case class Field(fieldName: String, typeName: String)

    def getFields(structType: StructType) = structType.fields.map(field => {
      Field(field.name, field.dataType.simpleString)
    })

    val commonPartOfError = s"$structType1Name schema:\n${structType1.treeString}" +
      s"$structType2Name schema:\n${structType2.treeString} ".stripMargin

    val structType1Compare = getFields(structType1)
    val structType2Compare = getFields(structType2)
    structType1Compare.foreach(field => {
      if (!structType2Compare.contains(field)) {
        val errorMessage =
          s"$structType1Name schema contains field $field," +
            s" which does not exists in schema of $structType2Name. $commonPartOfError"
        logger.error(errorMessage)
        throw new RuntimeException(errorMessage)
      }
    })
    structType2Compare.foreach(field => {
      if (!structType1Compare.contains(field) && !firstMightBeSubset) {
        val errorMessage =
          s"$structType2Name schema contains field $field," +
            s" which does not exists in result table $structType1Name. $commonPartOfError"
        logger.error(errorMessage)
        throw new RuntimeException(errorMessage)
      }
    })
  }

  def normaliseSchema[T](df: DataFrame,
                         itemClass: Class[T]
                        ): DataFrame = {
    val fields = FieldAnnotationUtils.getFields(itemClass).map(_.getName)
    df.selectExpr(fields: _*)
  }

  def normaliseSchema(df: DataFrame,
                      tableName: String,
                      subsetIsOk: Boolean = false
                     ): DataFrame = {
    val tableSchema = df
      .sparkSession
      .table(tableName)
      .schema
    val dfSchema = df
      .schema

    compareSchema(
      tableSchema, s"table $tableName",
      dfSchema, "df",
      firstMightBeSubset = subsetIsOk)

    reorder(df, tableSchema, subsetIsOk)
  }

  def reorder(df: DataFrame,
              schema: StructType,
              subsetIsOk: Boolean = false
             ): DataFrame = {
    val dfColumns = df.columns
    val columns = schema
      .map(_.name)
      .filter(name => {
        (!subsetIsOk) || dfColumns.contains(name)
      })
      .map(df.col(_))
    df.select(columns: _*)
  }

}
