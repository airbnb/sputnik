package com.airbnb.sputnik.tools

import com.airbnb.sputnik.Logging
import com.airbnb.sputnik.annotations.{JsonField, MapField}
import com.airbnb.sputnik.tools.FieldAnnotationUtils.getFields
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Encoders}

import scala.collection.mutable.ListBuffer

object DatasetEncoder extends Logging {

  def toSQL(dataFrame: DataFrame, encoderClass: Class[_]): DataFrame = {
    val mapFieldsAnnotations = FieldAnnotationUtils.getFieldsAnnotation(
      itemClass = encoderClass,
      annotationClass = classOf[MapField]
    )
    val jsonFieldsAnnotations = FieldAnnotationUtils.getFieldsAnnotation(
      itemClass = encoderClass,
      annotationClass = classOf[JsonField]
    )
    val fields = getFieldsMap(encoderClass)

    val columns = dataFrame.columns.toList.sorted
    val fieldNames = fields.keySet.toList.sorted
    assert(columns == fieldNames,
      "Wrong usage of this method. Columns in input dataframe should have the same " +
        s"names as fields in encoderCLass. Dataframe field = ${columns.mkString(",")} " +
        s", fields = ${fieldNames.mkString(",")}, ")

    var resultDataFrame = dataFrame
    columns
      .foreach(columnName => {
        val oldColumn = resultDataFrame.col(columnName)
        if (jsonFieldsAnnotations.get(columnName).isDefined) {
          resultDataFrame = resultDataFrame.withColumn(columnName, to_json(oldColumn))
        } else if (mapFieldsAnnotations.get(columnName).isDefined) {
          val field = fields.get(columnName).get
          val mapClassFields = getFields(field.getType)
          val typeOfAKeyInMap = mapClassFields.head.getType

          mapClassFields.foreach(mapClassField =>
            assert(mapClassField.getType == typeOfAKeyInMap,
              "Types of inner class for map should be identical"))

          val mapColumns = new ListBuffer[Column]
          mapClassFields.foreach(mapClassField => {
            mapColumns += lit(mapClassField.getName)
            mapColumns += resultDataFrame.col(field.getName + "." + mapClassField.getName)
          })
          resultDataFrame = resultDataFrame.withColumn(columnName, map(mapColumns: _*))
        }
      })
    resultDataFrame
  }

  def toJava(dataFrame: DataFrame, encoderClass: Class[_]): DataFrame = {
    val fields = getFieldsMap(encoderClass)
    val mapFieldsAnnotations = FieldAnnotationUtils.getFieldsAnnotation(
      itemClass = encoderClass,
      annotationClass = classOf[MapField]
    )
    val jsonFieldsAnnotations = FieldAnnotationUtils.getFieldsAnnotation(
      itemClass = encoderClass,
      annotationClass = classOf[JsonField]
    )

    var resultDataFrame = dataFrame
    dataFrame
      .columns
      .foreach(columnName => {
        fields.get(columnName) match {
          case Some(field) => {
            if (mapFieldsAnnotations.get(field.getName).isDefined || jsonFieldsAnnotations.get(field.getName).isDefined) {
              val fieldSchema = Encoders.bean(field.getType).schema
              if (fieldSchema.isEmpty) {
                throw new RuntimeException(s"${field.getType} doesn't look like valid java bean." +
                  s" Maybe it's a case class? sputnik right now supports only java beans")
              }
              var column = dataFrame(columnName)
              if (mapFieldsAnnotations.get(field.getName).isDefined) {
                column = to_json(column)
              }
              val newColumn = from_json(column, fieldSchema)
              resultDataFrame = resultDataFrame.withColumn(columnName, newColumn)
            }
          }
          case None => logger.error(s"Could not find field for column name $columnName. " +
            s"Available fields are ${fields.keySet.mkString(",")}")
        }
      })
    resultDataFrame
  }

  def getFieldsMap(encoderClass: Class[_]) = {
    getFields(encoderClass)
      .map(field => field.getName -> field)
      .toMap
  }

}
