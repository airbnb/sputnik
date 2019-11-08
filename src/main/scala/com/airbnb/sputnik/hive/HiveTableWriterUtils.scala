package com.airbnb.sputnik.hive

import java.lang.annotation.Annotation

import com.airbnb.sputnik.RunConfiguration.WriteConfig
import com.airbnb.sputnik.annotations._
import com.airbnb.sputnik.checks._
import com.airbnb.sputnik.hive.HiveTableWriterUtils.getComments
import com.airbnb.sputnik.tools.FieldAnnotationUtils
import com.airbnb.sputnik.{DS_FIELD, Logging}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType


class AnnotationParser extends Serializable {

  val classCheckAnnotations: List[ClassAnnotation] = List(
    NotEmptyCheck,
    RecordMinCount
  )

  val fieldCheckAnnotations: List[FieldAnnotation] = List(
    NotNull
  )

  def getChecks(itemClass: Class[_]): List[Check] = {
    val classChecks = classCheckAnnotations
      .filter(classAnnotation => itemClass.isAnnotationPresent(classAnnotation.getAnnotationType))
      .map(classAnnotation => {
        val annotation = itemClass
          .getAnnotation(classAnnotation.getAnnotationType)
          .asInstanceOf[Annotation]
        classAnnotation.fromAnnotation(annotation)
      })
    val fieldsChecks =
      fieldCheckAnnotations.flatMap(fieldAnnotation => {
        FieldAnnotationUtils
          .getFieldsAnnotation(itemClass, fieldAnnotation.getAnnotationType)
          .map({case (fieldName, annotation) => {
            fieldAnnotation.fromAnnotation(annotation, fieldName)
          }})
      })
    classChecks ++ fieldsChecks
  }

  def getTableProperties(itemClass: Class[_]): HiveTableProperties = {
    var hiveTableProperties = SimpleHiveTableProperties()

    def present[T](annotation: Class[_ <: Annotation]) = {
      itemClass.isAnnotationPresent(annotation)
    }

    val fieldComments = getComments(itemClass)

    if (!fieldComments.isEmpty) {
      hiveTableProperties = hiveTableProperties.copy(fieldComments = fieldComments)
    }

    if (present(classOf[TableDescription])) {
      val description = itemClass.getAnnotation(classOf[TableDescription]).value()
      hiveTableProperties = hiveTableProperties.copy(description = description)
    }

    if (present(classOf[FieldsSubsetIsAllowed])) {
      val fieldsSubsetIsAllowed = itemClass.getAnnotation(classOf[FieldsSubsetIsAllowed]).value()
      hiveTableProperties = hiveTableProperties.copy(fieldsSubsetIsAllowed = fieldsSubsetIsAllowed)
    }

    if (present(classOf[TableParallelism])) {
      val parallelism = itemClass.getAnnotation(classOf[TableParallelism]).value()
      hiveTableProperties = if (parallelism != -1) {
        hiveTableProperties.copy(defaultWriteParallelism = Some(parallelism))
      } else {
        hiveTableProperties.copy(defaultWriteParallelism = None)
      }
    }

    if (present(classOf[TableFormat])) {
      val fileFormat = itemClass.getAnnotation(classOf[TableFormat]).value()
      hiveTableProperties = hiveTableProperties.copy(fileFormat = fileFormat)
    }

    val partitionSpec = FieldAnnotationUtils.getFieldsAnnotation(
      itemClass = itemClass,
      annotationClass = classOf[PartitioningField]
    ).map(_._1).toList

    if (!partitionSpec.isEmpty) {
      hiveTableProperties = hiveTableProperties.copy(partitionSpec = Some(HivePartitionSpec(partitionSpec)))
    } else {
      hiveTableProperties = hiveTableProperties.copy(partitionSpec = None)
    }

    hiveTableProperties
  }

}

object DefaultAnnotationParser extends AnnotationParser

object HiveTableWriterUtils extends Logging {

  def getComments(itemClass: Class[_]): Map[String, String] = {
    val comments = FieldAnnotationUtils
      .getFieldsAnnotation(itemClass, classOf[Comment])
      .map({ case (fieldName, annotation) => {
        fieldName -> annotation.asInstanceOf[Comment].value()
      }
      })
    ColumnsFormatting.formatComments(comments, itemClass)
  }


  def repartition(
                   df: DataFrame,
                   writeConfig: WriteConfig,
                   hiveTableProperties: HiveTableProperties = SimpleHiveTableProperties()
                 ): DataFrame = {
    if (writeConfig.repartition && hiveTableProperties.defaultWriteParallelism.isDefined) {
      val partitionNumber = hiveTableProperties.defaultWriteParallelism.get
      logger.info(s"Repartitioning with $partitionNumber partitions")
      repartition(
        df,
        partitionNumber
      )
    } else {
      logger.info("Do not repartition because of the flag in writeConfig")
      df
    }
  }

  val PARTITION_BY_COLUMN = "partition_by_column"

  def repartition(df: DataFrame,
                  partitionNumber: Int
                 ): DataFrame = {
    if (df.columns.contains(DS_FIELD)) {
      logger.info(s"Partitioning with help of $DS_FIELD field. partitionNumber = $partitionNumber")
      var dfResult = addPartitionColumn(df, partitionNumber)
      dfResult = dfResult.repartition(dfResult.col(PARTITION_BY_COLUMN))
      dfResult.drop(dfResult.col(PARTITION_BY_COLUMN))
    } else {
      df.repartition(partitionNumber)
    }
  }

  def addPartitionColumn(df: DataFrame,
                         partitionNumber: Int): DataFrame = {
    import org.apache.spark.sql.functions.{concat, rand}
    val column = concat(df.col(DS_FIELD), rand().*(partitionNumber).cast(IntegerType))
    df.withColumn(PARTITION_BY_COLUMN, column)
  }

}
