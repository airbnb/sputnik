package com.airbnb.sputnik.hive

import com.airbnb.sputnik.RunConfiguration.WriteConfig
import com.airbnb.sputnik.enums.TableFileFormat
import com.airbnb.sputnik.{DS_FIELD, Logging}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.{MetadataBuilder, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object TableCreation extends Logging {

  val MAX_COMMENT_LENGTH = 256

  def createTableAndUpdateProperties(tableName: String,
                                     dataframe: DataFrame,
                                     writeConfig: WriteConfig,
                                     hiveTableProperties: HiveTableProperties = SimpleHiveTableProperties()
                                    ) = {

    val schema = dataframe.schema

    val fields = hiveTableProperties.partitionSpec match {
      case Some(spec) => schema.fields
        .filter(f => {
          if (spec.columns.contains(f.name) && !f.dataType.equals(StringType)) {
            throw new RuntimeException("Can partition only on a string field")
          }
          !spec.columns.contains(f.name)
        })
      case None => schema.fields
    }

    val schemaString =
      fields.map(field => {
        import field._
        s"${field.name} ${dataType.catalogString}"
      })
        .mkString("\n, ")

    if (!dataframe.sparkSession.catalog.tableExists(tableName)) {
      logger.info(s"Result table $tableName does not exists. Creating.")
      val partitionedStatement = partitioningStatement(hiveTableProperties.partitionSpec, dataframe)
      val rowSerdeFormat = if (TableFileFormat.RCFILE.equals(hiveTableProperties.fileFormat))
        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe'"
      else ""
      val createTableStatement =
        s"""
           |CREATE TABLE IF NOT EXISTS ${tableName} (
           |${schemaString}
           |)
           | $partitionedStatement
           | $rowSerdeFormat
           |STORED AS ${hiveTableProperties.fileFormat}
       """.stripMargin
      logger.info(s"Create table statement is \n $createTableStatement")
      dataframe.sparkSession.sql(createTableStatement)
    }

    if (!hiveTableProperties.fieldComments.isEmpty) {
      val comments = hiveTableProperties.fieldComments
      // alter table to update column comment
      val catalog = dataframe.sparkSession.sessionState.catalog
      val Array(dbName, justTableName) = tableName.split('.')
      val tableIdentifier = TableIdentifier(justTableName, Some(dbName))
      val table = catalog.getTableMetadata(tableIdentifier)
      val newDataSchema = table.dataSchema.fields.map {
        field =>
          val newComment = comments.get(field.name)
            .map(c => c.substring(0, math.min(c.length, MAX_COMMENT_LENGTH)))

          (field.getComment(), newComment) match {
            case (None, Some(comment)) => field.withComment(comment)
            case (Some(_), None) => {
              val newMetadata = new MetadataBuilder()
                .withMetadata(field.metadata)
                .remove("comment")
                .build()
              field.copy(metadata = newMetadata)
            }
            case (Some(oldComment), Some(comment)) if oldComment != comment => {
              field.withComment(comment)
            }
            case _ => field
          }
      }
      catalog.alterTableDataSchema(tableIdentifier, StructType(newDataSchema))
    }

    val metaInformation = hiveTableProperties.getMetaInformation
    if (!metaInformation.trim.isEmpty) {
      val alterTableStatement = s" ALTER TABLE ${tableName} SET TBLPROPERTIES (${metaInformation})"
      logger.info(s"Alter table statement is \n $alterTableStatement")
      dataframe.sparkSession.sql(alterTableStatement)
    }
  }

  def partitioningStatement(partitionSpec: Option[HivePartitionSpec],
                            dataframe: DataFrame) = {
    partitionSpec match {
      case Some(spec) => {
        val columnsInRightOrder = dataframe
          .columns
          .filter(column => spec.columns.contains(column))
        val columnsStatement =
          columnsInRightOrder
            .map(k => s"${k} STRING")
            .mkString("\n, ")
        s""" PARTITIONED BY ($columnsStatement) """.stripMargin
      }
      case None => ""
    }
  }


}
