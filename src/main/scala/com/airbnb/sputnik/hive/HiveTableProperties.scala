package com.airbnb.sputnik.hive

import com.typesafe.config.Config
import com.airbnb.sputnik.enums.TableFileFormat;

/**
  * Properties of the table. Most would be included as metainformation to the Hive.
  *
  * @param description             table comment that briefly explains the dataset this table
  *                                represents
  * @param tableRetention          how long to keep table and why
  * @param archiveDays             days of data above which is archived to S3. data can still be
  *                                accessed as normal but may become slow than storing on HDFS.
  *                                storage becomes much cheaper and does not count towards HDFS
  *                                quota
  * @param backupEnabled           whether to backup data to prevent data loss. should be set to
  *                                true
  *                                for only for data that cannot be regenerated or takes a long time
  *                                to regenerate
  * @param defaultWriteParallelism default number of part files to write (per partition) can be
  *                                overwritten in dataframe write function
  * @param otherMetaInformation    other information to include to table metainformation
  * @param fieldComments           comments on the fields in this table
  * @param fileFormat              file format for the files with data of the table
  */
trait HiveTableProperties {

  def description: String

  def defaultWriteParallelism: Option[Int]

  def otherMetaInformation: Map[String, String]

  def fieldComments: Map[String, String]

  def fileFormat: TableFileFormat

  def fieldsSubsetIsAllowed: Boolean

  def partitionSpec: Option[HivePartitionSpec]

  def getCollectOtherMetaInformation: Seq[(String, String)] = {
    otherMetaInformation.toSeq
  }

  def getMetaInformation: String = {
    val metaInformation = getCollectOtherMetaInformation ++
      Seq(
        "description" -> description
      )
        .map { case (k, v) => (k, v.toString) }

    metaInformation.filter { case (k, v) => v.nonEmpty }
      .map { case (k, v) => s"'${k}' = '${v}'" }
      .mkString("  ", "\n, ", "")
  }

}


case class SimpleHiveTableProperties(
                                      description: String = "",
                                      defaultWriteParallelism: Option[Int] = Some(50),
                                      otherMetaInformation: Map[String, String] = Map.empty,
                                      fieldComments: Map[String, String] = Map.empty,
                                      fileFormat: TableFileFormat = TableFileFormat.PARQUET,
                                      fieldsSubsetIsAllowed: Boolean = false,
                                      partitionSpec: Option[HivePartitionSpec] = HivePartitionSpec.DS_PARTITIONING
                                    ) extends HiveTableProperties


object HiveTableProperties {

  def getMap(config: Config) = {
    import scala.collection.JavaConverters._
    config
      .entrySet()
      .asScala
      .map(entry => {
        entry.getKey -> entry.getValue.unwrapped().toString
      })
      .toMap
  }

  def fromConfig(config: Config): HiveTableProperties = {
    var result = SimpleHiveTableProperties()
    if (config.hasPath("description")) {
      result = result.copy(description = config.getString("description"))
    }

    if (config.hasPath("defaultWriteParallelism")) {
      result = result.copy(defaultWriteParallelism = Some(config.getInt("defaultWriteParallelism")))
    }

    if (config.hasPath("partitionSpec")) {
      import scala.collection.JavaConverters._
      result = result.copy(partitionSpec = Some(
        HivePartitionSpec(config.getStringList("partitionSpec").asScala)))
    }

    if (config.hasPath("otherMetaInformation")) {
      result = result.copy(otherMetaInformation = getMap(config.getConfig("otherMetaInformation")))
    }
    if (config.hasPath("fieldComments")) {
      result = result.copy(fieldComments = getMap(config.getConfig("fieldComments")))
    }
    if (config.hasPath("fileFormat")) {
      result = result.copy(fileFormat = TableFileFormat.valueOf(config.getString("fileFormat")))
    }
    if (config.hasPath("fieldsSubsetIsAllowed")) {
      result = result.copy(fieldsSubsetIsAllowed = config.getBoolean("fieldsSubsetIsAllowed"))
    }

    result
  }

}
