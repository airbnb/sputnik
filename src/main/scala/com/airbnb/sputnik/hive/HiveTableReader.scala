package com.airbnb.sputnik.hive

import com.airbnb.sputnik.DS_FIELD
import com.airbnb.sputnik.SputnikJob.SputnikSession
import com.airbnb.sputnik.annotations.TableName
import com.airbnb.sputnik.tools.DateConverter.dateToString
import com.airbnb.sputnik.tools.{DatasetEncoder, DefaultEnvironmentTableResolver, EnvironmentSpecificLogic, EnvironmentTableResolver}
import org.apache.spark.sql.DataFrame

object HiveTableReader {

  def getTableName[T](itemClass: Class[T]): String = {
    try {
      itemClass.getAnnotation(classOf[TableName]).value
    } catch {
      case _: NullPointerException => {
        throw new RuntimeException("Annotation TableName " +
          "does not exist.")
      }
    }
  }

}

/**
  * Helper class to read from Hive tables
  */
class HiveTableReader(sputnikSession: SputnikSession,
                      environmentTableResolver: EnvironmentTableResolver = DefaultEnvironmentTableResolver) extends MetricsCollecting {

  override val sputnikMetrics = sputnikSession.metrics

  def getDataset(itemClass: Class[_],
                 tableName: Option[String] = None,
                 dateBoundsOffset: Option[DateOffset] = None,
                 partition: Option[String] = None): DataFrame = {
    val resultTableName = tableName.getOrElse(
      itemClass.getAnnotation(classOf[TableName]).value()
    )
    var dataFrame = getDataframe(resultTableName,
      dateBoundsOffset,
      partition)

    dataFrame = ColumnsFormatting.dfToJava(dataFrame, itemClass)
    dataFrame = DatasetEncoder.toJava(dataFrame, itemClass)
    dataFrame = SchemaNormalisation.normaliseSchema(dataFrame, itemClass)
    dataFrame
  }

  /**
    * Run aware way to read from Hive. This method works with logic in Sputnik to get the only
    * the data from Hive, which should be processed in this run. Usage of this method should
    * always be preferred comparing with reading directly from Hive.
    *
    * @param tableName     Name of the table to read from. Including database(namespace) Example: "core_data.listings"
    * @param partition     other partition besides ds to filter when  partition, in the format of xxx=yyyy
    * @return Dataframe with the data, which the job should process in this run
    */
  def getDataframe(tableName: String,
                   dateBoundsOffset: Option[DateOffset] = None,
                   partition: Option[String] = None
                  ): DataFrame = {
    val resolvedTableName = EnvironmentSpecificLogic
      .getInputTableName(tableName,
        environmentTableResolver,
        sputnikSession.runConfig)

    if (!tableName.equals(resolvedTableName)) {
      logger.info(s"Input table $tableName was resolved to $resolvedTableName")
    }
    var df = sputnikSession.ss
      .table(resolvedTableName)

    df = filterByDs(resolvedTableName, df, dateBoundsOffset)
    if (partition.nonEmpty)
      df = filterByPartition(resolvedTableName, df, partition.get)
    df = sampleIfNeeded(resolvedTableName, df)
    df = collectMetrics(resolvedTableName, df)
    df
  }

  private def filterByDs(tableName: String,
                         df: DataFrame,
                         dateBoundsOffset: Option[DateOffset]
                        ): DataFrame = {
    val containsDsField = df.schema.fieldNames.contains(DS_FIELD)
    logger.info(s"Getting dataframe for table $tableName. Contains DS field = $containsDsField")
    if (containsDsField) {
      val (beginDay, endDay) = sputnikSession.runConfig.ds match {
        case Left(ds) => (ds, ds)
        case Right(range) => (range.start, range.end)
      }
      val (beginDayWithOffset, endDayWithOffset) = dateBoundsOffset match {
        case Some(dateOffset) => (dateOffset.getLowerBoundDate(beginDay), dateOffset.getUpperBoundDate(endDay))
        case None => (beginDay, endDay)
      }
      df.where(df.col(DS_FIELD).between(dateToString(beginDayWithOffset), dateToString(endDayWithOffset)))
    } else {
      df
    }
  }

  private def filterByPartition(tableName: String,
                                df: DataFrame,
                                partition: String): DataFrame = {
    val partitions = partition.split("=")
    val containsField = df.schema.fieldNames.contains(partitions(0))
    logger.info(s"Getting dataframe for table $tableName. Contains $partitions(0) field = $containsField")
    if (containsField) {
      df.where(df.col(partitions(0)).equalTo(partitions(1)))
    } else {
      df
    }
  }

  private def sampleIfNeeded(tableName: String, df: DataFrame) = {
    sputnikSession.runConfig.sample.map(sampleSize => {
      logger.info(s"Sampling input data for table $tableName with fraction: $sampleSize")
      df.sample(sampleSize)
    }).getOrElse(df)
  }


}
