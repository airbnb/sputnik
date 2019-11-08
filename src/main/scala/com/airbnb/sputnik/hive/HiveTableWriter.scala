package com.airbnb.sputnik.hive

import java.util.concurrent.TimeUnit

import com.airbnb.sputnik.RunConfiguration.{JobRunConfig, WriteConfig}
import com.airbnb.sputnik.annotations._
import com.airbnb.sputnik.checks.Check
import com.airbnb.sputnik.hive.HiveTableWriterUtils._
import com.airbnb.sputnik.tools.{DatasetEncoder, DefaultEnvironmentTableResolver, EnvironmentSpecificLogic, EnvironmentTableResolver}
import com.airbnb.sputnik.{DS_FIELD, Metrics}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Helper class to write data to Hive tables
  */
class HiveTableWriter(
                       runConfig: JobRunConfig,
                       insert: Insert,
                       metrics: Metrics = new Metrics,
                       environmentTableResolver: EnvironmentTableResolver = DefaultEnvironmentTableResolver,
                       annotationParser: AnnotationParser = DefaultAnnotationParser
                     ) extends MetricsCollecting {

  override val sputnikMetrics = metrics

  def saveDatasetAsHiveTable[T](
                                 dataset: Dataset[T],
                                 itemClass: Class[T],
                                 checks: Seq[Check] = Seq.empty,
                                 dbTableName: Option[String] = None
                               ): String = {
    scala.reflect.runtime.universe
      .runtimeMirror(Thread.currentThread().getContextClassLoader)
      .staticClass(itemClass.getTypeName)
    val resultTableName = dbTableName.getOrElse(
      itemClass.getAnnotation(classOf[TableName]).value()
    )
    val tableProperties = annotationParser.getTableProperties(itemClass)
    val checksFromAnnotations = annotationParser.getChecks(itemClass)
    var df = SchemaNormalisation.normaliseSchema(dataset.toDF(), itemClass)
    df = DatasetEncoder.toSQL(df, itemClass)
    df = ColumnsFormatting.dfToSQL(df, itemClass)
    saveAsHiveTable(
      dataFrame = df,
      dbTableName = resultTableName,
      hiveTableProperties = tableProperties,
      checks = checks ++ checksFromAnnotations
    )
  }

  /**
    * Save DataFrame to Hive as a partitioned table (create new table if needed)
    *
    * Logic included:
    * <p><ul>
    * <li> It changes the result table name, if we run job in staging mode.
    * <li> It creates table with “CREATE TABLE” hive statement, if table does not yet exist. Default spark writer to hive does not do that and it creates problems with compatibility with other systems.
    * <li> It updates table metainformation
    * <li> It drops the result table and creates a new one if we specify such behavior with a command-line flag, so we can easily iterate in developer mode
    * <li> It changes schema of dataframe according to result schema of table, so even if we change the logic and it would result in change in order of the fields we would write correctly.
    * <li> It repartitions and tries to reduce number of result files on disk
    * <li> It does the checks on result, before inserting it.
    * </ul><p>
    *
    * Table is reshuffled according to specified parallelism to control number of output files.
    * This is usually desirable for medium sized output (< 1TB) as it decouples upstream
    * computation parallelism. The cost paid is this adds an additional shuffle stage before
    * writing data to HDFS.
    *
    * @param dataFrame           The data the job would want to write to result table. Includes DS field.
    * @param dbTableName         Name of the table to write to. Including database(namespace) Example: "core_data.listings"
    * @param hiveTableProperties Properties of the table. Would be used to create the table or update metadata
    * @param checks              checks, which we need to make on the result before writing it out
    * @return name of the table, which were written as the result. Might be different from dbTableName
    *         in case of running in testing mode
    */
  def saveAsHiveTable[T](
                          dataFrame: DataFrame,
                          dbTableName: String,
                          hiveTableProperties: HiveTableProperties = SimpleHiveTableProperties(),
                          checks: Seq[Check] = Seq.empty
                        ): String = {
    metrics.measureTime("saveAsHiveTable", () => {
      preventBadPractices(dataFrame, hiveTableProperties)

      val tableName = EnvironmentSpecificLogic
        .getOutputTableName(dbTableName,
          environmentTableResolver,
          runConfig)

      logger.info(s"Name of the table to be written: $tableName")

      var df = dataFrame

      DropTable.dropTableIfNeeded(writeConfig = runConfig.writeConfig,
        hiveTableProperties= hiveTableProperties,
        sparkSession = dataFrame.sparkSession,
        tableName = dbTableName)
      metrics.measureTime("create_table", () => {
        TableCreation.createTableAndUpdateProperties(
          hiveTableProperties = hiveTableProperties,
          tableName = tableName,
          dataframe = df,
          writeConfig = runConfig.writeConfig
        )
      }, TimeUnit.SECONDS)

      logger.info(s"Created table $tableName")

      df = SchemaNormalisation.normaliseSchema(df, tableName, subsetIsOk = hiveTableProperties.fieldsSubsetIsAllowed)
      logger.info(s"Normalised the schema of the df before writing to $tableName")

      df = repartition(df, runConfig.writeConfig, hiveTableProperties)

      df = collectMetrics(tableName, df)
      metrics.measureTime("insert", () => {
        insert.insert(df, tableName = tableName, hiveTableProperties.partitionSpec, runConfig.ds, checks)
      }, TimeUnit.MINUTES)

      tableName

    }, TimeUnit.MINUTES)
  }

  def preventBadPractices(dataFrame: DataFrame, tableProperties: HiveTableProperties) = {
    val partitioningContainsDS = tableProperties.partitionSpec match {
      case None => false
      case Some(spec) => spec.columns.contains(DS_FIELD)
    }
    if (dataFrame.columns.contains(DS_FIELD) && !partitioningContainsDS) {
      throw new RuntimeException(s"You can not use '$DS_FIELD' field, but do not partition based on it")
    }
  }

}
