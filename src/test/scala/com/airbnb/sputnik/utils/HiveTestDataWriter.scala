package com.airbnb.sputnik.utils

import com.airbnb.sputnik.Metrics
import com.airbnb.sputnik.RunConfiguration.{Environment, JobRunConfig, WriteConfig}
import com.airbnb.sputnik.hive.HiveUtils._
import com.airbnb.sputnik.hive.{HivePartitionSpec, HiveTableWriter, Insert, SimpleHiveTableProperties}
import org.apache.spark.sql.Dataset

object HiveTestDataWriter {

  /**
    * Writes data to hive so job can work with this data in unit tests.
    * Simplified version of {@link com.airbnb.sputnik.hive.HiveTableWriter#saveAsHiveTable saveAsHiveTable} for testing purposes.
    * Omits some parameters, since they are not needed to
    *
    * @param dataset       The data the job would want to write to result table. Includes DS field.
    * @param dbTableName   Name of the table to write to. Including database(namespace) Example: "core_data.listings"
    * @param partitionSpec Specifies how table should be partitioned
    */
  def writeInputDataForTesting[T](
                                   dataset: Dataset[T],
                                   dbTableName: String,
                                   itemClass: Option[Class[T]] = None,
                                   partitionSpec: Option[HivePartitionSpec] = None
                                 ): Unit = {
    val runConfig = JobRunConfig(writeConfig = WriteConfig(
      repartition = false,
      writeEnvironment = Environment.PROD,
      dropResultTables = Some(true),
      addUserNameToTable = false
    ))
    if (dbTableName.contains('.')) {
      dataset.sparkSession.sql(s"Create database if not exists ${getDatabase(dbTableName)}")
    }
    val hiveTableWriter = new HiveTableWriter(runConfig = runConfig, insert = new Insert(metrics = new Metrics, simplifiedInsertLogic = true))

    itemClass match {
      case None => {
        hiveTableWriter.saveAsHiveTable(
          dataFrame = dataset.toDF(),
          dbTableName = dbTableName,
          hiveTableProperties = SimpleHiveTableProperties(partitionSpec = partitionSpec)

        )
      }
      case Some(cl) => {
        hiveTableWriter.saveDatasetAsHiveTable(
          dataset = dataset,
          itemClass = cl,
          dbTableName = Some(dbTableName)
        )
      }
    }

  }

}
