package com.airbnb.sputnik.hive

import com.airbnb.sputnik.RunConfiguration.WriteConfig
import com.airbnb.sputnik.hive.TableCreation.logger
import org.apache.spark.sql.SparkSession

import scala.util.Try

object DropTable {

  def dropTable(ss: SparkSession, tableName: String): Unit = {
    Try(ss.sql(s"truncate table $tableName"))
    ss.sql(s"drop table if exists $tableName")
  }

  def dropTableIfNeeded(writeConfig: WriteConfig,
                hiveTableProperties: HiveTableProperties,
                sparkSession: SparkSession,
                tableName: String
               ) = {
    if ((writeConfig.dropResultTables.isDefined && writeConfig.dropResultTables.get) ||
      (writeConfig.dropResultTables.isEmpty && hiveTableProperties.partitionSpec.isEmpty)) {
      logger.info(s"Dropping table $tableName")
      dropTable(sparkSession, tableName)
    }
  }
}
