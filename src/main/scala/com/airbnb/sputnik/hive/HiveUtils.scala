package com.airbnb.sputnik.hive

import org.apache.spark.sql.SparkSession

import scala.util.Random

object HiveUtils {

  def tableRandomSuffix() = Math.abs(Random.nextLong())

  def getExistingPartitions(sparkSession: SparkSession, table: String) = {
    sparkSession
      .sql(s"show partitions $table")
      .collect()
      .map(_.getAs[String](0))
      .map(part => {
        if (part.contains("=")) {
          part.split("=")(1)
        } else {
          part
        }
      })
  }

  def getDatabase(fullTableName: String) = {
    fullTableName.split("\\.")(0)
  }

  def getTableName(fullTableName: String) = {
    fullTableName.split("\\.")(1)
  }

}
