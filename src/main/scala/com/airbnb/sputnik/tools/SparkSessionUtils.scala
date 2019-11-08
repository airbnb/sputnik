package com.airbnb.sputnik.tools

import com.airbnb.sputnik.Logging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionUtils extends Logging {

  var sparkConf: SparkConf = _

  private lazy val sparkSession: SparkSession = {
    logger.info(s"Creating spark session with config = ${sparkConf.toDebugString}")
    val session = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    if (sparkConf.contains("spark.driver.memory")) {
      val configuredMemory = sparkConf.get("spark.driver.memory").toLowerCase
      if (configuredMemory.endsWith("g")) {
        val configuredDriverMemoryG = configuredMemory.init.toInt
        val realDriverMemoryGRuntime = Runtime.getRuntime.maxMemory() * 1.0 / 1024 / 1024 / 1024
        logger.info(s"Real driver memory in G is $realDriverMemoryGRuntime")
        if (realDriverMemoryGRuntime < configuredDriverMemoryG * 0.25) {
          throw new RuntimeException(s"Spark config spark.driver.memory wasn't applied to Spark Session." +
            s"Current value is $realDriverMemoryGRuntime. Driver configs should be specified before the app starts: either " +
            s"as parameter to spark submit(--conf) or in spark-defaults.conf")
        }
      }
    }

    session
  }

  def getSparkSession(
                       sparkConf: SparkConf
                     ): SparkSession = {
    this.sparkConf = sparkConf
    sparkSession
  }

}
