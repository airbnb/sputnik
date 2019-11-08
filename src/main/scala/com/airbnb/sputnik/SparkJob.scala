package com.airbnb.sputnik

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class SparkJob {


  def main(args: Array[String]): Unit = {
    if (args.length < 1) { // checking ds parameter
      println("Please specify ds to process the data")
      System.exit(-1)
    }
    val day = "'" + args(0) + "'"
    val jobName = "my-job-" + day
    val sparkConfig = new SparkConf().setAppName(jobName) // create spark config
    val session = SparkSession // build spark session
      .builder()
      .config(sparkConfig)
      .enableHiveSupport()
      .getOrCreate()

    // real logic
    val inputDF = session
      .read
      .table("input_data")
    val result = inputDF
      .filter(inputDF.col("ds").equalTo(day))
      .select("col1", "col2")

    result
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("output_data")
  }
}



