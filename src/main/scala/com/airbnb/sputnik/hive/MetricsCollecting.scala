package com.airbnb.sputnik.hive

import com.airbnb.sputnik.{Logging, Metrics}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.util.LongAccumulator

trait MetricsCollecting extends Logging {

  val sputnikMetrics: Metrics

  val accumulators = scala.collection.mutable.ArrayBuffer.empty[LongAccumulator]

  def collectMetrics(tableName: String, df: DataFrame): DataFrame = {
    if (ConfigFactory.load().getBoolean("count_rows")) {
      val accumulator = df.sparkSession.sparkContext.longAccumulator(tableName)
      accumulators += accumulator
      df.map(row => {
        accumulator.add(1)
        row
      })(RowEncoder.apply(df.schema))
    } else {
      df
    }
  }

  def close() = {
    logger.info(s"Closing metricsCollecting. Accumulators size is ${accumulators.size}")
    accumulators.foreach(accumulator => {
      sputnikMetrics.count("row_count", accumulator.sum, Metrics.tagTableName(accumulator.name.get))
    })
  }

}
