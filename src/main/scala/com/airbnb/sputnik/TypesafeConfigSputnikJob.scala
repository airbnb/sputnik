package com.airbnb.sputnik

import com.airbnb.sputnik.HoconConfigSputnikJob.getMap
import com.airbnb.sputnik.hive.HiveTableProperties
import com.typesafe.config.Config

trait TypesafeConfigSputnikJob extends SputnikJob {

  /**
    * typesafe which was created from  {@link com.airbnb.sputnik.SparkJob#configPath configPath}
    * See {@link com.airbnb.sputnik.SparkJob#configPath configPath}
    */
  def config: Config

  /**
    * Allows you to get HiveTableProperties from {@link com.airbnb.sputnik.SparkJob#config config}
    *
    * @param key by which the HiveTableProperties is specified in "table_properties"
    */
  def getHiveTablePropertiesFromConfig(key: String): HiveTableProperties

  override lazy val sparkConfigs: Map[String, String] = getMap(config.getConfig("sparkConfigs"))

  override lazy val defaultStepSize: Option[Int] = {
    val key = "defaultStepSize"
    config.hasPath(key) match {
      case true => Some(config.getInt(key))
      case false => None
    }
  }


}
