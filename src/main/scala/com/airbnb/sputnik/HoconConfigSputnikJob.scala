package com.airbnb.sputnik

import com.airbnb.sputnik.hive.HiveTableProperties
import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Try

object HoconConfigSputnikJob {

  def getMap(config: Config) = {
    import scala.collection.JavaConverters._
    config
      .entrySet()
      .asScala
      .map(entry => {
        entry.getKey.tail.init -> entry.getValue.unwrapped().toString
      })
      .toMap
  }

}

/**
  * Spark job which is configurable with hocon config in resources
  */
trait HoconConfigSputnikJob extends TypesafeConfigSputnikJob with GetResourceFile {

  /**
    * Path to a <a href="https://github.com/lightbend/config/blob/master/HOCON.md">HOCON</a> config
    * file in the resources with the config for this job. Allows developer to:
    * <p><ul>
    * <li> Specify {@link com.airbnb.sputnik.SparkJob#defaultStepSize defaultStepSize}
    * <li> Specify {@link com.airbnb.sputnik.SparkJob#oneDayAtATime oneDayAtATime}
    * <li> Extract {@link com.airbnb.sputnik.hive.HiveTableProperties HiveTableProperties} definition to config. @see {@link com.airbnb.sputnik.SparkJob#getHiveTablePropertiesFromConfig getHiveTablePropertiesFromConfig}.
    * <li> Define job specific spark configs
    * </ul><p>
    */
  val configPath: Option[String] = None

  lazy val localConfig = {
    def inJob() = {
      configPath match {
        case Some(path) => {
          log.info(s"Using path $path from job object for config. " +
            s"No path to config passed from console.")
          path
        }
        case None => {
          throw new RuntimeException("There is no path to config in either " +
            "job object or console parameters")
        }
      }
    }

    val pathToUse =
      if (Try({
        sputnikSession.runConfig.configPath
      }).isSuccess) {
        sputnikSession.runConfig.configPath.getOrElse({
          inJob()
        })
      } else {
        inJob()
      }
    val configString = getFileContent(pathToUse)
    ConfigFactory.parseString(configString).withFallback(ConfigFactory.load())
  }

  def config: Config = localConfig

  val TABLE_PROPERTIES_KEY = "table_properties"

  def getHiveTablePropertiesFromConfig(key: String): HiveTableProperties = {
    HiveTableProperties.fromConfig(tablePropertiesConfig(key))
  }

  protected def tablePropertiesConfig(key: String) = {
    config.getConfig(TABLE_PROPERTIES_KEY).getConfig(key)
  }
}
