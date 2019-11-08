package com.airbnb.sputnik

import com.airbnb.sputnik.RunConfiguration.JobRunConfig
import com.airbnb.sputnik.SputnikJob.SputnikSession
import com.typesafe.config.{Config, ConfigFactory}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HoconConfigSputnikJobTest extends FunSuite {

  object HoconConfigSputnikTestJob extends SputnikJob with HoconConfigSputnikJob {

    override val configPath = Some("hocon_config_sputnik_job_test.conf")

    def run() = {

    }

  }

  object HoconConfigSputnikTestJob2 extends SputnikJob with HoconConfigSputnikJob {

    override val configPath = None

    def run() = {

    }

  }

  test("Test getting config right from console parameters") {
    HoconConfigSputnikTestJob2.setup(
      sputnikSession = SputnikSession(
        JobRunConfig().copy(configPath = Some("hocon_config_sputnik_job_test_2.conf"))
        , null, null))
    val sparkConfigs = HoconConfigSputnikTestJob2.sparkConfigs
    assert(sparkConfigs.get("spark.executor.memory").get === "20g") // from job config
  }

  test("Test getting config right") {
    HoconConfigSputnikTestJob.setup(
      sputnikSession = SputnikSession(
        JobRunConfig(), null, null))
    val sparkConfigs = HoconConfigSputnikTestJob.sparkConfigs
    assert(sparkConfigs.get("spark.executor.memory").get === "8g") // from job config
    assert(sparkConfigs.get("hive.exec.dynamic.partition.mode").get === "nonstrict") // from reference.conf
  }

  test("Test getHiveTablePropertiesFromConfig") {
    val sputnikJob = new HoconConfigSputnikJob {

      override val configPath = Some("")

      override lazy val config: Config =
        ConfigFactory.parseString(
          """
            | table_properties {
            |  user_data_new_users {
            |    description: "First occurrence of a user",
            |    defaultWriteParallelism: 20,
            |    tableRetention {
            |      days: -1,
            |      reason: "Build reports based on that table"
            |    },
            |    fieldComments {
            |      userId: "Id of a user",
            |      ds: "First day we've seen this user"
            |    }
            |  }
            |}
          """.stripMargin)

      override def run(): Unit = {}
    }

    assert(sputnikJob.getHiveTablePropertiesFromConfig("user_data_new_users").description === "First occurrence of a user")

  }

}
