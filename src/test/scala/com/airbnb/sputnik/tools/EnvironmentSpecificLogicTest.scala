package com.airbnb.sputnik.tools

import com.airbnb.sputnik.RunConfiguration.{Environment, JobRunConfig, WriteConfig}
import com.airbnb.sputnik.utils.TestRunConfig
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EnvironmentSpecificLogicTest extends FunSuite {

  test("DefaultEnvironmentTableResolver") {
    assert(DefaultEnvironmentTableResolver.resolve("a.a", Environment.DEV, Operation.Read) === "a.a_dev")
    assert(DefaultEnvironmentTableResolver.resolve("a.a", Environment.PROD, Operation.Read) === "a.a")
    assert(DefaultEnvironmentTableResolver.resolve("a.a", Environment.STAGE, Operation.Read) === "a.a_staging")
  }

  test("EnvironmentSpecificLogic.getInputTableName") {
    val dbTableName = "default.some_table"
    val tableResolver = DefaultEnvironmentTableResolver
    var runConfig = TestRunConfig.testJobRunConfig.copy(readEnvironment = Environment.DEV)
    assert(EnvironmentSpecificLogic.getInputTableName(dbTableName, tableResolver, runConfig) === "default.some_table_dev")
    runConfig = TestRunConfig.testJobRunConfig.copy(readEnvironment = Environment.STAGE)
    assert(EnvironmentSpecificLogic.getInputTableName(dbTableName, tableResolver, runConfig) === "default.some_table_staging")
    runConfig = TestRunConfig.testJobRunConfig.copy(readEnvironment = Environment.PROD)
    assert(EnvironmentSpecificLogic.getInputTableName(dbTableName, tableResolver, runConfig) === "default.some_table")
  }

  test("EnvironmentSpecificLogic.getOutputTableName") {
    val dbTableName = "default.some_table"
    val tableResolver = DefaultEnvironmentTableResolver
    var runConfig = JobRunConfig(writeConfig = WriteConfig(writeEnvironment = Environment.DEV))
    assert(EnvironmentSpecificLogic.getOutputTableName(dbTableName, tableResolver, runConfig) === "default.some_table_dev")
    runConfig = JobRunConfig(writeConfig = WriteConfig(writeEnvironment = Environment.STAGE))
    assert(EnvironmentSpecificLogic.getOutputTableName(dbTableName, tableResolver, runConfig) === "default.some_table_staging")
    runConfig = JobRunConfig(writeConfig = WriteConfig(writeEnvironment = Environment.PROD))
    assert(EnvironmentSpecificLogic.getOutputTableName(dbTableName, tableResolver, runConfig) === "default.some_table")
    runConfig = JobRunConfig(writeConfig = WriteConfig(writeEnvironment = Environment.PROD, addUserNameToTable = true))
    val resultTableName = EnvironmentSpecificLogic.getOutputTableName(dbTableName, tableResolver, runConfig)
    assert(resultTableName.startsWith("default.some_table_"))
    assert(resultTableName.length > "default.some_table_".length)
  }

}
