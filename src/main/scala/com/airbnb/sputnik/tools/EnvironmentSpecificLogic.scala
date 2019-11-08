package com.airbnb.sputnik.tools

import com.airbnb.sputnik.RunConfiguration.Environment.Environment
import com.airbnb.sputnik.RunConfiguration.{Environment, JobRunConfig}

object Operation extends Enumeration {
  type Operation = Value
  val Read, Write = Value
}
import Operation._

trait EnvironmentTableResolver extends Serializable {

  type FullTableName = String

  def resolve(dbTableName: FullTableName,
              environment: Environment,
              operation: Operation
             ): FullTableName

}

object DefaultEnvironmentTableResolver extends EnvironmentTableResolver {

  override def resolve(dbTableName: String,
                       environment: Environment,
                       operation: Operation
                      ): String = {
    val suffix = environment match {
      case Environment.DEV => "_dev"
      case Environment.STAGE => "_staging"
      case Environment.PROD => ""
    }
    dbTableName + suffix
  }
}

object EnvironmentSpecificLogic {

  def getOutputTableName(
                          dbTableName: String,
                          tableResolver: EnvironmentTableResolver,
                          runConfig: JobRunConfig
                        ) = {
    var newTableName = tableResolver.resolve(dbTableName, runConfig.writeConfig.writeEnvironment, Operation.Write)
    newTableName = if (runConfig.writeConfig.addUserNameToTable) {
      newTableName + "_" + System.getProperty("user.name")
    } else {
      newTableName
    }
    if (runConfig.writeConfig.backfill) {
      val db = newTableName.split("\\.")(0)
      val tableName = newTableName.split("\\.")(1)
      db + "." + "backfill_in_progress__" + tableName
    } else {
      newTableName
    }
  }

  def getInputTableName(
                         dbTableName: String,
                         tableResolver: EnvironmentTableResolver,
                         runConfig: JobRunConfig
                       ) = {
    tableResolver.resolve(dbTableName, runConfig.readEnvironment, Operation.Read)
  }

}
