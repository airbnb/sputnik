package com.airbnb.sputnik.utils

import java.io.File
import java.time.LocalDate

import com.airbnb.sputnik.RunConfiguration.{JobRunConfig, Range, SputnikRunnerConfig}
import com.airbnb.sputnik.SputnikJob.SputnikSession
import com.airbnb.sputnik.hive.{HivePartitionSpec, HiveTableReader}
import com.airbnb.sputnik.runner.SputnikJobRunner
import com.airbnb.sputnik.tools.{DateConverter, DefaultEnvironmentTableResolver, EnvironmentTableResolver}
import com.airbnb.sputnik.{HoconConfigSputnikJob, Metrics, SputnikJob}
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.RandomStringUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

class SputnikJobBaseTest extends SparkBaseTest {

  def hiveTableReader(implicit runConfig: JobRunConfig = JobRunConfig(),
                      environmentTableResolver: EnvironmentTableResolver = DefaultEnvironmentTableResolver
                     ) = {
    val sputnikSession = new SputnikSession(
      runConfig = runConfig,
      ss = ss,
      new Metrics())

    new HiveTableReader(sputnikSession, environmentTableResolver = environmentTableResolver)
  }

  def runJob(
              sputnikJob: SputnikJob,
              runConfig: JobRunConfig = JobRunConfig(),
              jobRunner: SputnikJobRunner = SputnikJobRunner
            ) = {
    val job = jobRunner.createSputnikJob(sputnikJob.getClass.getCanonicalName)
    val sputnikSession = SputnikSession(runConfig, ss, new Metrics)
    if (sputnikJob.isInstanceOf[HoconConfigSputnikJob]) {
      sputnikJob.setup(SputnikSession(runConfig, null, null))
    }
    jobRunner
      .run(
        sputnikJob = job,
        sputnikRunnerConfig = new SputnikRunnerConfig,
        sputnikSession = sputnikSession,
        exitAfter = false
      )
  }


  private def hiveTableTesting(resourcePath: String,
                               tableName: String,
                               partitionSpec: Option[HivePartitionSpec],
                               data: String => DataFrame): Unit = {
    val tempFile = new File("/tmp/" + RandomStringUtils.randomAlphanumeric(10).toUpperCase())
    try {
      val stream = getClass
        .getResource(resourcePath)
        .openStream()
      FileUtils.copyInputStreamToFile(stream, tempFile)
      val df = data(tempFile.getCanonicalPath)
      HiveTestDataWriter.writeInputDataForTesting(df,
        tableName,
        partitionSpec = partitionSpec)
    } finally {
      tempFile.delete()
    }
  }

  def javaRunConfig(ds: String): JobRunConfig = {
    val date: Either[LocalDate, Range] = Left(DateConverter.stringToDate(ds))
    JobRunConfig(ds = date)
  }

  def hiveTableFromJson(resourcePath: String,
                        tableName: String,
                        partitionSpec: Option[HivePartitionSpec] = HivePartitionSpec.DS_PARTITIONING) = {
    hiveTableTesting(resourcePath, tableName, partitionSpec, getJson)
  }

  def getJson(path: String) = {
    ss.read.option("multiLine", true).json(path)
  }

  def getCSV(targetSchema: Option[StructType] = None)(path: String) = {
    val data = ss
      .read
      .option("header", "true")
      .csv(path)
    targetSchema match {
      case Some(schema) => castSchema(data, schema)
      case None => data
    }
  }

  def hiveTableFromCSV(resourcePath: String,
                       tableName: String,
                       targetSchema: Option[StructType] = None,
                       partitionSpec: Option[HivePartitionSpec] = HivePartitionSpec.DS_PARTITIONING
                      ) = {
    hiveTableTesting(resourcePath, tableName, partitionSpec, getCSV(targetSchema))
  }

  def castSchema[T](inputDF: DataFrame, targetSchema: StructType) = {
    var resultDF = inputDF

    val newTypesMap = targetSchema.fields.map(field => {
      field.name -> field.dataType.simpleString
    }).toMap

    inputDF.schema.fields.map(_.name).foreach(fieldName => {
      val tempColumnName = fieldName + "temp"
      resultDF = resultDF.withColumnRenamed(fieldName, tempColumnName)
      resultDF = resultDF
        .withColumn(fieldName, resultDF.col(tempColumnName)
          .cast(newTypesMap.get(fieldName) match {
            case Some(type_1) => type_1
            case None => throw new RuntimeException(s"Could not find type for $fieldName in" +
              s" ${newTypesMap.keySet.mkString(",")}")
          }))
      resultDF = resultDF.drop(tempColumnName)
    })
    resultDF
  }

}
