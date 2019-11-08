package com.airbnb.sputnik.hive

import java.time.LocalDate
import java.util.concurrent.TimeUnit

import com.airbnb.sputnik.Metrics.{Tag, _}
import com.airbnb.sputnik.RunConfiguration.Range
import com.airbnb.sputnik.checks.Check
import com.airbnb.sputnik.hive.HivePartitionSpec.DS_PARTITIONING
import com.airbnb.sputnik.hive.HiveUtils._
import com.airbnb.sputnik.tools.DateConverter
import com.airbnb.sputnik.{Logging, Metrics, RunConfiguration}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Random, Success, Try}

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

class Insert(metrics: Metrics, simplifiedInsertLogic: Boolean = true) extends Logging {

  def insert(df: DataFrame,
             tableName: String,
             partitionSpec: Option[HivePartitionSpec] = None,
             ds: Either[LocalDate, Range] = Left(LocalDate.MIN),
             checks: Seq[Check] = Seq.empty
            ) = {
    val tableNameTag = tagTableName(tableName)
    if (partitionSpec.isDefined && partitionSpec.equals(DS_PARTITIONING)) {
      //[TODO] When we insert data into non-empty partitions, it will throw exceptions.
      // It's a bug from in file system.
      dropDsPartitions(ds, df, tableName)
    }
    if (partitionSpec.isDefined && partitionSpec.equals(DS_PARTITIONING) && !simplifiedInsertLogic) {
      val dataFrame = df.cache()
      val dsColumn = dataFrame.col("ds")

      lazy val resultDates = {
        metrics.measureTime("result_dates", () => {
          dataFrame
            .select(dsColumn)
            .distinct()
            .collect()
            .map(_.getAs[String](0))
        }, TimeUnit.SECONDS)
      }

      metrics.count("result_dates", resultDates.size, tableNameTag)

      logger.info(s"Result dates for table $tableName are ${resultDates.mkString(",")}")
      if (resultDates.size == 1) {
        simpleInsert(tableName, partitionSpec, dataFrame, checks)
      } else if (resultDates.size > 1) {
        processMultipleDaysInsert(
          dataFrame,
          tableName,
          checks,
          resultDates)
      } else {
        runChecks(checks, dataFrame, tableName)
      }

      val daysProcessing = RunConfiguration
        .daysToProcess(ds)
        .map(DateConverter.dateToString(_))

      logger.info(s"Days processing for table $tableName: ${daysProcessing.mkString(",")}")

      val datesToFill = daysProcessing.filter(dateToFill => !resultDates.contains(dateToFill))
      metrics.count("dates_to_fill", datesToFill.size, tableNameTag)
      createEmptyPartitions(datesToFill, dataFrame, tableName)
      dataFrame.unpersist()
    } else {
      simpleInsert(tableName, partitionSpec, df, checks)
    }

  }

  def dropDsPartitions(ds: Either[LocalDate, Range], dataFrame: DataFrame, tableName: String): Unit = {
    val daysProcessing = RunConfiguration
      .daysToProcess(ds)
      .map(DateConverter.dateToString(_))
    logger.info(s"Dropping partitions for table $tableName: ${daysProcessing.mkString(",")}")
    daysProcessing.foreach(date => {
      try {
        dataFrame.sparkSession.sql(s"ALTER TABLE $tableName DROP IF EXISTS PARTITION (ds='$date')")
      } catch {
        case _: NoSuchTableException => {
          logger.warn(s"table $tableName doesn't exist")
        }
      }
    })
  }

  def createEmptyPartitions(datesToFill: List[String], dataFrame: DataFrame, tableName: String) = {
    if (!datesToFill.isEmpty) {
      val existingPartitions = HiveUtils.getExistingPartitions(dataFrame.sparkSession, tableName)
      val datesToFillFiltered = datesToFill.filter(dateToFill => !existingPartitions.contains(dateToFill))
      logger.info(s"Days for which create empty partitions for table $tableName: ${datesToFill.mkString(",")}")
      datesToFillFiltered.foreach(date => {
        dataFrame.sparkSession.sql(s"ALTER TABLE $tableName ADD PARTITION (ds='$date')")
      })
    }
  }

  def simpleInsert(tableName: String,
                   partitionSpec: Option[HivePartitionSpec],
                   dataFrame: DataFrame,
                   checks: Seq[Check] = Seq.empty
                  ): Unit = {
    val df = if (!checks.isEmpty) {
      dataFrame.cache()
    } else {
      dataFrame
    }
    runChecks(checks, df, tableName)
    metrics.measureTime("simple_insert", () => {
      val justTableName = getTableName(tableName)
      val tmp_view = s"${justTableName}_dataframe_${tableRandomSuffix()}_tmp"
      df.createOrReplaceTempView(tmp_view)
      val (partitionPartStatement, overwriteStatement) = partitionSpec match {
        case Some(spec) => (s" PARTITION(${spec}) ", "OVERWRITE")
        case None => ("", "INTO")
      }

      val insertStatement =
        s"""
           |INSERT $overwriteStatement TABLE ${tableName}
           |$partitionPartStatement
           |SELECT *
           |FROM $tmp_view
       """.stripMargin
      logger.info(s"Inserting data to table $tableName with " +
        s"next insert statement:\n $insertStatement")
      df.sparkSession.sql(insertStatement)
      logger.info("Data inserted")
      df.sparkSession.catalog.dropTempView(tmp_view)
    }, TimeUnit.SECONDS)
    df.unpersist()
  }

  def runChecks(checks: Seq[Check], dataFrame: DataFrame, tableName: String) = {
    if (!checks.isEmpty) {
      metrics.count("checks_count", checks.size, tagTableName(tableName))
      metrics.measureTime("run_all_checks", () => {
        logger.info("Checks are not empty. Would do checks before inserting.")
        checks.foreach(check => {
          logger.info(s"""Working with check "${check.checkDescription}" """)
          metrics.measureTime("running_check", () => {
            check.check(dataFrame) match {
              case Some(errorMessage) => {
                val exceptionMessage = if (dataFrame.count() > 0) {
                  val storedResultMessage = Try {
                    val tmpDatabase = ConfigFactory.load().getString("tmp_database")
                    val fullTempTableName = tmpDatabase + ".sputnik_failed_check_persisted_result_" + tableName.replace('.', '_') + "_" + Math.abs(Random.nextLong())
                    dataFrame.write.saveAsTable(fullTempTableName)
                    fullTempTableName
                  } match {
                    case Success(tableName) => s"Stored the result to $tableName"
                    case Failure(exception) => {
                      logger.error(exception)
                      s"Could not store the result. Reason: ${exception.getMessage}"
                    }
                  }
                  s"$errorMessage; $storedResultMessage "
                } else {
                  errorMessage
                }
                throw new RuntimeException(exceptionMessage)
              }
              case None => logger.info(s"${check.checkDescription} has passed")
            }
          }, TimeUnit.SECONDS, Seq(Tag("check", check.checkDescription)) ++ tagTableName(tableName))
          logger.info(s"Done check ${check.checkDescription}")
        })
      }, TimeUnit.SECONDS, tagTableName(tableName))
    }
  }

  def processMultipleDaysInsert(dataFrame: DataFrame,
                                tableName: String,
                                checks: Seq[Check],
                                resultDates: Array[String]
                               ) = {
    case class DateRunResult(date: String, result: Try[Unit])
    val failures = resultDates
      .par
      .map(resultDate => {
        logger.info(s"Working with ${resultDate}")
        val oneDateDF = dataFrame.where(dataFrame.col("ds").equalTo(resultDate))
        val insertResult = Try {
          simpleInsert(tableName, DS_PARTITIONING, oneDateDF, checks)
        }
        insertResult match {
          case Failure(exception) => {
            logger.error(s"Failed insert for date $resultDate " +
              s"to table $tableName", exception)
          }
          case Success(_) => {
            logger.info(s"Successful insert for date $resultDate " +
              s"to table $tableName")
          }
        }
        DateRunResult(resultDate, insertResult)
      })
      .filter(_.result.isFailure)
      .toList
    metrics.count("failed_dates_on_insert", failures.size, tagTableName(tableName))
    failures.foreach(dateResult => {
      dateResult.result match {
        case Failure(exception) => {
          throw new RuntimeException(s"Failed to insert to ${dateResult.date}", exception)
        }
        case Success(_) =>
      }
    })
  }

}

