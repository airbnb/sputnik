package com.airbnb.sputnik.hive

import com.airbnb.sputnik.DS_FIELD
import com.airbnb.sputnik.RunConfiguration.Environment.Environment
import com.airbnb.sputnik.RunConfiguration.{Environment, JobRunConfig}
import com.airbnb.sputnik.annotations.checks.NotEmptyCheck
import com.airbnb.sputnik.annotations.{FieldsFormatting, FieldsSubsetIsAllowed, PartitioningField, TableName}
import com.airbnb.sputnik.hive.HiveTableWriterTest._
import com.airbnb.sputnik.tools.Operation.Operation
import com.airbnb.sputnik.tools.beans.ParsedData
import com.airbnb.sputnik.tools.{BeansDataset, DefaultEnvironmentTableResolver, EnvironmentTableResolver}
import com.airbnb.sputnik.utils.{SparkBaseTest, TestRunConfig}
import com.google.common.base.CaseFormat
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

object HiveTableWriterTest {

  case class User(
                   id: String,
                   country: String
                 )

  @FieldsFormatting(CaseFormat.LOWER_UNDERSCORE)
  @TableName("default.simple_toSnakeCase")
  @FieldsSubsetIsAllowed(value = true)
  @NotEmptyCheck
  case class Visit(
                    userId: String,
                    url: String,
                    @PartitioningField ds: String
                  )

  val users = List(
    User("123", "US"),
    User("234", "China"),
    User("455", "Russia")
  )


  val visits_12 = List(
    Visit("123", "airbnb/lux", "2018-12-12"),
    Visit("123", "airbnb/plus", "2018-12-12"),
    Visit("234", "airbnb/plus", "2018-12-12")
  )

  val visits_13 = List(
    Visit("123", "airbnb/plus", "2018-12-13"),
    Visit("234", "airbnb/lux", "2018-12-13")
  )

  val visits = visits_12 ++ visits_13
}

@RunWith(classOf[JUnitRunner])
class HiveTableWriterTest extends SparkBaseTest {

  test("test checks in annotations") {
    val sparkSession = ss
    import sparkSession.implicits._
    val df = spark.emptyDataset[Visit]
    implicit val runConfig = TestRunConfig.testJobRunConfig
    val hiveTableWriter = new HiveTableWriter(
      runConfig, insert = DefaultInsert
    )

    val caught = intercept[RuntimeException] {
      hiveTableWriter.saveDatasetAsHiveTable(
        dataset = df,
        itemClass = classOf[Visit]
      )
    }
    assert(caught.getMessage === "Dataframe is empty")

  }

  test("toSnakeCase") {
    val table = s"default.simple_toSnakeCase"
    val sparkSession = ss
    import sparkSession.implicits._
    val df = visits.toDF()
    val ds = df.select("url", "userId", "ds").as[Visit]
    implicit val runConfig = TestRunConfig.testJobRunConfig
    val hiveTableWriter = new HiveTableWriter(
      runConfig, insert = DefaultInsert
    )
    hiveTableWriter.saveDatasetAsHiveTable(
      dataset = ds,
      itemClass = classOf[Visit]
    )
    assert(ss.table(table).columns === Array("user_id", "url", "ds"))
  }

  test("when dataframe does not match result table") {
    val table = s"default.match_df"
    drop(table)
    ss.sql(
      s"""
         |create table $table (
         |userId string
         |) PARTITIONED BY (ds string)
      """.stripMargin)
    val sparkSession = ss
    import sparkSession.implicits._
    val df = visits.toDS()
    implicit val runConfig = TestRunConfig.testJobRunConfig
    val hiveTableWriter = new HiveTableWriter(
      runConfig, insert = DefaultInsert
    )
    var hiveTableProperties = SimpleHiveTableProperties()
    assertThrows[RuntimeException] {
      hiveTableWriter.saveAsHiveTable(df.toDF(), table, hiveTableProperties = hiveTableProperties)
    }
    hiveTableProperties = hiveTableProperties.copy(fieldsSubsetIsAllowed = true)
    hiveTableWriter.saveAsHiveTable(df.toDF(), table, hiveTableProperties = hiveTableProperties)
    hiveTableWriter.saveDatasetAsHiveTable(df, itemClass = classOf[Visit])
  }

  test("Assert, that we can write simple non partitioned dataframe to a hive table") {
    val sparkSession = ss
    import sparkSession.implicits._
    val table = s"default.simple_table_write"
    drop(table)
    val df = users.toDF()
    implicit var runConfig = JobRunConfig(writeConfig = TestRunConfig.testJobRunConfig.writeConfig.copy(dropResultTables = Some(false)))
    var hiveTableWriter = new HiveTableWriter(
      runConfig, insert = DefaultInsert
    )
    hiveTableWriter.saveAsHiveTable(df, table, hiveTableProperties = SimpleHiveTableProperties(partitionSpec = None))
    assertDataFrameAlmostEquals(df, ss.table(table))
    val moreUsersData = Array(
      User("454", "Canada"),
      User("458", "Mexico")
    )
    val moreUsers = ss.sparkContext
      .parallelize(moreUsersData)
      .toDF()
    hiveTableWriter.saveAsHiveTable(moreUsers, table, hiveTableProperties = SimpleHiveTableProperties(partitionSpec = None))
    assertDataFrameAlmostEquals(df.union(moreUsers), ss.table(table))
    runConfig = runConfig.copy(writeConfig = runConfig.writeConfig.copy(dropResultTables = Some(true)))
    hiveTableWriter = new HiveTableWriter(
      runConfig, insert = DefaultInsert
    )
    hiveTableWriter.saveAsHiveTable(moreUsers, table, hiveTableProperties = SimpleHiveTableProperties(partitionSpec = None))
    assertDataFrameAlmostEquals(moreUsers, ss.table(table))
    assert(ss.sql(
      s"""
         |select id, country from $table order by id
      """.stripMargin).as[User].collect() === moreUsersData)
  }

  test("Assert, that we can write partitioned dataframe to a hive table") {
    val sparkSession = ss
    import sparkSession.implicits._
    val table = s"default.simple_table_write"
    drop(table)
    val df = visits.toDF()
    implicit var runConfig = TestRunConfig.testJobRunConfig
    val hiveTableWriter = new HiveTableWriter(
      runConfig, insert = DefaultInsert
    )
    hiveTableWriter.saveAsHiveTable(
      dataFrame = df,
      dbTableName = table
    )
    assertDataFrameAlmostEquals(df, ss.table(table))
    val overwritingData = List(
      Visit("455", "airbnb/experience", "2018-12-13")
    )
    val overwriting = overwritingData.toDF()
    hiveTableWriter.saveAsHiveTable(
      dataFrame = overwriting,
      dbTableName = table
    )

    assert(ss.sql(
      s"""
         |select userId, url, ds from $table order by userId, url
       """.stripMargin).as[Visit].collect() === visits_12 ++ overwritingData)
  }

  test("Assert, that we can write multi column partitioned dataframe to a hive table") {
    val sparkSession = ss
    import sparkSession.implicits._
    val table = s"default.table_write_partitioned_multiple_columns"
    drop(table)
    val df = visits.toDF()
    implicit val runConfig = TestRunConfig.testJobRunConfig
    val hiveTableProperties  = SimpleHiveTableProperties(partitionSpec = Some(HivePartitionSpec(List("url", DS_FIELD))))
    var hiveTableWriter = new HiveTableWriter(
      runConfig, insert = DefaultInsert
    )
    hiveTableWriter.saveAsHiveTable(
      dataFrame = df,
      dbTableName = table,
      hiveTableProperties =hiveTableProperties
    )
    assertDataFrameAlmostEquals(df, ss.table(table))
    val overwritingData = List(
      Visit("455", "airbnb/plus", "2018-12-12")
    )
    val overwriting = overwritingData.toDF()
    hiveTableWriter.saveAsHiveTable(
      dataFrame = overwriting,
      dbTableName = table,
      hiveTableProperties = hiveTableProperties
    )

    assert(ss.sql(
      s"""
         |select userId, url, ds from $table order by userId, url
       """.stripMargin).as[Visit].collect() === (visits_13 ++ overwritingData ++ List(Visit("123", "airbnb/lux", "2018-12-12"))).sortBy(v => v.userId + v.ds))
    // test that we drop the table
    hiveTableWriter = new HiveTableWriter(
      runConfig.copy(writeConfig = runConfig.writeConfig.copy(dropResultTables = Some(true))),
      insert = DefaultInsert
    )
    hiveTableWriter.saveAsHiveTable(
      dataFrame = overwriting,
      dbTableName = table,
      hiveTableProperties = hiveTableProperties
    )
    assert(ss.sql(
      s"""
         |select userId, url, ds from $table order by userId, url
       """.stripMargin).as[Visit].collect() === overwritingData.sortBy(v => v.userId + v.ds))

  }

  object TestEnvironmentTableResolver extends EnvironmentTableResolver {

    def resolve(dbTableName: FullTableName,
                environment: Environment,
                operation: Operation
               ): FullTableName = {
      dbTableName + "_test_test"
    }

  }

  test("Testing the support of different environments mode") {
    val sparkSession = ss
    import sparkSession.implicits._
    val table = s"default.test_env"
    drop(table)
    val df = users.toDF()
    implicit val runConfig = JobRunConfig(writeConfig = TestRunConfig
      .testJobRunConfig
      .writeConfig
      .copy(writeEnvironment = Environment.DEV))

    implicit var environmentTableResolver: EnvironmentTableResolver = DefaultEnvironmentTableResolver
    var hiveTableWriter = new HiveTableWriter(
      runConfig, insert = DefaultInsert,
      environmentTableResolver = environmentTableResolver
    )
    val hiveTableProperties = SimpleHiveTableProperties(partitionSpec = None)
    hiveTableWriter.saveAsHiveTable(df, table, hiveTableProperties = hiveTableProperties)
    assertDataFrameAlmostEquals(df, ss.table(s"default.test_env_dev"))

    environmentTableResolver = TestEnvironmentTableResolver
    hiveTableWriter = new HiveTableWriter(
      runConfig, insert = DefaultInsert,
      environmentTableResolver = environmentTableResolver
    )
    hiveTableWriter.saveAsHiveTable(df, table, hiveTableProperties = hiveTableProperties)
    assertDataFrameAlmostEquals(df, ss.table(s"default.test_env_test_test"))
  }

  test("Testing dataset support") {
    drop("default.someTableParsedData")
    val input = BeansDataset.getDataset(ss)
    implicit val runConfig = TestRunConfig.testJobRunConfig
    val hiveTableWriter = new HiveTableWriter(
      runConfig, insert = DefaultInsert
    )
    hiveTableWriter.saveDatasetAsHiveTable(
      input,
      classOf[ParsedData]
    )
    val result = ss
      .table("default.someTableParsedData")
      .head()
    assert(result.getAs[String]("field_one") === "someValue1")
    assert(result.getAs[String]("json_field_one") === "{\"innerFieldOne\":\"hi\"}")
  }
}
