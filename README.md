# Sputnik
Framework for writing daily Spark batch jobs, which use Hive as primary storage. 

> [Sputnik](https://en.wikipedia.org/wiki/Sputnik_1) was the first artificial Earth satellite. The Soviet Union launched it into an elliptical low Earth orbit on 4 October 1957, orbiting for three weeks before its batteries died, then silently for two more months before falling back into the atmosphere.
![sputnik schema](/images/Sputnik_asm.jpg)
## Motivation

[Apache Spark](https://spark.apache.org/) is general purpose execution engine which provides a lot of power 
and flexibility. It allows a data engineer to read from different sources in different manners. Daily batch 
jobs, which read from Hive and write to Hive usually do not require such flexibility. On the opposite some 
restrictive code is required to implement some good practices of data engineering. Example of that might be
 a code, which reads partitioned data for current date and writes to this date's partition in result table.
  Backfilling of the result table is something what Spark does not do and require user to define.
Sputnik is a framework which helps follow good practices for data engineering of daily batch jobs working with
 data in Hive. It contains most of the code, which data engineer would need to write and operate their job. 
 This includes, but not limited to:
* Reading data from source table filtered by date or date range specified in console for this job run. 
* Backfilling data
* Running checks on result data before inserting it into the result table
* Writing data to testing version of the table, when job runs in testing or staging mode
* Utils to easily write unit test for a job
* Creating schema for result table from annotated case class or java bean
* Updating result table meta-information
* Creating result table through Hive “create statement” to improve compatibility

## Job logic vs run logic

Because of Spark's flexibility data engineer needs to specify not only the domain specific logic for processing 
the data, but orchestration logic for reading and writing the data. Sputnik tries to keep these two sets of 
logic separate and asks data engineer to define job logic, but keep run logic to Sputnik. 

Example of job specific logic:

* job multiply every value from input table by 2
* job specifies that source table is “core.numbers” and result table is “core.numbers_multiplied”
* job specifies that result table is partitioned by date
* job specifies retention of result tables
* job specifies checks for data, before it's written

Example of run specific logic: 

* we run job for one date, so we retrieve input data only for that date
* job tries to write to table, which does not exists, so we need to create the table
* job runs in staging mode, so all result tables are created with “_testing” 


![sputnik schema](/images/sputnik_schema.jpeg)

## Code example

User(data engineer) needs to extend `SparkJob`, define run method and use `HiveTableReader` and `HiveTableWriter` to read and write data.

```scala

object VisitsAggregationJob extends SparkJob {

  def run(): Unit = {

    val input = hiveTableReader.getDataframe("user_data.visits")

    val result = input
      .groupBy("userId", "ds")
      .agg(countDistinct("url").as("distinctUrlCount"))

    hiveTableWriter.saveAsHiveTable(
      dataFrame = result,
      dbTableName = "user_data.visits_aggregation",
      partitionSpec = HivePartitionSpec.DS_PARTITIONING
    )
  }

}
```

When user would run this job in console, they can specify the date to process and other parameters for this run:
```bash
spark-submit --class VisitsAggregationJob --ds 2019-01-10 \
 --dropResultTables true \
 --writeEnv PROD \
 --logLevel ERROR
```

# User Guide

## Writing spark job

### VisitsAggregationJob

Let's look at simple job. Imagine that you have input table `user_data.visits` with information about  how users visited Airbnb site. 
You need to generate table `user_data.visits_aggregation`, where url's would be aggregated by `userId` and `ds`. So the schemas for the tables are: 

```scala
  user_data.visits
    -- userId
    -- url
    -- ds
  user_data.visits_aggregation
    -- userId
    -- distinct_url_count
    -- ds
```

Like majority of other tables, table `user_data.visits` is partitioned by `ds`. 
So, we need the result table to be partitioned by `ds` as well. Let's look at the code of the job: 

```scala
object VisitsAggregationJob extends SputnikJob with AutoModeSputnikJob {

  override val startDate: LocalDate = DateConverter.stringToDate("2015-01-01")
  override val outputTables: Seq[String] = List("user_data.visits_aggregation")

  def run(): Unit = {

    val inputTable = "user_data.visits"

    val spark = sputnikSession.ss
    import spark.implicits._

    val input: DataFrame = hiveTableReader
      .getDataframe(tableName = inputTable)

    val result = input
      .groupBy("userId", DS_FIELD)
      .agg(countDistinct("url").as("distinctUrlCount"))
      .as[VisitAggregated]

    hiveTableWriter.saveDatasetAsHiveTable(
      dataset = result,
      itemClass = classOf[VisitAggregated]
    )
  }

}
```

The job extends class `SputnikJob`, which is base class for all spark jobs written in Sputnik.
Block which does the core logic of the job is 
```scala
val result = input
      .groupBy("userId", DS_FIELD)
      .agg(countDistinct("url").as("distinctUrlCount"))
      .as[VisitAggregated]
```
It has not special magic, just pure spark. 
Line: 
```scala
.as[VisitAggregated]
```
means, that we convert result `DataFrame` to `Dataset` of `VisitAggregated`.
 You can skip this step if you prefer to work with `DataFrame`s. 
 Everything in Sputnik builds around the `DataFrame` API, but user can use `Dataset`s for convenience. 
 We have block 
```code
val spark = ss
import spark.implicits._
```
to be able to convert `DataFrame` to `Dataset`, but it's just spark thing and has nothing to do with Sputnik.
 Now let's look more closely at blocks of code where Sputnik provides functionality additional to Spark. 
 We read from input table with `HiveTableReader`:
 ```code
 val input: DataFrame = hiveTableReader.getDataframe(inputTable)
 ```
Although the table is partitioned, we do not specify that or specify a filter to take data from the certain 
partition. The reason for not specifying partition logic here is that choosing the partition to take data
 from is “run specific” logic, not “job specific”. When we would run this job for one day, Sputnik would 
 filter out the data only for that day. When we run the job for batch of days, `HiveTableReader` would get 
 the data for batch of days. 
After we've done aggregation we need to write the result:
```scala
    hiveTableWriter.saveDatasetAsHiveTable(
      dataset = result,
      itemClass = classOf[VisitAggregated]
    )
```
When we write the result with Sputnik we specify where to write to and what the result table looks like. 
We do not specify how we want to write data. We specify all meta information about the table in annotations 
to case class of result `Dataset`. Or we use `HiveTableProperties` if we use `Dataframe` API(would be later in examples). 
```scala
  @TableName("user_data.visits_aggregation")
  @TableDescription("Counting distinct visited url for a user")
  @FieldsFormatting(CaseFormat.LOWER_UNDERSCORE)
  @TableFormat(TableFileFormat.RCFILE)
  case class VisitAggregated(
                              userId: String@Comment("Id of a user"),
                              distinctUrlCount: Long,
                              @PartitioningField ds: String
                            )
```
Annotation `TableName` specify the name of table with data for this case class. 
`TableDescription` specify description of the table in meta information in Hive. 
We annotate class with `FieldsFormatting`, when we want to convert field names before writing it to Hive. 
For example schema of the hive table for this class would have next field names: `user_id`, `distinct_count`, `ds`.
 Motivation for that is the fact that naming conventions in Java/Scala and Hive are different: in Java you use 
 camelCase to name field and in Hive you use snake_case to name columns. `PartitioningField` allows use to specify fields
 on which data would be partitioned. In 99% of the cases, when we partition, we want to partition by `ds` field. 
 
Logic, which `HiveTableWriter` does: 

* It changes the result table name, if we run job in staging mode
* It creates table with “CREATE TABLE” hive statement, if table does not yet exist. 
Default spark writer to hive does not do that and it creates problems with compatibility with other systems.
* It updates table meta information
* It drops the result table and creates a new one if we specify such behavior with a command-line flag, so we can easily iterate in developer mode 
* It changes schema of dataframe according to result schema of table, so even if we change the logic and it would result in 
change in order of the fields we would write correctly. 
* It repartitions and tries to reduce number of result files on disk
* It does checks on result, before inserting it. 

So, please use `HiveTableWriter`. 

### UsersToCountryJob

Let's move to the next job to explore more of Sputnik functionality. 
Job `UsersToCountryJob` joins tables to get user -> country mapping. Input is

```scala
user_data.users
    -- userId
    -- areaCode

user_data.area_codes
    -- areaCode
    -- country
```
to get 
```code
user_data.users_to_country
    -- userId
    -- country
```
The job code: 

```scala

object NullCheck extends SQLCheck {

  def sql(temporaryTableName: String): String = {
    s"""
       | SELECT
       | IF(country is null OR country = "", false, true) as countryExist,
       | IF(userId is null OR userId = "", false, true) as userExists
       | from $temporaryTableName
     """.stripMargin
  }

}

object UsersToCountryJob extends SputnikJob {

  def run(): Unit = {
    val userTable = "user_data.users"
    val areaCodesTable = "user_data.area_codes"
    val outputTable = "user_data.users_to_country"
    val users = hiveTableReader.getDataframe(userTable)
    val areaCodes = hiveTableReader.getDataframe(areaCodesTable)

    val joined = users.joinWith(areaCodes,
      users.col("areaCode")
        .equalTo(areaCodes.col("areaCode")), "inner")

    val result = joined.select(
      joined.col("_1.userId").as("userId"),
      joined.col("_2.country").as("country")
    )

    val tableProperties = SimpleHiveTableProperties(
      description = "Temporary table for user to country mapping",
      partitionSpec = None
    )

    hiveTableWriter.saveAsHiveTable(
      dataFrame = result,
      dbTableName = outputTable,
      hiveTableProperties = tableProperties,
      checks = List(NullCheck, NotEmptyCheck)
    )
  }
}

```
Getting input data is done with `HiveTableReader` like before. This time the input tables 
are not partitioned and `HiveTableReader` understands that and takes all the records from input tables:
```scala
  val users = hiveTableReader.getDataframe(userTable)
  val areaCodes = hiveTableReader.getDataframe(areaCodesTable)
```
Logic of a join itself is 
```scala
 val joined = users.joinWith(areaCodes,
      users.col("areaCode")
        .equalTo(areaCodes.col("areaCode")), "inner")

 val result = joined.select(
      joined.col("_1.userId").as("userId"),
      joined.col("_2.country").as("country")
    )
```
Writing the result: 
```scala
    val tableProperties = SimpleHiveTableProperties(
      description = "Temporary table for user to country mapping"
    )

    hiveTableWriter.saveAsHiveTable(
      dataFrame = result,
      dbTableName = outputTable,
      hiveTableProperties = tableProperties,
      partitionSpec = None,
      checks = List(NullCheck, NotEmptyCheck)
    )
```

This time we have a result, which is `DataFrame`, not `Dataset`. 
It means, that we need to pass all information to `HiveTableWriter`,
 which before we specified through annotations. And we do that by passing 
 instance of `HiveTableProperties`. Actually when Sputnik works with result 
 dataset it takes all info from annotations and convert if to `HiveTableProperties` 
 itself. You can look at Dataset API as a layer on top of DataFrame API.
 
 We specify checks we want to 
 do on the result data before writing it.
 ```scala
 checks = List(NullCheck, NotEmptyCheck)
``` 

Check is a test on result data of the job. If test passes - the data gets written to result table.
 If test fails -  exception is thrown. So check is pretty simple interface:
 ```scala

package com.airbnb.sputnik.checks

import org.apache.spark.sql.DataFrame

trait Check {

    type ErrorMessage = String
 
    val checkDescription = this.getClass.getCanonicalName
 
    def check(df: DataFrame): Option[ErrorMessage]
}
```

User needs to implement check method, which returns `ErrorMessage`, when checks fail and return `None`,
 when everything fine. Example of simplest check is `NotEmptyCheck`:
 ```scala
package com.airbnb.sputnik.checks

import com.airbnb.sputnik.Logging
import org.apache.spark.sql.DataFrame

object NotEmptyCheck extends Check with Logging {

  override val checkDescription = "Checking that dataframe is not empty"
  val errorMessage = "Dataframe is empty"

  def check(df: DataFrame): Option[ErrorMessage] = {
    val recordsCount = df.count()
    (recordsCount == 0) match {
      case true => Some(errorMessage)
      case false => {
        logger.info(s"Result contains $recordsCount records")
        None
      }
    }
  }
}
```
This check verifies, that result is not empty. SQL is a good language to define checks. 
Example is `NullCheck` in `UsersToCountryJob`:
```scala
object NullCheck extends SQLCheck {

  def sql(temporaryTableName: String): String = {
    s"""
       | SELECT
       | IF(country is null OR country = "", false, true) as countryExist,
       | IF(userId is null OR userId = "", false, true) as userExists
       | from $temporaryTableName
     """.stripMargin
  }

}
```
Parameter `temporaryTableName` is name of temporary table, which you can query with the data 
we want to write. Select statement should return Integer, Long, Boolean or String values. 
It can return just one record or a record per every row in temporaryTableName. true, “true”, 
“True”, 1, 6 is passed. false, “false”, 0 is failed. “2018-05-05” isn't allowed and would 
throw the `Exception`.

### Dataset vs Dataframe 

Sputnik provide Dataframe API as well as Dataset API, but it's hard to mix these two in one job. 
You can not simultaneously define table for `HiveTableWriter` with annotations and HiveTableProperties.
If you want `FieldFormatting` functionality you can get it only from Dataset API, because you need this functionality only 
if you use case classes to define your schema. And if you use case classes you should use Dataset API from Sputnik. If you 
use Dataframe API it means, that you don't want to have case classes for your schemas and you do not need to transform
 the field anme. 

### NewUsersJob

Jobs we looked at so far is relatively simple in terms of relying on previous runs of this job. 
You can run VisitsAggregationJob on any range of dates in any order, because dates are not connected
between each other. But pipeline often requires self join, especially when we deal with “first seen” pattern. 

Imagine that we have a table `user_data.visits_aggregation` which we created before as input and we
 need for every user to have a ds, when we've seen him the first time. So input would be 
 ```code
 user_data.visits_aggregation
     -- userId
     -- distinct_url_count
     -- ds
 ```

And output would be:

```code
user_data.new_users
    -- userId
    -- ds
```

The reason, that we can not simply start our job with 

```scala
val users = hiveTableReader.getDataframe("user_data.new_users")
```

is that this table doesn't exist before the first run of our job. So we need to first check if 
the table exists and based on this knowledge have different logic for different cases. 

```scala
object NewUsersJob extends SputnikJob with HoconConfigSputnikJob {

  override val configPath = Some("example/new_users_job.conf")

  val outputTable = "user_data.new_users"
  val inputTable = "user_data.visits_aggregation"

  def run(): Unit = {

    val spark = sputnikSession.ss
    import spark.implicits._

    val result = if (sputnikSession.ss.catalog.tableExists(outputTable)) {

      val alreadySeen =
        sputnikSession.ss
          .table(outputTable)
          .as[NewUser]
          .alias("already_seen")

      hiveTableReader.getDataframe(
        inputTable
      ).as[VisitAggregated]
        .groupBy("userId")
        .agg(
          min(col(DS_FIELD)).as(DS_FIELD)
        )
        .as[NewUser]
        .alias("processing")
        .join(
          alreadySeen,
          col("processing.userId").equalTo(col("already_seen.userId")),
          "left"
        )
        .where(col("already_seen.userId").isNull)
        .select(
          col("processing.userId").as("userId"),
          col(s"processing.$DS_FIELD").as(DS_FIELD)
        )
        .as[NewUser]

    } else {
      val input = hiveTableReader.getDataframe(
        tableName = inputTable,
        dateBoundsOffset = Some(FixedLowerBound(LocalDate.of(2016,1,1)))
      ).as[VisitAggregated]
      input
        .groupBy("userId")
        .agg(
          min(col(DS_FIELD)).as(DS_FIELD)
        )
        .as[NewUser]
    }

    val tableProperties = getHiveTablePropertiesFromConfig("user_data_new_users")

    hiveTableWriter.saveAsHiveTable(
      dataFrame = result.toDF(),
      dbTableName = outputTable,
      hiveTableProperties = tableProperties
    )
  }
}
```

The statement, where we check, that table already exists is 

```scala
sputnikSession.ss.catalog.tableExists(outputTable)
```
That's a good example of user going directly to SparkSession for some functionality, which
 Sputnik does not provide. Sputnik does not prevent user from working with Spark core API, 
 it just provides some wrappers for typical pipeline development operations. Right now Sputnik 
 does not have ambition to define all operations around working with self-join pipelines, so 
 it's up to a user to find a best solution for his problem. This Job provides an example of one approach. 
 
 #### Using config file

You can extract some of your logic into config file in resources. Config format is 
[HOCON](https://github.com/lightbend/config/blob/master/HOCON.md). You extend HoconConfigSparkJob 
and specify path to this config in 

```scala
val configPath: Option[String]
```

In `HoconConfigSputnikJob` the config is read into 

```scala
 def config: Config
```

on start of your job. You can retrieve values from this config and you can 
use `defaultStepSize` from `TypesafeConfigSputnikJob` as an example: 

```scala
  override lazy val defaultStepSize: Option[Int] = {
    val key = "defaultStepSize"
    config.hasPath(key) match {
      case true => Some(config.getInt(key))
      case false => None
    }
  }
```

If you have some spark configurations specific for your job you should put it in config as well. Example:

```json
sparkConfigs {
  "spark.executor.memory": "8g"
}
```

Most common use case for using config - when you do not use annotations extracting `HiveTableProperties` 
definition of the table into the config, so code wouldn't have too many constants:

```json
table_properties {
  user_data_new_users {
    description: "First occurrence of a user",
    defaultWriteParallelism: 20,
    tableRetention {
      days: -1,
      reason: "Build reports based on that table"
    },
    fieldComments {
      userId: "Id of a user",
      ds: "First day we've seen this user"
    },
    partitionSpec: ["ds"]
  }
}

sparkConfigs {
  "spark.executor.memory": "8g"
}
```

You can load it from the code to pass to `HiveTableWriter`:

```scala
    val tableProperties = getHiveTablePropertiesFromConfig("user_data_new_users")

    hiveTableWriter.saveAsHiveTable(
      dataFrame = result.toDF(),
      dbTableName = outputTable,
      hiveTableProperties = tableProperties
    )
```

### VisitsPerCountryJob

Imagine, that you need every day to have some aggregate for the past 7 days. 
It's an annoying requirement, isn't it? We've explained so far, that code:

```scala
hiveTableReader.getDataframe("user_data.visits_aggregation")
```

would filter out only dates we are processing right now from the input. 
But we need for every day previous 7 days as well. That's why Sputnik provides 
parameter `dateBoundsOffset`

```scala
hiveTableReader.getDataframe("user_data.visits_aggregation", dateBoundsOffset = Some(DateBoundsOffset(lowerOffset = -7)))
```

By default we take data from ds which we are processing, but `DateOffset` allows you to change 
date bounds of data you are reading.
Lower bound is the bound from which start to take data.
Upper bound is the bound till which we take the data starting from lower bound.
In case of just processing ds - ds value is lower bound and upper bound.
If we would like to process ds and date before that like we do
in `DS_AND_DATE_BEFORE_THAT`, than lower bound would be ds-1 and upper bound would be ds.

Examples:
* DateBoundsOffset(0, 0) - data for ds
* DateBoundsOffset(-1, -1) - data for day before ds
* DateBoundsOffset(-1, 0) - data for day before ds and ds
* DateBoundsOffset(-10, 0) - data for 10 days before ds and ds
* DateBoundsOffset(-10, -10) - data for day, which was 10 days before ds

We have a job VisitsPerCountry, which calculate some meaningless aggregates for the 
last 7 days per `country` per `ds`. To be able to do that it needs to join 
`user_data.users_to_country` and `user_data.visits_aggregation`, group by dates and do
 aggregation. The code of the job is:
 
 ```scala
object VisitsNonZeroCheck extends Check {

  override val checkDescription = "Checking that we have only records with more than 0 visits"

  def check(df: DataFrame): Option[ErrorMessage] = {
    val spark = df.sparkSession
    import spark.implicits._
    df
      .as[CountryStats]
      .filter(countryStats => {
        (countryStats.distinct_url_number == 0) || (countryStats == 0)
      })
      .take(1)
      .headOption match {
      case Some(badRecord) => {
        Some(s"There is at least one record " +
          s"with distinct_url_number or countryStats equals to 0: ${badRecord}")
      }
      case None => None
    }
  }

}

object VisitsPerCountryJob extends SputnikJob {

  override def run(): Unit = {

    val spark = sputnikSession.ss
    import spark.implicits._

    val visitsAggregated = hiveTableReader
      .getDataset(
        itemClass = classOf[Schemas.VisitAggregated],
        dateBoundsOffset = Some(DateBoundsOffset(lowerOffset = -7))
      ).as[Schemas.VisitAggregated]

    val daysProcessing: Dataset[Date] = daysToProcessAsDS()

    val groupedByDay =
      createSevenDaysGroups(daysProcessing, visitsAggregated)
        .alias("grouped")

    val userToCountry = hiveTableReader
      .getDataframe("user_data.users_to_country")
      .as[UserToCountry]
      .alias("country")

    val result = groupedByDay.join(userToCountry,
      col("grouped.userId").equalTo(col("country.userId")),
      "inner"
    )
      .select(
        col("grouped.userId").as("userId"),
        col(s"grouped.$DS_FIELD").as(DS_FIELD),
        col("country.country").as("country"),
        col("grouped.distinctUrlCount").as("distinct_url")
      )
      .groupBy(col("country"), col(DS_FIELD))
      .agg(
        sum(col("distinct_url")).as("distinct_url_number"),
        approx_count_distinct(col("userId")).as("user_count")
      )
      .as[CountryStats]

    val tableProperties = SimpleHiveTableProperties(
      description = "Information for number of distinct users " +
        " and urls visited for the last 7 days for a given country"
    )

    hiveTableWriter.saveAsHiveTable(
      dataFrame = result.toDF(),
      dbTableName = "user_data.country_stats",
      hiveTableProperties = tableProperties,
      checks = List(NotEmptyCheck, VisitsNonZeroCheck)
    )
  }

  def createSevenDaysGroups(daysProcessing: Dataset[Date],
                            visitsAgregated: Dataset[VisitAggregated]
                           ) = {
    val daysToProcess = daysProcessing.alias("daysToProcess")
    val visits_aggregation = visitsAgregated.alias("visits_aggregation")

    visits_aggregation
      .join(
        daysToProcess,
        datediff(
          col(s"daysToProcess.$DS_FIELD").cast("date"),
          col(s"visits_aggregation.$DS_FIELD").cast("date"))
          .between(0, 6),
        "inner")
      .select(
        col(s"daysToProcess.$DS_FIELD").as(DS_FIELD),
        col(s"visits_aggregation.userId").as("userId"),
        col(s"visits_aggregation.distinctUrlCount").as("distinctUrlCount")
      )
  }
}
```
Interesting part of the code is 
```scala
val daysProcessing: Dataset[Date] = daysToProcessAsDS()
```
It calls `daysToProcessAsDS`, which is method of `SputnikJobUtils`. It works with 
`JobRunConfig` to get the list of dates this job run is processing. DataFrame of 
these dates simplifies a code around takings last 7 days for every day we are 
processing. We use this dataframe in 

```scala
  def createSevenDaysGroups(daysProcessing: Dataset[Date],
                            visitsAgregated: Dataset[VisitAggregated]
                           ) = {
    val daysToProcess = daysProcessing.alias("daysToProcess")
    val visits_aggregation = visitsAgregated.alias("visits_aggregation")

    visits_aggregation
      .join(
        daysToProcess,
        datediff(
          col(s"daysToProcess.$DS_FIELD").cast("date"),
          col(s"visits_aggregation.$DS_FIELD").cast("date"))
          .between(0, 6),
        "inner")
      .select(
        col(s"daysToProcess.$DS_FIELD").as(DS_FIELD),
        col(s"visits_aggregation.userId").as("userId"),
        col(s"visits_aggregation.distinctUrlCount").as("distinctUrlCount")
      )
  }
```

The logic is the same as it was in “seen first” - we do not define how user should 
solve the problem of sliding window, but we suggest one solution and provide utils 
for this solution. 

### NewUsersCountDesktopJob (multiple columns partitioning)

Sometimes data in Hive table consists of data for very different domains or from different
datasource. In such case we might want to partition not only by `ds`, but by some other field. 
This gives us ability to add data to table for one date from different jobs and run these jobs independently. 
Advantages of that might be 
* We've added some new product and need to backfill the data for that product without rewriting data for other products.
* We have same type of data from different datasources with significantly different landing times. We don't want for processing of one datasource
to wait on processing of other datasource. 

Examples of using such functionality presented in `NewUsersCountDesktopJob` and `NewUsersCountMobileJob`:

```scala
  @TableName("user_data.new_user_count")
  @NotEmptyCheck
  case class NewUserCount(
                           user_count: Long,
                           @PartitioningField @NotNull platform: String,
                           @PartitioningField ds: String
                         )
```

```scala
object NewUsersCountDesktopJob extends SputnikJob
  with HoconConfigSputnikJob
  with SQLSputnikJob {

  override val configPath = Some("example/new_users_count_job.conf")

  def run(): Unit = {
    hiveTableReader
      .getDataset(classOf[NewUser], Some(NewUsersJob.outputTable))
      .createOrReplaceTempView("new_users")

    val spark = sputnikSession.ss
    import spark.implicits._

    val newUsersCounts = executeResourceSQLFile("example/new_users_count_desktop.sql")
      .as[NewUserCount]

    hiveTableWriter.saveDatasetAsHiveTable(
      newUsersCounts,
      classOf[NewUserCount]
    )
  }
}

```

example/new_users_count_desktop.conf:

```sqlite-sql
select count(distinct(userId)) as user_count, 'desktop' as platform,  ds
 from new_users
 group by ds
```

NewUsersCountMobileJob.scala:

```scala
object CountGreaterZero extends SQLCheck {
  def sql(temporaryTableName: String): String = sqlFromResourceFile("example/count_greater_zero_check.sql", temporaryTableName)
}

object NewUsersCountMobileJob extends SputnikJob with SQLSputnikJob {
  def run(): Unit = {
    hiveTableReader
      .getDataset(classOf[MobileUsersRowData])
      .createOrReplaceTempView("mobile_row_data")

    val spark = sputnikSession.ss
    import spark.implicits._

    val newUsersMobileCounts = executeSQLQuery(
      """
        | select count(distinct(userId)) as user_count, 'mobile' as platform,  ds
        | from mobile_row_data
        | where event="createdAccount"
        | group by ds
      """.stripMargin)
      .as[NewUserCount]

    hiveTableWriter.saveDatasetAsHiveTable(
      newUsersMobileCounts,
      classOf[NewUserCount],
      checks = Seq(CountGreaterZero)
    )
  }
}
```

The only significant change from previous jobs is partitioning schema. There is set of functionality,
which wouldn't work with tables partitioned by multiple columns. `AutoMode` wouldn't work correctly, because it checks 
`ds` partitions and some date can exist for one secondary partition, but do not exist for the other one. Insert logic
would support only simplifiedMode(without applying checks on every day separately).

## Testing the Job

To test a job, you need to create test class, which extends `SputnikJobBaseTest`. 
In this class you can write [FunSuite](http://www.scalatest.org/getting_started_with_fun_suite) tests, 
which are using utils from `SputnikJobBaseTest`. Example might be testing of `UsersToCountryJob`
with `UsersToCountryJobTest`:

```scala
@RunWith(classOf[JUnitRunner])
class UsersToCountryJobTest extends SputnikJobBaseTest {

  test("Test UsersToCountryJob") {
    ss.sql("create database if not exists user_data")

    import spark.implicits._
    drop("user_data.users_to_country")
    drop("user_data.users")
    drop("user_data.area_codes")
    val users = Data.users.toDF()

    HiveTestDataWriter.writeInputDataForTesting(
      dataset = users,
      dbTableName = "user_data.users"
    )

    hiveTableFromJson(resourcePath = "/area_codes.json",
      tableName = "user_data.area_codes",
      partitionSpec = None
    )

    runJob(UsersToCountryJob)

    val result = ss.table("user_data.users_to_country")
    val resultExpected = Data.usersToCountries.toDF

    assertDataFrameAlmostEquals(result, resultExpected)
  }
}
```
How you generally test a job:

You create databases, with which your job works.

```scala
ss.sql("create database if not exists user_data")
```

You drop all tables, with which your job works to insure, 
that no data being left there from running of other tests.

```scala
drop("user_data.users_to_country")
drop("user_data.users")
drop("user_data.area_codes")
```

You create dataframes with input data

```scala
    val users = Data.users.toDF()
```
```scala
  val users = List(
    User("1", "94502"),
    User("2", "67457"),
    User("3", "54765"),
    User("4", "57687"),
    User("5", "34567"),
    User("6", "34567")
  )
```
You write input data to Hive tables with special method of `HiveTableWriter`. 
This special method takes less parameters than `saveAsHiveTable`, because it knows
 the values for the rest of parameters. 

```scala
 HiveTestDataWriter.writeInputDataForTesting(
      dataset = users,
      dbTableName = "user_data.users"
    )

```

You run your job

```scala
runJob(UsersToCountryJob)
```

You get data from result table and expected result:

```scala
    val result = ss.table("user_data.users_to_country")
    val resultExpected = Data.usersToCountries.toDF
```

You verify, that the result and expected result are equal.

```scala
assertDataFrameAlmostEquals(result, resultExpected)
```

Method `assertDataFrameAlmostEquals` is an extension of [Holden's](https://github.com/holdenk/spark-testing-base) 
`assertDataFrameEquals`. It does not require the records to be in the same order and fields to be in the same order. 
Order often wouldn't be the same, because table partitioning influence the order. 
Class `DataFrameSuiteBase` is partially reimplemented and partially copy-pasted from Holden's `DataFrameSuiteBase`.

The source of SparkSession for both `SparkBaseTest` and `SputnikJobRunner` is the same - 
spark session singleton in `SparkSessionUtils`. This SparkSession is shared between all
tests during the run. We do that, because it saves overhead on creating local spark
context and closing it between tests.

`runJob` actually runs the job with help of SparkJobRunner, so we emulate the behavior in the real run, 
including creating instance with a reflection.
  
You do not need to test `HiveTableWriter` or `HiveTableReader`, because it already was 
tested by developers of Sputnik. 

`runJob` accepts `JobRunConfig`, so you can test your code in different run configurations.

We can verify, that checks work correctly by messing with input data and expecting the exception

```scala
val users = Data.users.map(user => user.copy(userId = "")).toDF()
```

```scala
assertThrows[RuntimeException](runJob(UsersToCountryJob))
```
### Testing data in resources

You can store your testing data in resource files and load this data to Hive tables with 
`hiveTableFromJson` or `hiveTableFromCSV`. In previous test we looked at:
```scala
    hiveTableFromJson(resourcePath = "/area_codes.json",
      tableName = "user_data.area_codes",
      partitionSpec = None
    )
```

We taking data from file `area_codes.json` from resource directory of the project and put data
to Hive table `user_data.area_codes` without partitioning. `area_codes.json` file looks like this:

```json
[
  {
    "areaCode": "94502",
    "country": "Russia"
  },
  {
    "areaCode": "67457",
    "country": "Russia"
  },
  {
    "areaCode": "54765",
    "country": "China"
  },
  {
    "areaCode": "57687",
    "country": "USA"
  },
  {
    "areaCode": "34567",
    "country": "USA"
  }
]
```
You can take data from CSV as well:
```scala
  hiveTableFromCSV(resourcePath = "/visits.csv",
      tableName = "user_data.visits"
    )
```
## Running the job
   
When we want to run job for one day we just pass

```bash
--ds 2018-01-06 --writeEnv PROD
```   

`--ds` flag specifies, which day we are processing. `--writeEnv` flag defines to which enviroment we are writing 
the data(PROD/STAGE/DEV). By setting its value to DEV we wouldn't overwrite production data by accident. By default
it's DEV.
   
if we need to run job for a range of days, we pass: 

```bash
--startDate 2018-01-07 --endDate 2018-01-09
```

Our job would run for a range of dates (2018-01-07, 2018-01-08, 2018-01-09) and data for these dates would be processed.
Data would be overwritten if data already exists for these days.

If we want table to be dropped before the data would be written we pass

```bash
--dropResultTables true
```

We might need to drop table, because we redefine the schema and need to backfill data. The table would 
be recreated from schema of dataframe, so no user manual operation with Hive table creation is necessary. 
All non-partitioned tables to which user writes are dropped before the data is written by 
default, but you can overwrite it. It works this way, because non-partitioned tables usually 
are temporary tables for a single dag run. 

When we try to run the job with big range of dates, let's say 2 years, the job might fail because of heavy
 volume of data. Solution might be just to break down the date range into small ranges and run them sequentially. 
 To avoid performing this operation manually Sputnik has a flag 
   
```bash
--stepSize 50
```

This flag specifies, how many dates in one run we would try to process.

We can specify default `stepSize` for a job, because we know about heavy load of data while we
 develop the code of the job. That's why we can specify default step size in the job code. 

```scala
lazy val defaultStepSize: Option[Int] = None
```
Sometimes we have a logic in the job, which needs the result 
from previous runs of this job and approach we've taken in the `NewUsersJob` is
 not possible. It means, that we would need to run one day at a time only. Since 
 this the logic, which job specific we specify it in the job:

```scala
lazy val defaultStepSize: Option[Int] = Some(1)
```

In this case we still would be able to use range run, but sputnik would run only on day at a time. 

You can run your job in “auto” mode.

```bash
--autoMode
```

Auto mode would try to find all ds, which do not exist in result tables and run the job for these 
partitions. We've seen an example for this functionality in `VisitsAggregationJob`. Auto mode is 
external to all logic I've mentioned before, so step size logic  would work with auto mode as well. 
For auto mode to be able to understand which days to process, it needs info about result tables 
names and start date for result data. To specify that, you need to extend trait `AutoModeSparkJob` 
for your job. You need to define output tables:

```scala
val outputTables: Seq[String]
```

and you need to define `startDate` from `SputnikJob`:

```scala
val startDate: LocalDate 
```

or define startDate in HOCON config

```json
startDate = "2018-01-01"
```
