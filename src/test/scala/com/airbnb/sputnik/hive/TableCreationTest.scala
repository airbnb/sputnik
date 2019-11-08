package com.airbnb.sputnik.hive

import com.airbnb.sputnik.utils.{SomeTestingData, SparkBaseTest, TestRunConfig}
import org.apache.spark.sql.AnalysisException
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class TableCreationTest extends SparkBaseTest with SomeTestingData {

  test("Assert that table being created") {

    drop("default.test_table_create")

    assertThrows[AnalysisException] {
      ss.table("default.test_table_create").count()
    }
    TableCreation.createTableAndUpdateProperties(
      tableName = "default.test_table_create",
      dataframe = simpleRowDF,
      writeConfig = TestRunConfig.testJobRunConfig.writeConfig,
      hiveTableProperties = SimpleHiveTableProperties(partitionSpec = None)
    )
    assertSchemaEqual(simpleRowDF.schema, ss.table("default.test_table_create").schema)
  }

  test("Assert, that we partition") {
    drop("default.test_partition")

    TableCreation.createTableAndUpdateProperties(
      tableName = "default.test_partition",
      dataframe = simpleRowDF,
      writeConfig = TestRunConfig.testJobRunConfig.writeConfig,
      hiveTableProperties = SimpleHiveTableProperties(partitionSpec = Some(HivePartitionSpec(List("field1"))))
    )
    assertSchemaEqual(
      simpleRowDF.schema,
      ss.table("default.test_partition").schema
    )
    assert(ss.sql("show create table default.test_partition")
      .collect()
      .head
      .getString(0)
      .contains("PARTITIONED BY (`field1") === true)
  }

  test("Assert, that we partition on multiple columns") {
    drop("default.test_partition_mult")

    TableCreation.createTableAndUpdateProperties(
      tableName = "default.test_partition_mult",
      dataframe = multipleFieldsPartitioningRowDF,
      writeConfig = TestRunConfig.testJobRunConfig.writeConfig,
      hiveTableProperties = SimpleHiveTableProperties(partitionSpec = Some(HivePartitionSpec(List("partitionedfield2", "partitionedfield1"))))
    )
    assertSchemaEqual(
      multipleFieldsPartitioningRowDF.schema,
      ss.table("default.test_partition_mult").schema
    )
    assert(ss.sql("show create table default.test_partition_mult")
      .collect()
      .head
      .getString(0)
      .contains("PARTITIONED BY (`partitionedfield1` STRING, `partitionedfield2`") === true)
  }


  test("Test adding comments to the fields") {
    drop("default.test_comments")
    val hiveTableProperties = SimpleHiveTableProperties(
      fieldComments = Map(
        "field1" -> "Some comment about field 1",
        "field2" -> "Some comment about field 2"
      ),
      partitionSpec = None
    )
    TableCreation.createTableAndUpdateProperties(
      tableName = "default.test_comments",
      dataframe = simpleRowDF,
      writeConfig = TestRunConfig.testJobRunConfig.writeConfig,
      hiveTableProperties = hiveTableProperties
    )
    val tableDescription = ss.sql("show create table default.test_comments")
      .collect()
      .map(_.getAs[String](0))
      .mkString(" ")

    assert(tableDescription.contains("Some comment about field 1"))
    assert(tableDescription.contains("Some comment about field 2"))
  }

}
