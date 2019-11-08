package com.airbnb.sputnik.hive

import com.airbnb.sputnik.enums.TableFileFormat.PARQUET;
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HiveTablePropertiesTest extends FunSuite {
  test("Test from config") {
    val configString =
      """
        | description: "Comment A",
        | defaultWriteParallelism: 10,
        | otherMetaInformation {
        |   owner: "me"
        | },
        | fieldComments {
        |   field1: "hi",
        |   field2: "be"
        | },
        | partitionSpec: ["ds"],
        | fileFormat: "PARQUET"
      """.stripMargin

    val config = ConfigFactory.parseString(configString)
    val hiveTableProperties = HiveTableProperties.fromConfig(config)
    assert(hiveTableProperties.description === "Comment A")
    assert(hiveTableProperties.defaultWriteParallelism === Some(10))
    assert(hiveTableProperties.otherMetaInformation === Map("owner" -> "me"))
    assert(hiveTableProperties.fieldComments === Map("field1" -> "hi", "field2" -> "be"))
    assert(hiveTableProperties.partitionSpec === Some(HivePartitionSpec(List("ds"))))
    assert(hiveTableProperties.fileFormat === PARQUET)
  }

}
