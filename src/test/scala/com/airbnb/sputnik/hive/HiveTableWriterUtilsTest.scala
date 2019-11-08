package com.airbnb.sputnik.hive

import com.airbnb.sputnik.annotations._
import com.airbnb.sputnik.annotations.checks.{NotEmptyCheck, NotNull, RecordMinCount}
import com.airbnb.sputnik.enums.TableFileFormat.PARQUET
import com.airbnb.sputnik.example.Data.newUsers
import com.airbnb.sputnik.utils.SparkBaseTest
import com.google.common.base.CaseFormat
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

object HiveTableWriterUtilsTest {
  @TableDescription("asdf")
  @TableFormat(PARQUET)
  @TableParallelism(1)
  @FieldsFormatting(CaseFormat.LOWER_UNDERSCORE)
  @NotEmptyCheck
  @RecordMinCount(minCount = 50)
  case class A(@Comment("Comment of the field") @PartitioningField @NotNull someField: String)
}


@RunWith(classOf[JUnitRunner])
class HiveTableWriterUtilsTest extends SparkBaseTest {

  test("Test repartitioning") {
    import spark.implicits._

    val data = newUsers.toDF()
    val result = HiveTableWriterUtils.addPartitionColumn(data, 2).collect
    result.foreach(row => {
      val partitionField = row.getAs[String]("partition_by_column")
      assert(partitionField.length === 11)
      assert((partitionField.last == '0') || (partitionField.last == '1'))
    })
  }

  test("Test getTableProperties") {

    val tableProperties = DefaultAnnotationParser.getTableProperties(classOf[HiveTableWriterUtilsTest.A])
    assert(tableProperties.description === "asdf")
    assert(tableProperties.fileFormat === PARQUET)
    assert(tableProperties.defaultWriteParallelism === Some(1))
    assert(tableProperties.partitionSpec === Some(HivePartitionSpec(List("someField"))))
    assert(tableProperties.fieldComments.get("some_field").get === "Comment of the field")
  }


  test("Test getChecks") {

    val checks = DefaultAnnotationParser.getChecks(classOf[HiveTableWriterUtilsTest.A])
    assert(checks(0) === com.airbnb.sputnik.checks.NotEmptyCheck)
    assert(checks(1).asInstanceOf[com.airbnb.sputnik.checks.RecordMinCount].getMinCount() === 50)
    assert(checks(2).asInstanceOf[com.airbnb.sputnik.checks.NotNull].getColumnName() === "someField")

  }

}
