package com.airbnb.sputnik.hive

import com.airbnb.sputnik.annotations.FieldsFormatting
import com.airbnb.sputnik.utils.SparkBaseTest
import com.google.common.base.CaseFormat
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@FieldsFormatting(CaseFormat.LOWER_UNDERSCORE)
case class A(someField: String, someOtherField: String)
case class B(some_field: String, some_other_field: String)

@RunWith(classOf[JUnitRunner])
class ColumnsFormattingTest extends SparkBaseTest {

  test("test toSnakeCase") {
    val sparkSession = ss
    import sparkSession.implicits._
    val input = List(
      A("", "")
    ).toDF()
    val output = ColumnsFormatting.dfToSQL(input, classOf[A])
    assert(output.columns === Array("some_field", "some_other_field"))
  }


  test("test toCamelCase") {
    val sparkSession = ss
    import sparkSession.implicits._
    val input = List(
      B("", "")
    ).toDF()
    val output = ColumnsFormatting.dfToJava(input, classOf[A])
    assert(output.columns === Array("someField", "someOtherField"))
  }


}
