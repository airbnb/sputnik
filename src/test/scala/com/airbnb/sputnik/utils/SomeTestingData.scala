package com.airbnb.sputnik.utils

import com.airbnb.sputnik.utils.SomeTestingData.{MultiplePartitionedFields, OneMoreSimpleRow, OtherSimpleRow, SimpleRow}

object SomeTestingData {

  case class SimpleRow(field1: String,
                       field2: Int
                      )

  case class OtherSimpleRow(field1: String,
                            field2: Int,
                            field3: Int
                           )

  case class OneMoreSimpleRow(field1: String,
                              field2: String
                             )

  case class MultiplePartitionedFields(
                                      field1: String,
                                      partitionedfield1: String,
                                      partitionedfield2: String
                                      )

}

trait SomeTestingData extends SparkBaseTest {

  lazy val simpleRowDF = {
    val sparkSession = ss
    import sparkSession.implicits._
    List(
      SimpleRow("some_value_1", 1),
      SimpleRow("some_value_2", 2)
    ).toDF()
  }

  lazy val oneMoreSimpleRowDF = {
    val sparkSession = ss
    import sparkSession.implicits._
    List(
      OneMoreSimpleRow("field1_some_value_1", "field2_some_value_1"),
      OneMoreSimpleRow("field1_some_value_2", "field2_some_value_2")
    ).toDF()
  }

  lazy val multipleFieldsPartitioningRowDF = {
    val sparkSession = ss
    import sparkSession.implicits._
    List(
      MultiplePartitionedFields("field1_some_value_1", "partitionedField1_value_1", "partitionedField2_value_1"),
      MultiplePartitionedFields("field1_some_value_2", "partitionedField1_value_2", "partitionedField2_value_1"),
      MultiplePartitionedFields("field1_some_value_2", "partitionedField1_value_1", "partitionedField2_value_2"),
      MultiplePartitionedFields("field1_some_value_3", "partitionedField1_value_2", "partitionedField2_value_2")
    ).toDF()
  }

  lazy val otherSimpleRowDF = {
    val sparkSession = ss
    import sparkSession.implicits._
    List(OtherSimpleRow("", 1, 2)).toDF()
  }



}
