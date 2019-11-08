package com.airbnb.sputnik.tools

import com.airbnb.sputnik.annotations.MapField
import com.airbnb.sputnik.tools.DatasetEncoderTest._
import com.airbnb.sputnik.tools.beans.{JsonData, ParsedData, ParsedDataMap}
import com.airbnb.sputnik.utils.SparkBaseTest
import org.apache.spark.sql.Encoders
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

object DatasetEncoderTest {

  case class DataWithJsonString(
                                 fieldOne: String,
                                 jsonFieldOne: String
                               )

  case class DataWithMap(
                          fieldOne: String,
                          mapData: Map[String, String]
                        )

  case class SomeMapDataWrapper(
                                 @MapField someMapData: SomeMapData,
                                 someOtherField: String
                               )

  case class SomeMapData(
                          fieldOne: String
                        )

}

@RunWith(classOf[JUnitRunner])
class DatasetEncoderTest extends SparkBaseTest {

  test("test toJava json ") {

    import spark.implicits._
    val inputData = List(
      DataWithJsonString("someValue1", """{"innerFieldOne": "value"}""")
    ).toDF()

    val resultDF = DatasetEncoder
      .toJava(inputData, classOf[ParsedData])
    val resultDataset = resultDF.as(Encoders.bean(classOf[ParsedData]))

    val jsonData = new JsonData()
    jsonData.setInnerFieldOne("value")
    val parsedData = new ParsedData()
    parsedData.setJsonFieldOne(jsonData)
    parsedData.setFieldOne("someValue1")
    val resultItem = resultDataset.head()
    assert(resultItem.getFieldOne === parsedData.getFieldOne)
    assert(resultItem.getJsonFieldOne.getInnerFieldOne === parsedData.getJsonFieldOne.getInnerFieldOne)
  }

  test("test toJava map") {

    import spark.implicits._
    val inputData = List(
      DataWithMap("someValue1", Map("innerFieldOne" -> "val", "innerFieldTwo" -> "val2"))
    ).toDF()

    val resultDF = DatasetEncoder
      .toJava(inputData, classOf[ParsedDataMap])
    val resultDataset = resultDF.as(Encoders.bean(classOf[ParsedDataMap]))

    val resultItem = resultDataset.head()
    assert(resultItem.getFieldOne === "someValue1")
    assert(resultItem.getMapData.getInnerFieldOne === "val")
    assert(resultItem.getMapData.getInnerFieldTwo === "val2")
  }

  test("test toSQL for json field") {
    val inputData = BeansDataset.getDataset(spark)
    val result = DatasetEncoder.toSQL(inputData.toDF, classOf[ParsedData])
    assert(result.head.getAs[String]("fieldOne") === "someValue1")
    assert(result.head.getAs[String]("jsonFieldOne") === "{\"innerFieldOne\":\"hi\"}")
  }

  test("test toSQL for map field") {

    import spark.implicits._
    val inputData = List(
      SomeMapDataWrapper(SomeMapData("someValue1"), "sfsd")
    ).toDF()

    val result = DatasetEncoder
      .toSQL(inputData, classOf[SomeMapDataWrapper])
      .head()
    assert(result.getAs[String]("someOtherField") === "sfsd")
    assert(result.getAs[Map[String, String]]("someMapData") === Map("fieldOne" -> "someValue1"))
  }


}
