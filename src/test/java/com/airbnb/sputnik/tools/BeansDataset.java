package com.airbnb.sputnik.tools;

import com.airbnb.sputnik.tools.beans.JsonData;
import com.airbnb.sputnik.tools.beans.ParsedData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;

public class BeansDataset {

  public static Dataset<ParsedData> getDataset(SparkSession sparkSession) {
    JsonData jsonData = new JsonData();
    jsonData.setInnerFieldOne("hi");
    ParsedData parsedData = new ParsedData();
    parsedData.setJsonFieldOne(jsonData);
    parsedData.setFieldOne("someValue1");

    Encoder<ParsedData> jsonDataEncoder = Encoders.bean(ParsedData.class);
    Dataset<ParsedData> dataset =
        sparkSession.createDataset(Collections.singletonList(parsedData), jsonDataEncoder);
    return dataset;
  }
}
