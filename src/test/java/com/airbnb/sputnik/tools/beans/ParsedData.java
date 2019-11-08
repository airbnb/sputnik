package com.airbnb.sputnik.tools.beans;

import com.airbnb.sputnik.annotations.Comment;
import com.airbnb.sputnik.annotations.FieldsFormatting;
import com.airbnb.sputnik.annotations.JsonField;
import com.airbnb.sputnik.annotations.TableName;
import com.google.common.base.CaseFormat;

import java.io.Serializable;

@TableName("default.someTableParsedData")
@FieldsFormatting(CaseFormat.LOWER_UNDERSCORE)
public class ParsedData implements Serializable {

  @Comment("Some comment")
  private String fieldOne;

  @JsonField private JsonData jsonFieldOne;

  public String getFieldOne() {
    return fieldOne;
  }

  public void setFieldOne(String fieldOne) {
    this.fieldOne = fieldOne;
  }

  public JsonData getJsonFieldOne() {
    return jsonFieldOne;
  }

  public void setJsonFieldOne(JsonData jsonFieldOne) {
    this.jsonFieldOne = jsonFieldOne;
  }
}
