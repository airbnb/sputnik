package com.airbnb.sputnik.tools.beans;

import com.airbnb.sputnik.annotations.MapField;

public class ParsedDataMap {
  private String fieldOne;
  @MapField private MapData mapData;

  public String getFieldOne() {
    return fieldOne;
  }

  public void setFieldOne(String fieldOne) {
    this.fieldOne = fieldOne;
  }

  public MapData getMapData() {
    return mapData;
  }

  public void setMapData(MapData mapData) {
    this.mapData = mapData;
  }
}
