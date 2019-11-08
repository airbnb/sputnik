package com.airbnb.sputnik.tools.beans;

import java.io.Serializable;

public class JsonData implements Serializable {

  private String innerFieldOne;

  public String getInnerFieldOne() {
    return innerFieldOne;
  }

  public void setInnerFieldOne(String innerFieldOne) {
    this.innerFieldOne = innerFieldOne;
  }
}
