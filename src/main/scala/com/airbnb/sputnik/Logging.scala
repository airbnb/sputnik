package com.airbnb.sputnik

trait Logging {

  @transient lazy val logger = org.apache.log4j.LogManager.getLogger(this.getClass)

}
