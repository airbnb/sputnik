package com.airbnb.sputnik

import java.time.{LocalDate, Month}

import com.airbnb.sputnik.tools.DateConverter
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DateConverterTest extends FunSuite {

  test("Test date convertions") {

    assert(DateConverter.dateToString(
      LocalDate.of(2017, Month.JANUARY, 2)
    ) === "2017-01-02")

    assert(DateConverter.stringToDate("2017-01-02")
      === LocalDate.of(2017, Month.JANUARY, 2))
  }

}
