package com.airbnb.sputnik.hive

import java.time.LocalDate

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DateOffsetTest extends FunSuite {

  test("DateBoundsOffset") {

    var offset = DateBoundsOffset(0, 0)
    val ds = LocalDate.of(2019, 5, 5)
    assert(offset.getLowerBoundDate(ds) === ds)
    assert(offset.getUpperBoundDate(ds) === ds)

    offset = DateBoundsOffset(-1, 0)
    assert(offset.getLowerBoundDate(ds) === LocalDate.of(2019, 5, 4))
    assert(offset.getUpperBoundDate(ds) === ds)
    offset = DateBoundsOffset(-3, 3)
    assert(offset.getLowerBoundDate(ds) === LocalDate.of(2019, 5, 2))
    assert(offset.getUpperBoundDate(ds) === LocalDate.of(2019, 5, 8))
  }

  test("FixedDate") {

    val constantDate = LocalDate.of(2019, 5, 2)
    val offset = FixedDate(constantDate)
    var ds = LocalDate.of(2019, 5, 5)
    assert(offset.getLowerBoundDate(ds) === constantDate)
    assert(offset.getUpperBoundDate(ds) === constantDate)
    ds = LocalDate.of(2017, 3, 7)
    assert(offset.getLowerBoundDate(ds) === constantDate)
    assert(offset.getUpperBoundDate(ds) === constantDate)
  }

  test("FixedLowerBound") {
    val constantBeginningDate = LocalDate.of(2019, 5, 2)
    var offset = FixedLowerBound(constantBeginningDate)
    val ds = LocalDate.of(2019, 10, 5)
    assert(offset.getLowerBoundDate(ds) === constantBeginningDate)
    assert(offset.getUpperBoundDate(ds) === ds)
    offset = FixedLowerBound(constantBeginningDate, -1)
    assert(offset.getLowerBoundDate(ds) === constantBeginningDate)
    assert(offset.getUpperBoundDate(ds) === LocalDate.of(2019, 10, 4))
  }

}
