package com.airbnb.sputnik.hive

import java.time.LocalDate

/**
  * By default we take data from ds which we are processing,
  * but DateOffset allows you to change date bounds of
  * data you are reading.
  *
  * Lower bound is the bound from which start to take data.
  * Upper bound is the bound till which we take the data starting from lower bound.
  * In case of just processing ds - ds value is lower bound and upper bound.
  * If we would like to process ds and date before that like we do
  * in DS_AND_DATE_BEFORE_THAT, than lower bound would be ds-1 and
  * upper bound would be ds
  *
  * Noticeable implementations:
  * <p><ul>
  * <li> case class {@link com.airbnb.sputnik.hive.DateBoundsOffset}
  * which allows you to adjust lower bound and upper bound relative to ds
  * <li> case class  {@link com.airbnb.sputnik.hive.FixedDate}
  * which allows you filter exact date
  * <li> case class {@link com.airbnb.sputnik.hive.FixedLowerBound}
  * which allows you to specify exact date for lower bound and provide offset to upper
  * bound in case you need it
  * <li> constant FROM_BEGINNING_TO_DS
  * which allow you get all data from beginning of times(1970-01-01) till ds(including ds)
  * <li> constant DAY_BEFORE_DS
  * which allow you get data for day before DS
  * <li> constant DS_AND_DATE_BEFORE_THAT
  * which allow you get data for day before DS and data in DS
  * </ul><p>
  */
trait DateOffset {
  def getLowerBoundDate(lowerBoundDate: LocalDate): LocalDate

  def getUpperBoundDate(upperBoundDate: LocalDate): LocalDate
}

/**
  * case class which allows you to provide exact day for which you would take the data
  * on every run, independently of current ds
  *
  * @param localDate day for which you want to take the data
  */
case class FixedDate(localDate: LocalDate) extends DateOffset {
  def getLowerBoundDate(lowerBoundDate: LocalDate): LocalDate = {
    localDate
  }

  def getUpperBoundDate(upperBoundDate: LocalDate): LocalDate = {
    localDate
  }
}

/**
  * case class which allows you to fix lowerBound, but upper bound is ds by default,
  * but you can provide offset the way you would do for DateBoundsOffset
  *
  * @param lowerBound  fixed lower bound
  * @param upperOffset offset which would be added to upper bound
  */
case class FixedLowerBound(lowerBound: LocalDate, upperOffset: Int = 0) extends DateOffset {
  def getLowerBoundDate(lowerBoundDate: LocalDate): LocalDate = {
    lowerBound
  }

  def getUpperBoundDate(upperBoundDate: LocalDate): LocalDate = {
    upperBoundDate.plusDays(upperOffset)
  }
}

/**
  * case class, which used to specify offset on lower bound and upper bound.
  * Integers, which you pass get days added to their respective bounds.
  * Examples:
  * <p><ul>
  * <li> DateBoundsOffset(0, 0) - data for ds
  * <li> DateBoundsOffset(-1, -1) - data for day before ds
  * <li> DateBoundsOffset(-1, 0) - data for day before ds and ds
  * <li> DateBoundsOffset(-10, 0) - data for 10 days before ds and ds
  * <li> DateBoundsOffset(-10, -10) - data for day, which was 10 days before ds
  * </ul><p>
  *
  * @param lowerOffset offset which would be added to lower bound
  * @param upperOffset offset which would be added to upper bound
  */
case class DateBoundsOffset(lowerOffset: Int = 0, upperOffset: Int = 0) extends DateOffset {
  def getLowerBoundDate(lowerBoundDate: LocalDate): LocalDate = {
    lowerBoundDate.plusDays(lowerOffset)
  }

  def getUpperBoundDate(upperBoundDate: LocalDate): LocalDate = {
    upperBoundDate.plusDays(upperOffset)
  }
}

object DateOffset {

  val FROM_BEGINNING_TO_DS = Some(FixedLowerBound(lowerBound = LocalDate.of(1970, 1, 1)))
  val DAY_BEFORE_DS = Some(DateBoundsOffset(lowerOffset = -1, upperOffset = -1))
  val DS_AND_DATE_BEFORE_THAT = Some(DateBoundsOffset(lowerOffset = -1))

}
