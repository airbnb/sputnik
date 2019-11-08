package com.airbnb.sputnik.tools

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object DateConverter {

  private val formatter = DateTimeFormatter.ISO_DATE

  def dateToString(date: LocalDate): String = {
    date.format(formatter)
  }

  def stringToDate(ds: String): LocalDate = {
    LocalDate.parse(ds, formatter)
  }
}
