package com.airbnb.sputnik.example

import com.airbnb.sputnik.annotations.{Comment, FieldsFormatting, PartitioningField, TableDescription, TableName}
import com.google.common.base.CaseFormat
import com.airbnb.sputnik.enums.TableFileFormat
import com.airbnb.sputnik.annotations.TableFormat
import com.airbnb.sputnik.annotations.checks.NotEmptyCheck
import com.airbnb.sputnik.annotations.checks.NotNull

object Schemas {

  case class Visit(
                    userId: String,
                    url: String,
                    ds: String
                  )

  @TableName("user_data.visits_aggregation")
  @TableDescription("Counting distinct visited url for a user")
  @FieldsFormatting(CaseFormat.LOWER_UNDERSCORE)
  @TableFormat(TableFileFormat.RCFILE)
  case class VisitAggregated(
                              userId: String@Comment("Id of a user"),
                              distinctUrlCount: Long,
                              @PartitioningField ds: String
                            )

  case class User(
                   userId: String,
                   areaCode: String
                 )

  case class AreaCode(
                       areaCode: String,
                       country: String
                     )

  case class UserToCountry(
                            userId: String,
                            country: String
                          )

  case class CountryStats(
                           distinct_url_number: Long,
                           user_count: Long,
                           country: String,
                           @PartitioningField ds: String
                         )

  case class NewUser(
                      userId: String,
                      ds: String
                    )

  @TableName("user_data.new_user_count")
  @NotEmptyCheck
  case class NewUserCount(
                           user_count: Long,
                           @PartitioningField @NotNull platform: String,
                           @PartitioningField ds: String
                         )

  @TableName("user_data.mobile_row_data")
  case class MobileUsersRowData(
                                 userId: String,
                                 event: String,
                                 @PartitioningField ds: String
                               )

}
