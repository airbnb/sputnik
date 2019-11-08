package com.airbnb.sputnik.hive

import com.airbnb.sputnik.DS_FIELD

object HivePartitionSpec {
  val DS_PARTITIONING = Some(HivePartitionSpec(List(DS_FIELD)))
}

/**
  * Represents partition specification for a Hive table. I
  */
case class HivePartitionSpec(
                              columns: Seq[String]
                            ) {

  override def toString: String = {
    columns.mkString(", ")
  }

}