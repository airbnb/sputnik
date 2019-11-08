package com.airbnb.sputnik.tools

import java.sql.Timestamp

import com.airbnb.sputnik.hive.SchemaNormalisation
import com.airbnb.sputnik.tools.DataFrameSuiteBase._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import scala.math.abs

object DataFrameSuiteBase {

  /** Approximate equality, based on equals from [[Row]] */
  def approxEquals(r1: Row, r2: Row, tol: Double): Boolean = {
    if (r1.length != r2.length) {
      return false
    } else {
      (0 until r1.length).foreach(idx => {
        if (r1.isNullAt(idx) != r2.isNullAt(idx)) {
          return false
        }

        if (!r1.isNullAt(idx)) {
          val o1 = r1.get(idx)
          val o2 = r2.get(idx)
          o1 match {
            case b1: Array[Byte] =>
              if (!java.util.Arrays.equals(b1, o2.asInstanceOf[Array[Byte]])) {
                return false
              }

            case f1: Float =>
              if (java.lang.Float.isNaN(f1) !=
                java.lang.Float.isNaN(o2.asInstanceOf[Float])) {
                return false
              }
              if (abs(f1 - o2.asInstanceOf[Float]) > tol) {
                return false
              }

            case d1: Double =>
              if (java.lang.Double.isNaN(d1) !=
                java.lang.Double.isNaN(o2.asInstanceOf[Double])) {
                return false
              }
              if (abs(d1 - o2.asInstanceOf[Double]) > tol) {
                return false
              }

            case d1: java.math.BigDecimal =>
              if (d1.compareTo(o2.asInstanceOf[java.math.BigDecimal]) != 0) {
                return false
              }

            case t1: Timestamp =>
              if (abs(t1.getTime - o2.asInstanceOf[Timestamp].getTime) > tol) {
                return false
              }

            case _ =>
              if (o1 != o2) return false
          }
        }
      })
    }
    true
  }

}

// copy-past from com.holdenkarau.spark.testing.DataFrameSuiteBase
trait DataFrameSuiteBase extends Serializable {

  val maxUnequalRowsToShow = 10

  /**
    * Compares if two [[DataFrame]]s are equal, checks the schema and then if that
    * matches checks if the rows are equal.
    */
  def assertDataFrameEquals(expected: DataFrame, result: DataFrame) {
    assertDataFrameApproximateEquals(expected, result, 0.0)
  }

  def assertSchemaEqual(schema1: StructType, schema2: StructType) = {
    SchemaNormalisation.compareSchema(schema1, "schema1", schema2, "schema2")
  }


  def assertDataFrameAlmostEquals[T](ds1: Dataset[T], ds2: Dataset[T]) = {
    val df1 = ds1.toDF()
    val df2 = ds2.toDF()
    assertDataFrameEquals(
      SchemaNormalisation.reorder(df1, df2.schema).sort(df2.columns.map(df1.col(_)): _*),
      SchemaNormalisation.reorder(df2, df2.schema).sort(df2.columns.map(df2.col(_)): _*)
    )
  }

  def assertDatasetApproximateEquals[T](ds1: Dataset[T], ds2: Dataset[T], tol: Double) = {
    val df1 = ds1.toDF()
    val df2 = ds2.toDF()
    assertDataFrameApproximateEquals(
      SchemaNormalisation.reorder(df1, df2.schema).sort(df2.columns.map(df1.col(_)): _*),
      SchemaNormalisation.reorder(df2, df2.schema).sort(df2.columns.map(df2.col(_)): _*),
      tol)
  }

  /**
    * Compares if two [[DataFrame]]s are equal, checks that the schemas are the same.
    * When comparing inexact fields uses tol.
    *
    * @param tol max acceptable tolerance, should be less than 1.
    */
  def assertDataFrameApproximateEquals(
                                        expected: DataFrame, result: DataFrame, tol: Double) {
    assertSchemaEqual(expected.schema, result.schema)
    try {
      expected.rdd.cache
      result.rdd.cache

      val expectedIndexValue = zipWithIndex(expected.rdd)
      val resultIndexValue = zipWithIndex(result.rdd)

      val unequalRDD = expectedIndexValue.fullOuterJoin(resultIndexValue).
        filter { case (idx, (r1, r2)) =>
          r1.isEmpty || r2.isEmpty || !(r1.equals(r2) || approxEquals(r1.get, r2.get, tol))
        }
      unequalRDD.cache()
      try {
        if (!unequalRDD.isEmpty()) {
          val expectedCount = expected.rdd.count
          val resultCount = result.rdd.count
          if (expectedCount != resultCount) {
            throw new RuntimeException(s"Expected df size($expectedCount) is not equal " +
              s"actual result df size($resultCount). Rows diff: ${unequalRDD.collect().mkString("\n")}")
          } else {
            throw new RuntimeException(s"There are unequal rows: ${unequalRDD.collect().mkString("\n")}")
          }
        }
      } finally {
        unequalRDD.unpersist()
      }
    } finally {
      expected.rdd.unpersist()
      result.rdd.unpersist()
    }
  }


  /**
    * Zip RDD's with precise indexes. This is used so we can join two DataFrame's
    * Rows together regardless of if the source is different but still compare
    * based on the order.
    */
  def zipWithIndex[U](rdd: RDD[U]) = {
    rdd.zipWithIndex().map { case (row, idx) => (idx, row) }
  }

}
