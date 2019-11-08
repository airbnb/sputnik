package com.airbnb.sputnik.utils

import java.io.File

import com.airbnb.sputnik.hive.{DropTable, TableCreation}
import com.airbnb.sputnik.tools.{DataFrameSuiteBase, SparkSessionUtils}
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

object SparkBaseTest {

  lazy val removeMetastore = {
    val filePaths = List("spark-warehouse", "metastore_db")
    filePaths.foreach(filePath => {
      val file = new File(filePath)
      if (file.exists()) {
        FileUtils.deleteDirectory(file)
      }
    })
  }

}

trait SparkBaseTest extends FunSuite with BeforeAndAfterAll with DataFrameSuiteBase {

  lazy val spark = ss
  implicit var ss: SparkSession = _

  def drop(tableName: String) = {
    DropTable.dropTable(ss, tableName)
  }

  override def beforeAll() {
    SparkBaseTest.removeMetastore
    val sparkConf = new SparkConf
    sparkConf.set("spark.master", "local")
    sparkConf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    sparkConf.set("spark.sql.shuffle.partitions", "1")
    sparkConf.set("spark.default.parallelism", "1")
    ss = SparkSessionUtils.getSparkSession(sparkConf)

  }

}
