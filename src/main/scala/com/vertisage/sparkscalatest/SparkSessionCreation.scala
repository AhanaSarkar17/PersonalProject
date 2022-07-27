package com.vertisage.sparkscalatest

import org.apache.spark.sql.SparkSession

class CreatingSparkSession {
  def getSparkSession() : SparkSession = {

    val sparkSession = SparkSession.builder.
      master("local[*]")
      .appName("Hello world")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    sparkSession
  }
}

object SparkSessionCreation {
  def main(args: Array[String]): Unit = {

    val obj = new CreatingSparkSession
    val spark = obj.getSparkSession()

    val df = spark.read.option("header","true").
      csv("D:\\Study\\deng_workspace\\deng_workspace\\Data\\location_temp.csv")

    df.show()
  }

}
