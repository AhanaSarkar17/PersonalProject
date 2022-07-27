package com.vertisage.sparkscalatest

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import Implicit._

object FileOperations {
  def main(args: Array[String]): Unit = {

    val dataFrame = InvokeSchema.df
    AggregateOps.operationsum(dataFrame)
    //dataFrame.show()
    AggregateOps.display(AggregateOps.columnUpdate(dataFrame))
    AggregateOps.impliDisplay(dataFrame)
  }

  object userDefinedFunc {
    val convertF = (temp: Int) => {
      ((temp*(9/5))+ 32).toInt
    }

    val lengthDF = (dF: DataFrame) => {
      dF.count().toInt
    }
  }

  object AggregateOps {
     def operationsum (df : DataFrame) ={
       //println(df.select("location_id").distinct().count())
       println(df.dropDuplicates("location_id").count())
       }

    def columnUpdate (df : DataFrame) = {
      val FConvert = udf(userDefinedFunc.convertF)
      val dfNew = df.withColumn("temp_ferhen", FConvert(col("temp_celcius")))
      dfNew
    }

    def display(df : DataFrame) = {
      df.show()
    }

    def displayEntireDF(df: DataFrame): Unit = {
      df.show(userDefinedFunc.lengthDF(df), false)
    }

    def impliDisplay(df: DataFrame): Unit = {
      val len = df.lengthDF()
      df.show(len,false)
    }

  }

}
