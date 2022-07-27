package com.vertisage.sparkscalatest

import org.apache.spark.sql.DataFrame

object Implicit {
  implicit class DataFrameExtnd(dataframe: DataFrame) {

    def lengthDF() = {
      dataframe.count().toInt
    }

  }
}

