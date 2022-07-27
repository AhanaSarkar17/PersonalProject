package com.vertisage.sparkscalatest
import org.apache.spark.sql._

object Checks {

  val dfserver = InvokeSchema.df2
  dfserver.select("server_id")

  val anArray =  Array(1,2,3,4,5,6,7)
  val newArray = anArray.map(_+2)
  newArray.foreach(println)

}
