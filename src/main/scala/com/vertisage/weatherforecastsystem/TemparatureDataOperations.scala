package com.vertisage.weatherforecastsystem
import com.vertisage.sparkscalatest.InvokeSchema
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col

object TemparatureDataOperations {

  val dataFrameTemp = InvokeSchema.df

  //Function calculating highest temperature for a given location_id and returns Int
  def highestTemperature(location: String) = {
    val hTDF = dataFrameTemp.filter(dataFrameTemp.col("location_id") === location)
      .agg(functions.max("temp_celcius").alias("max_temp"))
      .select("max_temp")
    hTDF.collect()(0)(0).asInstanceOf[Int]
  }

  //Function calculating lowest temperature for a given location_id and returns Int
  def lowestTemperature(location: String) = {
    val lTDF = dataFrameTemp.filter(dataFrameTemp.col("location_id") === location)
      .agg(functions.min("temp_celcius").alias("min_temp"))
      .select("min_temp")
    lTDF.collect()(0)(0).asInstanceOf[Int]
  }

  //Function returns location based on avg temp which is greater than a given temp
  def avgTempLocations(temp: Int) = {
    dataFrameTemp.groupBy("location_id").agg(functions.avg("temp_celcius").alias("avg_temp"))
      .filter(col("avg_temp") >= temp)
  }

  //Function returns all the location_id which had a specific temperature
  def findCity(temp: Int) = {
    dataFrameTemp.filter(col("temp_celcius") === temp).select("location_id").distinct().collect()

  }

  //Function returns temp for each location for a given date
  def dateTemp(date: String) = {
    dataFrameTemp.filter(col("event_date") === date).select("location_id", "temp_celcius")
  }
}

  //Singleton object to run main

