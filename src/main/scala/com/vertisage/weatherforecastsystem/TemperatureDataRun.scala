package com.vertisage.weatherforecastsystem

object TemperatureDataRun{
  def main(args: Array[String]): Unit = {

    /** Taking the location as input */
    val location = scala.io.StdIn.readLine()

    println(TemparatureDataOperations.highestTemperature(location))
    //        println(TemparatureDataOperations.lowestTemperature(location))

    /** Taking temperature as input */
    //    val temp = scala.io.StdIn.readInt()
    //    avgTempLocations(temp)
    //    findCity(temp).foreach(println)

    /** Taking date as input */
    // val date = scala.io.StdIn.readLine()
    //TemparatureDataOperations.dateTemp(date)

  }
}
