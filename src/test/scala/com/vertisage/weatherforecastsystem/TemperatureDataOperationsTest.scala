package com.vertisage.weatherforecastsystem
import com.vertisage.sparkscalatest.Implicit._
import org.junit.Test
import org.junit.Assert._

class TemperatureDataOperationsTest {

  @Test
  def maxTempCheck: Unit = {
    assertEquals(TemparatureDataOperations.highestTemperature("loc248"),36)
  }

  @Test
  def minTempCheck: Unit = {
    assertEquals(TemparatureDataOperations.lowestTemperature("loc0"),27)
  }


  @Test
  def dateTimedfCheck: Unit = {
    assert(TemparatureDataOperations.dateTemp("03/04/2019 19:48:59").lengthDF() == 3)
  }
}
