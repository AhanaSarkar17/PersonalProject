package com.vertisage.sparkscalatest
import com.typesafe.config.ConfigFactory

object getSparkS extends CreatingSparkSession {
  getSparkSession()
}

class FileLoadSchemaless {
  val config = ConfigFactory.load()
  val filepath = config.getConfig("file.name")
  val file1 = filepath.getString("inputFilePathloctemp")
  val file2 = filepath.getString("inputFilePathutil")
  val file3 = filepath.getString("inputFilePathServer")

  val sparkSession = getSparkS.getSparkSession()
  val df = sparkSession.read.option("header","true").csv(file1)
  val df1 = sparkSession.read.option("header","false").csv(file2)
  val df2 = sparkSession.read.option("header","true").csv(file3)
}
object InvokingFileTest {

  def main(args: Array[String]): Unit = {

    val obj = new FileLoadSchemaless

    println(obj.file1)
    println(obj.file2)

    obj.df.show()
    obj.df1.show()

  }

}
