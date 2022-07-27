package com.vertisage.sparkscalatest
import org.apache.spark.sql._
import com.vertisage.sparkscalatest.FileOperations.AggregateOps
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, mean, round}

object SqlOperations {
 val dataFrameUtilization = InvokeSchema.df1
  val dataFrameServer = InvokeSchema.df2

  def main(args: Array[String]): Unit = {
//    AggregateOps.display(dataFrameUtilization)
//    AggregateOps.display(dataFrameServer)
//    dataFrameServer.printSchema()
//    dataFrameUtilization.head(10).foreach(println)
//    println()
//    dataFrameUtilization.take(2).foreach(println)
//    //println(dataFrameUtilization.columns.mkString("Array(", ", ", ")"))
//    //println(dataFrameServer.describe())
//    dataFrameUtilization.sample(0.001).show()
//    dataFrameUtilization.sort(col("free_memory").desc).show()
//    dataFrameUtilization.orderBy("free_memory").show()
//   // println(dataFrameUtilization.filter(col("free_memory") === 0.0).count())
//    dataFrameUtilization.groupBy("server_id").count().show()
    val newDataFrame = dataFrameUtilization.groupBy("server_id").agg(round(mean("free_memory"),3))
    val showDataFrame = newDataFrame.withColumnRenamed("round(avg(free_memory), 3)", "MeanFreeMemory")
    println(showDataFrame.count())
   showDataFrame.repartition(10).write.mode(SaveMode.Overwrite).csv("D:\\Study\\deng_workspace\\deng_workspace\\Data\\mean")

  }
}
