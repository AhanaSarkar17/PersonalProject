package com.vertisage.sparkscalatest
import Implicit._
import javassist.Loader.Simple
import org.apache.spark.sql.{Column, DataFrame, Encoders, functions}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{array_join, col, collect_list, concat, concat_ws, lit, mean, regexp_replace, round, udf, when}
import org.apache.spark.sql.types.StringType

import java.util.Base64.Encoder

object Joins extends App {

  val dataFrameUtilization = InvokeSchema.df1
  val dataFrameServer = InvokeSchema.df2

  //Inner Join
  val innerJoinedDF = dataFrameUtilization.join(dataFrameServer, dataFrameUtilization("server_id") === dataFrameServer("server_id"), "inner")
  //  innerJoinedDF.dropDuplicates("server_id").orderBy(dataFrameUtilization.col("server_id")).show(innerJoinedDF.lengthDF())

  // Outer Join
  //  val outerJoinedDF = dataFrameUtilization.join(dataFrameServer, dataFrameUtilization("server_id") === dataFrameServer("server_id"),"full")
  // outerJoinedDF.dropDuplicates("server_id").sort(dataFrameServer.col("server_id").desc).show(outerJoinedDF.lengthDF())

  val outer_df = dataFrameUtilization.join(dataFrameServer, Seq("server_id"), "outer")
  //outer_df.dropDuplicates("server_id").sort("server_id")show(outer_df.lengthDF())

  //  val outerDropDuplicateCol = outerJoinedDF.drop(dataFrameUtilization.col("server_id"))
  //  outerDropDuplicateCol.dropDuplicates("server_id").show(outerDropDuplicateCol.lengthDF())
  //
  //  val filteredOuterDF = outerDropDuplicateCol.filter(col("session"))
  //
  //  val newcolumDF = outerJoinedDF.withColumn("server_ID", MyFunc.allServerID(dataFrameUtilization.col("server_id"),dataFrameServer))

  val mean_df = outer_df.groupBy("server_id").agg(round(mean("free_memory"), 3))

  /** Returns a single column(match column) with entire set of values from both tables*/
  val entire_df = outer_df.join(mean_df, Seq("server_id"), "full").withColumnRenamed("round(avg(free_memory), 3)", "avg_free_memory")
  //entire_df.dropDuplicates("server_id").select("server_id", "server_name", "avg_free_memory")show(entire_df.lengthDF())

  //  val cpu_util_df = outer_df.groupBy("cpu_utilization").count()
  val cpu_util_df = outer_df.select("server_id", "server_name", "cpu_utilization").distinct().orderBy("cpu_utilization")
  //val cpu_util_group_df = cpu_util_df.groupBy("cpu_utilization").count()

  //cpu_util_group_df.show(cpu_util_group_df.lengthDF())

  /** checking multiple values from other distinct rows into a single row for a single value in a different column */
  //cpu_util_df.orderBy("server_name").groupBy("cpu_utilization").agg(array_join(collect_list("server_name"), ", ")).show(false)
  //cpu_util_df.groupBy(col("cpu_utilization")).agg(concat_ws(",", collect_list("server_name"))).show(false)
  //cpu_util_df.groupBy("cpu_utilization").agg(concat_ws(",", collect_list("server_name"))).show(false)

//  val newtestDF = cpu_util_df.toDF("server_id", "server_name", "cpu_utilization")
//  newtestDF.groupBy("cpu_utilization").agg(concat_ws(",", collect_list("server_name"))).show(false)

  //entire_df.na.fill("-").na.fill(0).dropDuplicates("server_id").show(entire_df.lengthDF())

  /** Using when() otherwise */
  //entire_df.withColumn("session_count", when(col("session_count") === 55, 458798)).dropDuplicates("server_id").show(entire_df.lengthDF())
//  val updatedDf = entire_df.withColumn("session_count", regexp_replace(col("session_count"), "55", "123654"))
//  updatedDf.dropDuplicates("session_count").show(500)

  /** Replacing a regex match */
  val updatedDf = entire_df.withColumn("server_name", regexp_replace(col("server_name"), " ", "-"))
  //updatedDf.dropDuplicates("server_name").show(500)

  /** Removing null values */
  val nonullDF = entire_df.na.fill("-").na.fill(0)

  /** Using udf to modify string of a column */
  val modifyServerName = udf(MyFunc.stringChange)
  val modifiednameDF = nonullDF.withColumn("server_name", modifyServerName(col("server_name"))).dropDuplicates("server_id")
//  modifiednameDF.select("event_datetime","cpu_utilization","free_memory","session_count","server_id","server_name").show(modifiednameDF.lengthDF())

/** Using Map functions */
//val val1 = modifiednameDF.map(r => (r.getString(4)+ "0"))(Encoders.STRING).show()
//  modifiednameDF.select("event_datetime","cpu_utilization","free_memory","session_count","server_id","server_name")
//    .map(r => (r.getString(0), r.getString(5)+ "0"))(Encoders.tuple[String,String](Encoders.STRING,Encoders.STRING)).show()

/** Concatenating value to front or back of a column */
  //modifiednameDF.withColumn("server_id", concat(lit("0"),col("server_id").cast(StringType))).show()

/** Splitting a single column into multiple column */
  val splitDF: DataFrame = modifiednameDF.withColumn("event_date",functions.split(col("event_datetime")," ").getItem(0))
    .withColumn("event_time",functions.split(col("event_datetime")," ").getItem(1))
    .drop("event_datetime")

}
object MyFunc {

  def allServerID(df1col:Int, df2col:Int) = {
    if (df1col == null) df2col
    else df1col
  }

  val stringChange = (name: String) => {
    if (name == "-") "-"
    else {
      val anArray = name.split(" ")
      anArray(1) + anArray(0)
      (anArray.tail.head + anArray.head)
    }
  }

}
