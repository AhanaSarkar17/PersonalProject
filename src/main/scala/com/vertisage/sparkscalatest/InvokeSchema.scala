package com.vertisage.sparkscalatest

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object InvokeSchema extends FileLoadSchemaless {

  val schema = StructType (Array(
    StructField("event_date",StringType,true),
    StructField("location_id",StringType,true),
    StructField("temp_celcius",IntegerType,true),
  ))

  val schema2 = StructType (Array(
    StructField("event_datetime",StringType,true),
    StructField("server_id",IntegerType,true),
    StructField("cpu_utilization",DoubleType,true),
    StructField("free_memory",DoubleType,true),
    StructField("session_count",IntegerType,true)
  ))

//  val schema3 = StructType (Array(
//    StructField("server_id",IntegerType,true),
//    StructField("server_name",StringType,true),
//    StructField("server_ip",StringType,true)
//  ))

  val schema3 = new StructType()
    .add("server_id", IntegerType, nullable = true)
    .add("server_name", StringType, nullable = true)
    .add("server_ip", StringType, nullable = true)

  val obj = new FileLoadSchemaless

  override val df = obj.sparkSession.read.option("header","true").schema(schema).csv(obj.file1)
  override val df1: DataFrame = obj.sparkSession.read.option("header","false").schema(schema2).csv(obj.file2)
  override val df2: DataFrame = obj.sparkSession.read.option("header","true").schema(schema3).csv(obj.file3)

}
