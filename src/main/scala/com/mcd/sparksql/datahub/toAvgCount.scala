package com.mcd.sparksql.datahub

import java.math.BigDecimal
import org.apache.spark.sql.expressions.{ MutableAggregationBuffer, UserDefinedAggregateFunction }
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ DoubleType, StringType, StructField, StructType, DateType, LongType, DataType }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.text.ParseException
import java.util.Date
import  java.util.Calendar
import org.apache.spark.sql.functions._
  
class totAvgCount() extends UserDefinedAggregateFunction {
  def inputSchema: StructType = StructType(Array(StructField("totalavg", DoubleType), StructField("billdate", DateType)))
  def bufferSchema = StructType(Array(StructField("sum", DoubleType), StructField("cnt", LongType))) 
  def dataType: DataType = DoubleType 
  def deterministic = true 
  
  val billCycleDate = new SimpleDateFormat("yyyy-MM-dd").parse("2016-06-26")
  val  c = Calendar.getInstance()
  c.setTime(new Date(billCycleDate.getTime()))
  c.add(Calendar.MONTH, -5) 
  val thersholdDateToCaculateAvg = c.getTime();    
  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = 0.toDouble
    buffer(1) = 0L
  }
  def update(buffer: MutableAggregationBuffer, input: Row) = {
   val billCycleDateForEachCustomer = new Date(input.getDate(1).getTime())
    if (billCycleDateForEachCustomer.compareTo(thersholdDateToCaculateAvg) > 0) {
      if(!buffer.getDouble(0).isNaN() && !input.getDouble(0).isNaN())
      {
        buffer(0) = buffer.getDouble(0) + input.getDouble(0)
      buffer(1) = buffer.getLong(1) + 1
        
      }
      
    }
  }
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
     if(!buffer1.getDouble(0).isNaN() && !buffer2.getDouble(0).isNaN()){
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
     }
  }
  def evaluate(buffer: Row) = {
    buffer.getDouble(0) / buffer.getLong(1).toDouble
  }
}