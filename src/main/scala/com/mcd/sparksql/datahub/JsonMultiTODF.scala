package com.mcd.sparksql.datahub

import org.apache.spark.SparkContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.dayofmonth
import org.apache.spark.sql.functions.hour
import org.apache.spark.sql.functions.minute
import org.apache.spark.sql.functions.month
import org.apache.spark.sql.functions.year
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory
import com.mcd.sparksql.util.DaasUtil
import org.apache.spark.sql.Row

class JsonMultiTODF {
  
}
object JsonMultiTODF {
  
  def main(args: Array[String]): Unit = {

    if (args.length < 0) {
      println("Please specify <JobName> <Master> <Exec Memort> <Driver Memory>")
      System.exit(-1)
    }
    //val mapProps = DaasUtil.getConfig("Daas.properties")
    val mapProps = DaasUtil.getConfig("Daas.properties")
    val master=DaasUtil.getValue(mapProps, "Master")
    val driverMemory=DaasUtil.getValue(mapProps, "Driver.Memory")
    val executorMemory=DaasUtil.getValue(mapProps, "Executor.Memory")
    val jobName="PartitionExample"
    val conf = DaasUtil.getJobConf(jobName, master, executorMemory, driverMemory);
    val sparkContext = new SparkContext(conf)
    val sqlContext = new HiveContext(sparkContext)
    val logger = LoggerFactory.getLogger("JDBCSparkSqlWithPropertiesFile03")
     val inputFile = "Input/Json/multi.json" //else args(0)
   // val output = args(1) // Output/Json
    
    val df=getDataFrame(sqlContext, inputFile);
    
    df.registerTempTable("rows")
   df.printSchema();
   df.show();
    
   
   val rows = df.select("rows");
        rows.registerTempTable("rows");
        rows.printSchema();
        rows.show();
        
       
    println("*********************************************")
    val topicsRecords = sqlContext.sql("select * from rows")
    println("---------------------------------------------")
    println(topicsRecords.count())
    println("*********************************************")
    topicsRecords.foreach(println)
    import scala.collection.mutable.WrappedArray
   
   topicsRecords.flatMap { x => x.toSeq.mkString("|").split("WrappedArray") }.foreach { x=> println(x.replace("),", "").replace("(", "").replace("))", "")) };


   // println("-->--->--->---> "+rowArray)
    
    
    
}
   def getDataFrame(sqlContext: SQLContext, sourceFilePath: String): DataFrame ={
    
 /* {"event":"AAA", "timestamp":"2015-06-10 12:54:43"}
    {"event":"AAA", "timestamp":"2015-06-10 12:54:43"}
    {"event":"AAA", "timestamp":"2015-06-10 14:54:43"} 
    {"event":"ZZZ", "timestamp":"2015-06-25 12:54:43"}
    {"event":"ZZZ", "timestamp":"2015-06-25 12:54:53"}*/
    
    sqlContext.read.json(sourceFilePath)
      
      
  }
   
   def yearCol(fromField: String): Column = {
    year(col(fromField)) as "year"
  }

  def monthCol(fromField: String): Column = {
    month(col(fromField)) as "month"
  }

  def dayCol(fromField: String): Column = {
    dayofmonth(col(fromField)) as "day"
  }

  def hourCol(fromField: String): Column = {
    hour(col(fromField)) as "hour"
  }

  def minuteCol(fromField: String): Column = {
    minute(col(fromField)) as "minute"
  }
  
  
}