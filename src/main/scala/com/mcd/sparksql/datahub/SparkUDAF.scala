package com.mcd.sparksql.datahub
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType }
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.mcd.sparksql.datahub._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType }
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.mcd.sparksql.datahub._
import com.mcd.sparksql.util._
import org.apache.spark.sql._
import com.mcd.sparksql.datahub._
import org.slf4j.LoggerFactory
import org.apache.spark.sql.hive.HiveContext
import java.util.Date
import java.util.GregorianCalendar
import java.util.Calendar
import java.text.SimpleDateFormat
object SparkUDAF {
  
  def main(args: Array[String]): Unit = {
    
   if (args.length < 0) {
      println("Please specify <JobName> <Master> <Exec Memort> <Driver Memory>")
      System.exit(-1)
    }
    //val mapProps = DaasUtil.getConfig("Daas.properties")
    val props = DaasUtil.get("Daas.properties")
    val master=props.getProperty("Master")
    println(master)
    //val driverMemory=props.getProperty("Driver.Memory")
    //val executorMemory=props.getProperty("Executor.Memory")
    val jobName="DataMartRefreshProcess"
    val conf = DaasUtil.getJobConf(jobName, master);
    val sparkContext = new SparkContext(conf)
    //val sqlContext = new HiveContext(sparkContext)
    val logger = LoggerFactory.getLogger("DataMarts")

    val hiveContext = new SQLContext(sparkContext);
    
    
    
    
    val customDatahubSchema=StructType(Array(
StructField("customerid", IntegerType, true),
StructField("accountid", IntegerType, true),
StructField("billdate", DateType, true),
StructField("totalcurrentcharges", DoubleType, true)))
import org.apache.spark.sql._
val datahubDataFrame = hiveContext.load(
      "com.databricks.spark.csv",
      schema = customDatahubSchema,
      Map("path" -> "datahubudaf", "header" -> "false","delimiter" -> "|"))

   /* val selectedData = df.select("pos_itm_lvl_nu", "last_updt_ts")
    selectedData.take(5).foreach(println)
    selectedData.save("newcars.csv", "com.databricks.spark.csv")*/
    
    datahubDataFrame.registerTempTable("tld_datahub")

  val df = hiveContext.sql("select customerid,accountid,billdate,totalcurrentcharges FROM tld_datahub")
  //df.show()
  val totAvgCount = new totAvgCount()
  val ef =  df.groupBy("customerid","accountid").agg(totAvgCount(df.col("totalcurrentcharges"), df.col("billdate")).as("AVE_TOT_CUR_CHARGS"))
    
   ef.show
   
  }
  
}