package com.mcd.json.parsing
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType }
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.mcd.sparksql.datahub._
import com.mcd.sparksql.util._
import org.apache.spark.sql._
import com.mcd.sparksql.datahub._
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import java.io.StringReader
import org.apache.spark._
import scala.util.parsing.json.JSON
import scala.collection.JavaConversions._
import au.com.bytecode.opencsv.CSVReader
import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD

/*
 * * 
 * http://wpcertification.blogspot.com/2015/12/i-wanted-to-build-spark-program-that.html
 * Limitations: Each line should be Json
 * 
 * */

object WholeJsonParsing {
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("Please specify <JobName> <Master> <Exec Memort> <Driver Memory>")
      System.exit(-1)
    }
    val inputFile = args(0) // Input/Json
    val output = args(1) // Output/Json

    println(inputFile)
    val mapProps = DaasUtil.getConfig("Daas.properties")
    val master=DaasUtil.getValue(mapProps, "Master")
    val driverMemory=DaasUtil.getValue(mapProps, "Driver.Memory")
    val executorMemory=DaasUtil.getValue(mapProps, "Executor.Memory")
    val jobName="WholeJsonParsing"
    val conf = DaasUtil.getJobConf(jobName, master, executorMemory, driverMemory);
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)

    val logger = LoggerFactory.getLogger("JSONFileReaderWriter")
    val mapper = new ObjectMapper

    val hadoopConf = new org.apache.hadoop.conf.Configuration()

    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:9000"), hadoopConf)

    try { hdfs.delete(new org.apache.hadoop.fs.Path(output), true) } catch { case _: Throwable => {} }

    logger.debug(s"Read json from $inputFile and write to $output")

    val errorRecords = sparkContext.accumulator(0)

    /*  The RDD consists of a tuple whose 1st element is a filename and whose 2nd element is the data with the lines separated by whitespace.
				In order to prepare the data for proper ingestion by Spark SQL, we utilize the following transformation
				http://searchdatascience.com/spark-adventures-1-processing-multi-line-json-files*/

    val records = sparkContext.wholeTextFiles(inputFile, 2) // whole text file returns tuple as suggested by ticket from apache spark
    
    val jsonData=parseJsonData(records, mapper, errorRecords);
    val results=filterJsonDataByCity(jsonData, mapper, errorRecords)
    
    val df=convertRDDToDF(records, mapper, errorRecords, sparkContext);
    df.printSchema()
    
    // val jsonDataWithDF=convertRDDToDF

    /*var results = records.flatMap { record =>
      try {
        Some(mapper.readValue(record._2, classOf[Person])) // whole text file returns tuples whose first value if file location and next one if data in file
      } catch {
        case e: Exception => {
          errorRecords += 1
          None
        }
      }
    }.filter(person => person.address.city.equals("mumbai"))*/
    
    results.saveAsTextFile(output)
    
    println("Number of bad records " + errorRecords)

  }
  
  def parseJsonData(records:RDD[(String,String)],mapper:ObjectMapper,errorRecords:Accumulator[Int]):RDD[Person]={
     var results = records.flatMap { record =>
      try {
        Some(mapper.readValue(record._2, classOf[Person])) // whole text file returns tuples whose first value if file location and next one if data in file
      } catch {
        case e: Exception => {
          errorRecords += 1
          None
        }
      }
    }
     return results;
  }
  
  def filterJsonDataByCity(records:RDD[Person],mapper:ObjectMapper,errorRecords:Accumulator[Int]):RDD[Person]={
    val results=records.filter(person => person.address.city.equals("mumbai"))   
    return results;
  }
  
  def convertRDDToDF(records:RDD[(String,String)],mapper:ObjectMapper,errorRecords:Accumulator[Int],sparkContext:SparkContext):DataFrame=
  {
    val sqlContext = new SQLContext(sparkContext) 
    import sqlContext.implicits._
    var results = records.map { record =>
      try {
       record._2
       
      } catch {
        case e: Exception => {
          errorRecords += 1
          None
        }
      }
    }
   
    results.foreach { println }.toString();
    val input = sqlContext.jsonRDD(results.map { x => x.toString() })
    //results.foreach { println }.toString()
    return input;
    //input.printSchema()
  }
  
  

}