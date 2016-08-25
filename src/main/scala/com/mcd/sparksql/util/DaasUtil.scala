package com.mcd.sparksql.util
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType }
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.mcd.sparksql.datahub._
import scala.io.Source
import java.util.Properties




object DaasUtil {
  def getJobConf(jobName: String, masters: String, execMemory: String="2g", driverMemory: String="2g") : SparkConf= {
    new SparkConf().setAppName(jobName).setMaster(masters).set("spark.executor.memory", execMemory).set("spark.driver.memory", driverMemory);
  }
  
  def getJobConf(jobName: String, masters: String) : SparkConf= {
    new SparkConf().setAppName(jobName).setMaster(masters);
  }

  def getJobConf(jobName: String, masters: String, execMemory: String): SparkConf ={
    new SparkConf().setAppName(jobName).setMaster(masters).set("spark.executor.memory", execMemory)
  }
  
  def getJobConfForCassandra(jobName: String, masters: String, execMemory: String, driverMemory: String,cassandraHost:String): SparkConf ={
    new SparkConf(true).set("spark.cassandra.connection.host", cassandraHost).setAppName(jobName).setMaster(masters).set("spark.executor.memory", execMemory).set("spark.driver.memory", driverMemory);
  }
  
  def getConfig(filePath: String):Map[String,String]= {
    Source.fromFile(filePath).getLines().filter(line => line.contains("=")).map{ line =>
      println("Values from property file --> "+line)
      val tokens = line.split("=")
      ( tokens(0) -> tokens(1))
    }.toMap
  }
  
  def get(filePath: String): Properties = {
    val stream = InputStream from filePath
    val prop: Properties = new Properties
    try {
      prop.load(stream)
    } catch {
      case e:NullPointerException => throw new Exception
    }
    prop
  }
  
  
  def getValue(mapProperties:Map[String,String],attribute:String):String={
    val propertyValue=mapProperties(attribute);
    return propertyValue
    
  }

}