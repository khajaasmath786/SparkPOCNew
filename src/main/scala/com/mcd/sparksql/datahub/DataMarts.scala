package com.mcd.sparksql.datahub
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
//import com.mcd.sparksql.datahub.CalDt
object DataMarts{
   def main(args: Array[String]): Unit = {
   if (args.length < 0) {
      println("Please specify <JobName> <Master> <Exec Memort> <Driver Memory>")
      System.exit(-1)
    }
    //val mapProps = DaasUtil.getConfig("Daas.properties")
    val props = DaasUtil.get(args(0))
    val master=props.getProperty("Master")
    println(master)
    //val driverMemory=props.getProperty("Driver.Memory")
    //val executorMemory=props.getProperty("Executor.Memory")
    val jobName="DataMartRefreshProcess"
    val conf = DaasUtil.getJobConf(jobName, master);
    val sparkContext = new SparkContext(conf)
    //val sqlContext = new HiveContext(sparkContext)
    val logger = LoggerFactory.getLogger("DataMarts")

    val sqlContext = new HiveContext(sparkContext);
    
    val refreshDays=props.getProperty( "REFRESH_DAYS")
    
    val DLY_DYPT_POS_TRN_NP_POPULATE=props.getProperty( "DLY_DYPT_POS_TRN_NP_POPULATE")
    val DLY_TM_SEG_POS_TRN_OFFR_NP_POPULATE=props.getProperty( "DLY_TM_SEG_POS_TRN_OFFR_NP_POPULATE")
    val POS_TRN_OFFR_NP_POPULATE=props.getProperty("POS_TRN_OFFR_NP_POPULATE")
    
    
    val refreshStartDate=refreshTimePeriod(refreshDays, sqlContext);   
    
    
    populateDlyDayPartPOSTrnOffer(sqlContext,DLY_DYPT_POS_TRN_NP_POPULATE,refreshDays);
    populateDlyTimeSegmentPOSTrnOffer(sqlContext,DLY_TM_SEG_POS_TRN_OFFR_NP_POPULATE,refreshDays);
    populatePOSTrnOffer(sqlContext,POS_TRN_OFFR_NP_POPULATE,refreshDays);

   }
   
    def populateDlyDayPartPOSTrnOffer(sqlContext:SQLContext,dlyDayPartPOSTrnOfferQuery:String,refreshStartDate:String): Unit = {
      val query=dlyDayPartPOSTrnOfferQuery.replace("${hivevar:MYDATE}", refreshStartDate);
      sqlContext.sql(query);
         
    }
    def populateDlyTimeSegmentPOSTrnOffer(sqlContext:SQLContext,dlyTmSegPOSTrnOfferQuery:String,refreshStartDate:String): Unit = {
      val query=dlyTmSegPOSTrnOfferQuery.replace("${hivevar:MYDATE}", refreshStartDate);
      sqlContext.sql(query);
      
    }
    def populatePOSTrnOffer(sqlContext:SQLContext,posTrnOfferQuery:String,refreshStartDate:String): Unit = {
      val query=posTrnOfferQuery.replace("${hivevar:MYDATE}", refreshStartDate);
      sqlContext.sql(query);
      
    }
        
    def refreshTimePeriod(refreshDays:String,sqlContext:SQLContext):String={
      val today = new Date();
		  val cal = new GregorianCalendar();
		  cal.setTime(today);
		  val rtp= Integer.parseInt(refreshDays);
		  cal.add(Calendar.DATE, -rtp );
		  val  today120 = cal.getTime();
		//cal.add(Calendar.DATE, -today120.getDay());// Logic for calculating the last sunday before the date
		  val df3 = new SimpleDateFormat("yyyy-MM-dd");
		  df3.format(cal.getTime()).toString();
    }
   
}
