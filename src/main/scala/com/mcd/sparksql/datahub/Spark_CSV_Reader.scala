package com.mcd.sparksql.datahub
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import com.mcd.sparksql.util.DaasUtil
object Spark_CSV_Reader {
  def main(args: Array[String]) {
    val conf = DaasUtil.getJobConf("SparkCSVReader", "local[*]", "1g", "1g");
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val auctionDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("Input/ebay.csv")
    //auctionDF.printSchema()
    //auctionDF.select("auctionid", "bidder").show

    // How many auctions were held?
    val count = auctionDF.select("auctionid").distinct.count
    println("Distinct items : " + count)
    // How many bids per item?
    auctionDF.groupBy("auctionid", "item").count.sort("auctionid").show

    // What's the min number of bids per item? what's the average? what's the max? 
    auctionDF.groupBy("item", "auctionid").count.agg(min("count"), avg("count"), max("count")).show
    // Get the auctions with closing price > 100
    auctionDF.filter("price > 100").sort("auctionid").show

    // register the DataFrame as a temp table 
    auctionDF.registerTempTable("auction")
    // SQL statements can be run 
    // How many  bids per auction?
    val results = sqlContext.sql("SELECT auctionid, item,  count(bid) as BidCount FROM auction GROUP BY auctionid, item")
    // display dataframe in a tabular format
    results.sort("auctionid").show()

    sqlContext.sql("SELECT auctionid,item, MAX(price) as MaxPrice FROM auction  GROUP BY item,auctionid").sort("auctionid").show()
  }
}