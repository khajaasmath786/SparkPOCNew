package itvarsity.class10

import java.io.File
import scala.collection.mutable.HashMap
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD


object WordCountScala{
  def main(args: Array[String]): Unit = {
    var inputFileLocation = "/user/cloudera/sqoop_import/departments"; //or "hdfs://quickstart.cloudera:8022/user/cloudera/sqoop_import/departments"
    var outputLocation = "/user/cloudera/scalaspark/departmentsSeqWithKey"; //or "hdfs://quickstart.cloudera:8022/user/cloudera/scalaspark/departmentsTesting"
    var appName = "SequenceFileOutputFormatWithKey"
    var master = "local[*]" //"spark://192.168.56.1:7077"

    if (args.length >= 4) {
      println("Inside arguments list")
      inputFileLocation = args(0);
      outputLocation = args(1);
      appName = args(2)
      //master = args(3)
    }
    
  
    var errorsMap = new HashMap[String, String]()
    
    FileUtils.deleteDirectory(new File(outputLocation));
    println("master -->"+master)
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf);


    val lineRdd = sc.textFile(inputFileLocation).cache() //cache will make RDD not to recompile every time we use RDD in next steps.

    //*******************************************************************************
    
    
     val mapline = sc.textFile("Input/categories/Errors").cache()
   

     val flatMapRdd=mapline.flatMap { line => line.split(" ") }//.foreach { x =>{errorsMap.put(x.toString().trim(), x.toString().trim())} }
     val myHasmMapBC = sc.broadcast(flatMapRdd.map { case (x) => (x,x) }.collectAsMap)
     println(myHasmMapBC.value.size)
     //https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-broadcast.html
     
     //*******************************************************************************
     
     
    //Flat map converts the lines into List of words instead of list of listofwords.
    val wordsRdd = lineRdd.flatMap(line => line.split(" "));
    println("---------------")
    wordsRdd.collect().foreach(println);
    println("****************")
    
    //Map converts the words into tuples here. Ex (Asmath,1)
    val wordsAggRdd:RDD[(String,Int)] = wordsRdd.filter { word => !myHasmMapBC.value.contains(word)}.map { word => (word, 1) }
   
   // val wordsAggRdd = wordsRdd.map { word => (word.replaceAll(",", ""), 1) }
     wordsAggRdd.collect().foreach(println);
    
     /*ReducebyKey you have to pass accumulator. First value of accumulator is 0 and then it keeps on adding.
     Above case accumulator starts with 0 for that key and keeps on adding with the value(1 in word count case)*/

    val wordCount = wordsAggRdd.reduceByKey { (acc, wordbyKey) => acc + wordbyKey };
    wordCount.collect().foreach(println);

    wordCount.saveAsTextFile(outputLocation);
    
    
    

    /*    
    
    //ClouderaVM --> spark-submit --class itvarsity.class8.ReadHdfsFileAndSaveHdfs01 --master local sparkexamples_2.10-1.0.jar
    
    //Windows -> spark-submit --class itvarsity.class8.WriteObjectFile --master local C:\Users\mc41946\git\MySparkExamples\sparkcode\target\scala-2.10\sparkexamples_2.10-1.0.jar
     * spark-submit --class itvarsity.class8.ReadHdfsFileAndSaveHdfs01 --master local C:\Users\mc41946\git\MySparkExamples\sparkcode\target\scala-2.10\sparkexamples_2.10-1.0.jar
     */
  }
}