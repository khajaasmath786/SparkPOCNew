package com.mcd.sparksql.datahub

import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

object HDFSScala{
  private val conf = new Configuration()
  private val hdfsCoreSitePath = new Path("/etc/hadoop/conf/core-site.xml")
  private val hdfsHDFSSitePath = new Path("/etc/hadoop/conf/hdfs-site.xml") 
  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)
  val fileSystem = org.apache.hadoop.fs.FileSystem.get(conf)
  def main(args: Array[String]): Unit = {
   
    removeFile(args(0))
  }


  

   

  def saveFile(filepath: String): Unit = {
    val file = new File(filepath)
    val out = fileSystem.create(new Path(file.getName))
    val in = new BufferedInputStream(new FileInputStream(file))
    var b = new Array[Byte](1024)
    var numBytes = in.read(b)
    while (numBytes > 0) {
      out.write(b, 0, numBytes)
      numBytes = in.read(b)
    }
    in.close()
    out.close()
  }

  def removeFile(filename: String): Boolean = {
    val path = new Path(filename)
    println("removing file")
    fileSystem.delete(path, true)
  }

  def getFile(filename: String): InputStream = {
    val path = new Path(filename)
    fileSystem.open(path)
  }

  def createFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }
}