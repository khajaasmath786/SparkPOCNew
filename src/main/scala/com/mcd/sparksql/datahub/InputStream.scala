/*
 * Copyright 2016 McDonalds All Rights Reserved.
 *
 * This software contains valuable trade secrets and proprietary information of McDonalds and is protected by law. It
 * may not be copied or distributed in any form or medium, disclosed to third parties, reverse engineered or used in any
 * manner without prior written authorization from McDonalds.
 */
package com.mcd.sparksql.datahub

import java.io.{BufferedReader, InputStream, InputStreamReader}
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import java.io.FileReader

/**
 * InputStream object reads a file on HDFS, S3 or Local and returns an java.io.InputStream
 */
object InputStream {
  private val logger = Logger.getLogger(this.getClass)
  implicit class InputStreamHelper(is: java.io.InputStream){
    /**
     * This is an override method on java.io.InputStream. It converts java.io.InputStream to a String
     *
     * {{{
     *    val inStream: InputStream = InputStream from "/tmp/test.dat"
     *    val data: String = inStream.show
     * }}}
     *
     * @return String
     */
    def show: String = inputStreamToString(is)
  }

  /**
   * The from method takes in a file path and returns an java.io.InputStream containing the file contents.
   *
   * {{{
   *    val inStream: InputStream = InputStream from "/tmp/test.dat"
   *    val inStream: InputStream = InputStream from "s3a://daas/mcd/data.dat"
   *    val inStream: InputStream = InputStream from "hdfs://10.205.80.73:9000/ec2-user/data.dat"
   * }}}
   *
   * @param fname File to read
   * @return java.io.InputStream
   */
  def from(fname: String):InputStream = {
    logger.info("Start of getStream method for " + fname)
    val fs = FileSystemDaas from fname
     logger.info("File System"+fs)
    try {
      
      val p = new Path(fname)
      logger.info("File Path --> "+p + "  fs "+ fs)
      //var br= new BufferedReader(new FileReader(fname))
      println("File Path --> "+p + "  fs "+ fs)
      fs.open(p)
    } catch {
      case e: Exception => {
        logger.warn("Error opening file from InputStream: " + fname)
        throw new Exception
      }
    } finally {
     // fs.close()
    }
  }

  private def inputStreamToString(is: InputStream) = {
    val rd: BufferedReader = new BufferedReader(new InputStreamReader(is, "UTF-8"))
    val builder = new StringBuilder()
    try {
      var line = Option(rd.readLine)
      while (line.isDefined) {
        builder.append(line.get + "\n")
        line = Option(rd.readLine)
      }
    } finally {
      rd.close()
    }
    builder.toString()
  }
}
