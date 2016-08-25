/*
 * Copyright 2015 McDonalds All Rights Reserved.
 *
 * This software contains valuable trade secrets and proprietary information of McDonalds and is protected by law. It
 * may not be copied or distributed in any form or medium, disclosed to third parties, reverse engineered or used in any
 * manner without prior written authorization from McDonalds.
 */
package com.mcd.sparksql.datahub

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.log4j.Logger

/**
 * FileSystem object returns a FileSystem for a given path on HDFS, S3 or Local
 */
object FileSystemDaas{
  private val logger = Logger.getLogger(this.getClass)

  /**
   * The from method takes in a file path and return the FileSystem for the specified path.
   *
   * {{{
   *    val fs = FileSystem from "/tmp/test.dat"
   *    val fs = FileSystem from "s3a://daas/data.dat"
   *    val fs = FileSystem from "hdfs://10.205.80.73:9000/ec2-user/data.dat"
   * }}}
   *
   * @param file_path File Path
   * @return FileSystem
   */
  def from(file_path: String): FileSystem = {

    logger.debug("Start of getFSObject method for " + file_path)
    val configuration: Configuration = new Configuration
    val awsAccessKeyId = sys.env.getOrElse("AWS_ACCESS_KEY_ID", "AKIAJ2HX2VMDURPZGJGQ")
    val awsSecretAccessKey = sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", "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")

    logger.info("AWS_ACCESS_KEY_ID = " + awsAccessKeyId)
    logger.info("AWS_SECRET_ACCESS_KEY = " + awsSecretAccessKey)
    
    if (file_path.startsWith("hdfs"))
    {
      val hdfsCoreSitePath = new Path("/etc/hadoop/conf/core-site.xml")
      val hdfsHDFSSitePath = new Path("/etc/hadoop/conf/hdfs-site.xml")
      logger.info("inside hdfs core site")
      configuration.addResource(hdfsCoreSitePath)
      configuration.addResource(hdfsHDFSSitePath)
    }
    if(file_path.startsWith("s3://"))
    {
    if (awsAccessKeyId != "" && awsSecretAccessKey != "") {
      configuration.set("fs.s3n.awsAccessKeyId", awsAccessKeyId)
      configuration.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)
      configuration.set("fs.s3a.access.key", awsAccessKeyId)
      configuration.set("fs.s3a.secret.key", awsSecretAccessKey)
      configuration.set("fs.s3.awsAccessKeyId", awsAccessKeyId)
      configuration.set("fs.s3.awsSecretAccessKey", awsSecretAccessKey)
    }
    
    configuration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    configuration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    configuration.set("fs.s3a.server-side-encryption-algorithm", "AES256")
    }
    //configuration.setBoolean("fs.hdfs.impl.disable.cache", true);

    logger.info("exiting getFSObject")

    var fs = org.apache.hadoop.fs.FileSystem.newInstance(new URI(file_path), configuration)
    fs=org.apache.hadoop.fs.FileSystem.get(configuration)
    logger.info("exiting getFSObject")
    val pt = new Path(file_path)
    logger.info("exiting getFSObject")
    fs
  }
}
