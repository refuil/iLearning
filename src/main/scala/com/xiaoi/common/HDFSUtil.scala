package com.xiaoi.common

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.hadoop.fs.FileSystem

/**
 * Created by ligz on 15/9/11.
 * Modified History 增加dfsURI为空的判断，当dfsURI为空读取当前系统HDFS配置
 */
object HDFSUtil {

  def removeDir(dir : String): Unit = {
    val dfsURI = dir
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = if("".equals(dfsURI)) FileSystem.get(hadoopConf)
    else FileSystem.get(new java.net.URI(dfsURI), hadoopConf)
    // Delete the existing path, ignore any exceptions thrown if the path doesn't exist
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(dir), true)
    } catch {
      case _: Throwable => {}
    }
  }

  def removeDir(dfsURI : String, dir : String): Unit = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = if("".equals(dfsURI)) FileSystem.get(hadoopConf)
            else FileSystem.get(new java.net.URI(dfsURI), hadoopConf)
    // Delete the existing path, ignore any exceptions thrown if the path doesn't exist
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(dir), true)
    } catch {
      case _: Throwable => {}
    }
  }

  def makeDir(dfsURI : String, dir : String): Unit = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = if("".equals(dfsURI)) FileSystem.get(hadoopConf) else FileSystem.get(new java.net.URI(dfsURI), hadoopConf)
    // Create the path, if the path doesn't exist
    try {
      if(!exists(dfsURI, dir)){
        hdfs.mkdirs(new org.apache.hadoop.fs.Path(dir))
      }
    } catch {
      case _: Throwable => {}
    }
  }

  def backupDir(dfsURI : String, dir : String): Unit = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = if("".equals(dfsURI)) FileSystem.get(hadoopConf) else FileSystem.get(new java.net.URI(dfsURI), hadoopConf)
    val df = new SimpleDateFormat("YYYY-MM-dd_hh_mm_ss")
    val bakPath = dir + "_" +  df.format(new Date()) + "_bak"
    try {
      hdfs.rename(new org.apache.hadoop.fs.Path(dir), new org.apache.hadoop.fs.Path(bakPath))
    } catch {
      case _ : Throwable => {}
    }
  }

  def backupDir(dfsURI : String, fromDir : String, hisDir : String): Unit = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = if("".equals(dfsURI)) FileSystem.get(hadoopConf) else FileSystem.get(new java.net.URI(dfsURI), hadoopConf)
    try {
      val df = new SimpleDateFormat("YYYY-MM-dd_hh_mm_ss")
      val bakPath = hisDir + "/"+ df.format(new Date())
      hdfs.rename(new org.apache.hadoop.fs.Path(fromDir), new org.apache.hadoop.fs.Path(bakPath))
    } catch {
      case _ : Throwable => {}
    }
  }

  def exists(dfsURI : String, dir : String): Boolean = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = if("".equals(dfsURI)) FileSystem.get(hadoopConf) else FileSystem.get(new java.net.URI(dfsURI), hadoopConf)
    hdfs.exists(new org.apache.hadoop.fs.Path(dir))
  }

  def removeOrBackup(removeMode: Boolean, dfsURI : String, dir : String): Unit = {
    if (exists(dfsURI, dir)) {
      if (removeMode) {
        HDFSUtil.removeDir(dfsURI, dir)
      } else {
        HDFSUtil.backupDir(dfsURI, dir)
      }
    }
  }

  def removeOrBackup(dfsURI : String, dir : String): Unit = {
    removeOrBackup(true, dfsURI, dir)
  }

  def main(args: Array[String]) {
//    backupDir("hdfs://davinci08:9000", "hdfs://davinci08:9000/tmp/ligz_test")
    removeDir("hdfs://davinci08:9000", "hdfs://davinci08:9000/tmp/ligz_test_2015-09-11_bak")
  }

}
