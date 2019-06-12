package com.xiaoi.spark.etl

import java.util.Properties

import com.xiaoi.common.{DateUtil, HDFSUtil}
import com.xiaoi.spark.BaseOffline
import com.xiaoi.spark.MainBatch.Params
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.ini4j.Ini
import com.xiaoi.constant.Constants._
import org.joda.time.DateTime
import scala.collection.mutable

object ImportByDay extends BaseOffline {

  /**
    * 重写父类方法
    * @param spark
    * @param params
    */
  def process(spark: SparkSession, params: Params) = {
    logging(s"this is import data")

    val ini = loadIni(params)
    val importType = params.data_type
    if(importType.contains("retail")){
      //    simulateRetailSource(lines.sparkSession)
      readRetailDB(ini, importType, spark)
    }else if(importType.contains("ecom")){
      readEComFromCsv(params, spark)
    }

  }

  /**
    * method后期要与saveByDay合并，需要统一time SLDAT field
    * @param params
    * @param spark
    */
  def readEComFromCsv(params: Params, spark: SparkSession)={
    val importType = params.data_type
    val ecomSavePath = params.ecom_save_path
    if(importType == "ecom_day"){
      val startTime = DateUtil.getDateBefore(1)
      val endTime = DateUtil.getDate()
      val csv = readActionCsv(spark, params.ecom_source_path).
        na.fill(value="0",cols=Array("model_id","type_id","cate","brand"))
      val todayPath = params.ecom_save_path+"/"+ (new DateTime()).
        plusDays(-1).toString("yyyy/MM/dd")

      logger.info(s"e-com recom import userAction $startTime. $todayPath")
      HDFSUtil.removeDir(todayPath)
//      ecomSaveByDay(spark,csv, startTime,endTime,todayPath)
      import spark.implicits._
      csv.filter($"time" >= startTime && $"time" <= endTime).
        rdd.map(x => x.mkString("|")).saveAsTextFile(todayPath)

    }else if(importType == "ecom_old"){
      val startDate = params.ecom_old_start_date
      val endDate = params.ecom_old_end_date
      val csv = readActionCsv(spark, params.ecom_source_path).
          na.fill(value="0",cols=Array("model_id","type_id","cate","brand"))
      logger.info(s"ecom import user_action from $startDate to $endDate")
      HDFSUtil.removeDir(ecomSavePath)
      ecomSaveByDay(spark, csv,
        startDate, endDate,
        ecomSavePath)
    }
  }

  def ecomSaveByDay(spark: SparkSession, data:DataFrame,
                starttime: String, endtime:String,
                target_dir:String) = {
    import spark.implicits._
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val patternToStr = (pa:String) => DateTime.parse(pa.take(10)).toString("yyyy-MM-dd")
    var start = patternToStr(starttime)
    val end = patternToStr(endtime)
    while(start <= end){
      val dayEnd = DateTime.parse(start.take(10)).plusDays(1).toString(pattern)
      val dayStart = DateTime.parse(start).toString(pattern)
      logging(s"Save file: ${start} time from ${dayStart} to ${dayEnd}")
      data.filter($"time" >= dayStart && $"time" <= dayEnd).rdd
        .map(x => x.mkString("|"))
        .saveAsTextFile(target_dir + "/" + DateTime.parse(start).toString("yyyy/MM/dd"))
      start = DateTime.parse(start).plusDays(1).toString("yyyy-MM-dd")
    }
  }

  def readRetailDB(ini: Ini, importType: String, spark: SparkSession)={
    val host = ini.get(DATABASE, "host")
    val user = ini.get(DATABASE, "user")
    val password = ini.get(DATABASE, "password")
    val database = ini.get(DATABASE, "db")
    val port = ini.get(DATABASE, "port")
    val database_type = ini.get(DATABASE, "database_type")
    val table_prefix = ini.get("old_import", "table_prefix")
    val target_dir = ini.get("old_import", "target_dir")

    val driver = if (database_type == "mysql") "com.mysql.jdbc.Driver"
      else "oracle.jdbc.OracleDriver"
    val dburl = if (database_type == "mysql")
      "jdbc:mysql://%s:%s/%s".format(host, port, database)
      else "jdbc:oracle:thin:@%s:%s:%s".format(host, port, database)

//    UID,SLDAT,VIPNO,PRODNO,PLUNAME,DPTNO,DPTNAME,BNDNO,BNDNAME,
    // QTY,AMT,VIP_GENDER,VIP_BIRTHDAY,VIP_CREATED_DATE

    val pattern = "yyyy-MM-dd HH:mm:ss"
    if (importType == "retail_import_day") {
      logging(s"Import data every day...")

    } else if (importType == "retail_old_import") {
      val source_type = ini.get(OLD_IMPORT, "source_type")
      if (source_type == "db") {
        val begin_date = ini.get(OLD_IMPORT, "begin_date")
        val end_date = ini.get(OLD_IMPORT, "end_date")

        var beginDT = DateTime.parse(begin_date)
        val endDT = DateTime.parse(end_date)
        val beginStr = beginDT.toString("yyyyMM")
        val endStr = endDT.toString("yyyyMM")

        //根据时间段划分表名,开始时间,结束时间. 以月为单位划分
        while (beginDT.toString("yyyyMM") <= endStr) {

          val table = table_prefix + beginDT.toString("yyyyMM")
          val starttime = if (beginDT.toString("yyyyMM") == beginStr) beginDT.toString(pattern)
            else beginDT.dayOfMonth.withMinimumValue().toString(pattern)
          val endtime = if (beginDT.toString("yyyyMM") == endStr) endDT.toString(pattern)
            else beginDT.dayOfMonth.withMaximumValue().toString(pattern)

          val options = new mutable.HashMap[String, String]
          options.put("url", dburl)
          options.put("driver", driver)
          options.put("dbtable", table)
          options.put("user", user)
          options.put("password", password)
          val jdbcDF = spark.read.format("jdbc").options(options).load()

          saveByDay(spark,jdbcDF,starttime,endtime,target_dir)
          beginDT = beginDT.plusMonths(1)
        }
      }else if(source_type == "file"){
        logging(s"reading big file...")
      }

    }
  }

  /**
    * save by day
    * yyyy/MM/dd 00:00:00
    * @param starttime
    * @param endtime
    */
  def saveByDay(spark: SparkSession, data:DataFrame,
                starttime: String, endtime:String,
                target_dir:String) = {
    import spark.implicits._
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val patternToStr = (pa:String) => DateTime.parse(pa.take(10)).toString("yyyy-MM-dd")
    var start = patternToStr(starttime)
    val end = patternToStr(endtime)
    while(start <= end){
      val dayEnd = DateTime.parse(start.take(10)).plusDays(1).toString(pattern)
      val dayStart = DateTime.parse(start).toString(pattern)
      logging(s"Save file: ${start} time from ${dayStart} to ${dayEnd}")
      data.filter($"SLDAT" >= dayStart && $"SLDAT" <= dayEnd).rdd
        .map(x => x.mkString("|"))
        .saveAsTextFile(target_dir + "/" + DateTime.parse(start).toString("yyyy/MM/dd"))
      start = DateTime.parse(start).plusDays(1).toString("yyyy-MM-dd")
    }
  }

  /**
    * 将之前的文本数据导入数据库模拟真实业务情况
    * @param spark
    */
  def simulateRetailSource(spark: SparkSession)={
    val sourcePath = "file:///xiaoi/crm/data/step_1/exception_handled"
    import spark.implicits._
    val source = spark.read.textFile(sourcePath).map(x=> x.split("\\|")).rdd

    val begin_date = "2018-09"
    val end_date = "2019-02"
    var beginDT = DateTime.parse(begin_date)
    val endDT = DateTime.parse(end_date)
    val beginStr = beginDT.toString("yyyyMM")
    val endStr = endDT.toString("yyyyMM")

    while (beginDT.toString("yyyyMM") <= endStr) {
      val tableName = "log_user_action_%s".format(beginDT.toString("yyyyMM"))
      val actionDF = source.filter(x => x(1).contains(beginDT.minusMonths(16).toString("yyyy-MM")))
        .filter(x=> x.size >= 58)
        .map(x => Row(x(0),
          DateTime.parse(x(1).take(10)).plusMonths(16).toString("yyyy-MM-dd")+x(1).drop(10),
          x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10),
          x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20),
          x(21), x(22), x(23), x(24), x(25), x(26), x(27), x(28), x(29), x(30),
          x(31), x(32), x(33), x(34), x(35), x(36), x(37), x(38), x(39), x(40),
          x(41), x(42), x(43), x(44), x(45), x(46), x(47), x(48), x(49), x(50),
          x(51), x(52), x(53), x(54), x(55), x(56), x(57), x(58)))

      val schema = StructType(List(
        StructField("UID", StringType),
        StructField("SLDAT", StringType),
        StructField("PNO", StringType),
        StructField("VIPNO", StringType),
        StructField("PRODNO", StringType),
        StructField("BCD", StringType),
        StructField("PLUNAME", StringType),
        StructField("SPEC", StringType),
        StructField("PKUNIT", StringType),
        StructField("DPTNO", StringType),
        StructField("DPTNAME", StringType),
        StructField("BNDNO", StringType),
        StructField("BNDNAME", StringType),
        StructField("QTY", StringType),
        StructField("AMT", StringType),
        StructField("DISAMT", StringType),
        StructField("ISMMX", StringType),
        StructField("MTYPE", StringType),
        StructField("MDOCNO", StringType),
        StructField("ISDEL", StringType),
        StructField("SITE_NO", StringType),
        StructField("SITE_NAME", StringType),
        StructField("TRADE_ID", StringType),
        StructField("TENDER_01", StringType),
        StructField("TENDER_0401", StringType),
        StructField("TENDER_0111", StringType),
        StructField("TENDER_0201", StringType),
        StructField("TENDER_0202", StringType),
        StructField("TENDER_0203", StringType),
        StructField("TENDER_0204", StringType),
        StructField("TENDER_0301", StringType),
        StructField("TENDER_0302", StringType),
        StructField("TENDER_0303", StringType),
        StructField("TENDER_0510", StringType),
        StructField("TENDER_0703", StringType),
        StructField("TENDER_0704", StringType),
        StructField("TENDER_0707", StringType),
        StructField("TENDER_0708", StringType),
        StructField("TENDER_0801", StringType),
        StructField("TENDER_0802", StringType),
        StructField("TENDER_0803", StringType),
        StructField("TENDER_0805", StringType),
        StructField("TENDER_0806", StringType),
        StructField("TENDER_0807", StringType),
        StructField("TENDER_0808", StringType),
        StructField("TENDER_0810", StringType),
        StructField("TENDER_0811", StringType),
        StructField("VIP_GENDER", StringType),
        StructField("VIP_BIRTHDAY", StringType),
        StructField("VIP_PROVINCE_NAME", StringType),
        StructField("VIP_CITY_NAME", StringType),
        StructField("VIP_DISTRICT_NAME", StringType),
        StructField("VIP_POST_CODE", StringType),
        StructField("VIP_STATUS", StringType),
        StructField("VIP_SOURCE_SITE_NO", StringType),
        StructField("VIP_SOURCE_SITE_NAME", StringType),
        StructField("VIP_BELONG_SITE_NO", StringType),
        StructField("VIP_BELONG_SITE_NAME", StringType),
        StructField("VIP_CREATED_DATE", StringType)))
      val tableDF = spark.createDataFrame(actionDF, schema)

      val driver = "com.mysql.jdbc.Driver"
      val options = new Properties()
      options.put("driver", driver)
      options.put("user", "meizu")
      options.put("password", "123")
      //    val dbUrl = "jdbc:mysql://122.226.240.158:3306/rec_demo"
      //    DBUtil.update(dbUrl, "meizu", "123", tableDF,"user_action")
      tableDF.write.mode("append")
        .jdbc("jdbc:mysql://122.226.240.158:3306/rec_demo", "rec_demo.%s".format(tableName), options)
      beginDT = beginDT.plusMonths(1)
    }
  }
}
