package com.xiaoi.spark.etl_v1

import com.xiaoi.common.HDFSUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Days}
import scopt.OptionParser

/**
  * 数据拆分，将ETL的多天数据根据日期拆分到对应目录中
  * create by zzj 2017-8-17
  */
object DataSplit {

  case class Params(
    beginDate : String = "",
    endDate : String = "",
    inputPath : String = "",
    askoutPath : String = "")

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("etl init") {
      head("etl init")
      opt[String]("beginDate")
        .text(s"起始日期，格式：yyyy-MM-dd")
        .action((x, c) => c.copy(beginDate = x))
      opt[String]("endDate")
        .text("结束日期，格式：yyyy-MM-dd")
        .action((x, c) => c.copy(endDate = x))
      opt[String]("inputPath")
        .text("输入路径")
        .action((x, c) => c.copy(inputPath = x))
      opt[String]("askoutPath")
        .text("ask输出路径")
        .action((x, c) => c.copy(askoutPath = x))

      checkConfig { params =>
        success
      }
    }
    parser.parse(args, defaultParams).map{params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("DataSplit")
    val sc = new SparkContext(conf)
    val beginDate = params.beginDate
    val endDate = params.endDate
    val inputPath = params.inputPath
    val askoutPath = params.askoutPath
    val dateFormat = "yyyy-MM-dd"

    //获取时间计算天数
    val Array(beginYear, beginMonth, beginDay) = beginDate.split("-")
    val beginTime = new DateTime(beginYear.toInt, beginMonth.toInt, beginDay.toInt, 0, 0, 0, 0)
    val Array(endYear, endMonth, endDay) = endDate.split("-")
    val endTime = new DateTime(endYear.toInt, endMonth.toInt, endDay.toInt, 0, 0, 0, 0)
    //计算天数
    val diffDays = Days.daysBetween(beginTime, endTime).getDays

    if(endTime.getMillis < beginTime.getMillis){
      println(s"截至日期【$endDate】应大于起始日期【$beginDate】")
      System.exit(1)
    }

    //读取eltOld数据，并取出日期作为key，（date，log）
    val etlOldData = sc.textFile(inputPath).map(x=>{
      val visitTime = x.split("\\|")(0)
      val visitDate = if (visitTime.length >= 10){
        visitTime.substring(0, 10)
      }else{
        ""
      }
      (visitDate, x)
    }).filter(_._1.length > 0).cache()

    //根据天数，循环过滤数据，存储到对应日期
    for (i <- 0 to diffDays){
      val handleDay = beginTime.plusDays(i).toString(dateFormat)
      val Array(year, month, day) = handleDay.split("-")
      val outputPath = askoutPath+ "/"+ year+ "/"+ month+ "/"+ day

      HDFSUtil.removeOrBackup(outputPath, outputPath)

      etlOldData.filter(_._1.equals(handleDay)).map(_._2)
      .saveAsTextFile(outputPath)
    }

    etlOldData.unpersist()
    sc.stop()
  }
}
