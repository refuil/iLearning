package com.xiaoi.spark.question

import com.xiaoi.common.HDFSUtil
import com.xiaoi.spark.{BaseFun, BaseOffline}
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import scopt.OptionParser

import scala.reflect.internal.MissingRequirementError

/**
  * MainClass for offline recommend
  * created by josh on 201903
  */
object MainBatch extends BaseFun{

  val nDaysRegex = "(\\d{1,2})(days)".r
  val nHoursRegex = "(\\d{1,2})(hours)".r

  def run(params: Params)={
    val spark = SparkSession.builder.
      appName("offline batch").
      getOrCreate()

    val basePkg = "com.xiaoi.spark.offline."
    val inputPath = getTargetPath(params)
    val lines = spark.read.textFile(inputPath)
    import spark.implicits._
//    val lines = spark.createDataset(List(""))
    try {
      import reflect.runtime.universe._
      val rm = runtimeMirror(this.getClass.getClassLoader)
      val module = rm.staticModule(basePkg + params.statType)
      val rec = rm.reflectModule(module).instance.asInstanceOf[BaseOffline]
      rec.process(lines, params)
    } catch {
      case mre: MissingRequirementError => {
        errorIndicator(_)
        mre.printStackTrace()
        throw new IllegalArgumentException("indicator [statType] don't exist！")
      }
      case t: Throwable => {
        t.printStackTrace()
        throw new Exception("compute error")
      }
    }
  }

  def getTargetPath(params: Params) = {
    val date = params.dateLevel match {
      case "hour" => List(DateTime.now().plusHours(-1).toString("yy-MM-dd/HH"))
      case "day" => List(DateTime.now().plusDays(-1).toString("yy-MM-dd/*/*"))
      case nHoursRegex(n, _) => {
        (0 to (n.toInt - 1)).
          map(x => DateTime.now().plusHours(-1 * x).toString("yy-MM-dd/HH")).toList
      }
      case nDaysRegex(n, _) => {
        (0 to (n.toInt - 1)).
          map(x => DateTime.now().plusDays(-1 * x).toString("yy-MM-dd/*/*")).toList

      }
      case value: String => List(value)
    }
    val inputPath = date.map(d => params.inputPath + "/" + d)
      .filter(p => HDFSUtil.exists(p, p)).mkString(",")
    inputPath.split(",").foreach(println)
    inputPath
  }

  def errorIndicator(statType: String) {
    System.err.println(
      s"""
         |批量处理[${statType}]暂不支持,
        """.stripMargin)
    System.exit(1)
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("offlineRecommend") {
      opt[String]("statType")
        .text("(ImportOldData/ImportData/EtlOldData)")
        .action((x, c) => c.copy(statType = x))
      opt[String]("dateLevel")
        .text("分析周期（hour、day或者指定日期格式）")
        .action((x, c) => c.copy(dateLevel = x))
      opt[String]("inputPath")
        .text("输入路径")
        .action((x, c) => c.copy(inputPath = x))
      opt[String]("configIni")
        .text("ini配置文件路径")
        .action((x, c) => c.copy(configIni = x))
      opt[String]("data_type")
        .text("data_type for import")
        .action((x, c) => c.copy(data_type = x))
      opt[String]("ecom_source_path")
        .action((x, c) => c.copy(ecom_source_path=x))
      opt[String]("ecom_old_start_date")
        .action((x, c) => c.copy(ecom_old_start_date=x))
      opt[String]("ecom_old_end_date")
        .action((x, c) => c.copy(ecom_old_end_date=x))
      opt[String]("ecom_save_path")
        .action((x, c) => c.copy(ecom_save_path=x))
      opt[String]("cleaned_path")
        .action((x, c) => c.copy(cleaned_path=x))
      opt[String]("match_result")
        .action((x,c)=> c.copy(match_result = x))
      opt[Int]("days")
        .action((x, c) => c.copy(days=x))
      //redis params
      opt[String]("redisHost")
        .action((x,c)=> c.copy(redis_host = x))
      opt[Int]("redisPort")
        .action((x,c)=> c.copy(redis_port = x))
      opt[String]("redisPass")
        .action((x,c)=> c.copy(redis_pass = x))
      //params for ecomTrans
      opt[String]("data_cleaned_hasVIPNO_output_path")
        .action((x, c) => c.copy(data_cleaned_hasVIPNO_output_path = x))
      opt[String]("data_cleaned_noVIPNO_output_path")
        .action((x, c) => c.copy(data_cleaned_noVIPNO_output_path = x))
      opt[String]("data_cleaned_hasVIPNO_input_path")
        .action((x, c) => c.copy(data_cleaned_hasVIPNO_input_path = x))
      opt[String]("data_cleaned_noVIPNO_input_path")
        .action((x, c) => c.copy(data_cleaned_noVIPNO_input_path = x))
      opt[String]("pluname2id_output_path")
        .action((x, c) => c.copy(pluname2id_output_path = x))
      opt[String]("vipno2id_output_path")
        .action((x, c) => c.copy(vipno2id_output_path = x))
      opt[String]("dptno_item_output_path")
        .action((x, c) => c.copy(dptno_item_output_path = x))
      opt[String]("data_14cols_1_output_path")
        .action((x, c) => c.copy(data_14cols_1_output_path = x))
      opt[String]("data_14cols_2_output_path")
        .action((x, c) => c.copy(data_14cols_2_output_path = x))
      opt[String]("data_2cols_output_path")
        .action((x, c) => c.copy(data_2cols_output_path = x))
      opt[String]("data_rating_output_path")
        .action((x,c) => c.copy(data_rating_output_path = x))
      opt[String]("data_mf_output_path")
        .action((x,c) => c.copy(data_mf_output_path = x))
      //match pools
      opt[String]("i_sim_input_path")
        .action((x,c) => c.copy(i_sim_input_path = x))
    }

    parser.parse(args, defaultParams).map {
      params => run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }


  case class Params(statType: String = "ImportOldData",
                    dateLevel: String = "hour",
                    inputPath: String = "",
                    configIni: String = "",
                    data_type: String = "",
                    ecom_source_path: String="",
                    ecom_old_start_date: String="",
                    ecom_old_end_date: String="",
                    ecom_save_path: String="",
                    days:Int=30,
                    cleaned_path: String="",
                    match_result:String = "",
                    //redis params
                    redis_host: String = "localhost",
                    redis_port: Int = 6379,
                    redis_pass: String = "dis2019",
                    //for ecomTrans
                    data_cleaned_hasVIPNO_output_path: String = "",
                    data_cleaned_noVIPNO_output_path: String = "",
                    data_cleaned_hasVIPNO_input_path: String = "",
                    data_cleaned_noVIPNO_input_path: String = "",
                    pluname2id_output_path:String = "",
                    vipno2id_output_path:String = "",
                    dptno_item_output_path:String = "",
                    data_14cols_1_output_path:String = "",
                    data_14cols_2_output_path:String = "",
                    data_2cols_output_path:String = "",
                    data_rating_output_path: String = "",
                    data_mf_output_path: String = "",
                    //online match pool
                    i_sim_input_path: String = ""
                   )


}
