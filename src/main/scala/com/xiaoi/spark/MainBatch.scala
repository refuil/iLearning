package com.xiaoi.spark

import org.apache.spark.sql.SparkSession
import scopt.OptionParser

import scala.reflect.internal.MissingRequirementError

/**
  * MainClass for offline recommend
  * created by josh on 201903
  */
object MainBatch extends BaseFun{

  def run(params: Params)={
    val spark = SparkSession.builder.
      appName("offline batch").
      getOrCreate()

    val basePkg = "com.xiaoi.spark.offline."
    try {
      import reflect.runtime.universe._
      val rm = runtimeMirror(this.getClass.getClassLoader)
      val module = rm.staticModule(basePkg + params.statType)
      val rec = rm.reflectModule(module).instance.asInstanceOf[BaseOffline]
      rec.process(spark, params)
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
      opt[Int]("days")
        .action((x, c) => c.copy(days=x))
      opt[String]("ignoredQuesPath")
        .action((x, c) => c.copy(ignoredQuesPath = x))

    }

    parser.parse(args, defaultParams).map {
      params => run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }


  case class Params(statType: String = "ImportOldData",
                    dateLevel: String = "day",
                    inputPath: String = "/experiment/josh/data/guangda/data/ask",
                    configIni: String = "",
                    data_type: String = "",
                    days:Int=30,
                    ecom_source_path: String="",
                    ecom_old_start_date: String="",
                    ecom_old_end_date: String="",
                    ecom_save_path: String="",
                    //question
                    ansTypes: String = "0,11",
                    unclearAnswerPath: String = "/production/guangda/config/unclear_answer",
                    dfsUri: String = "",
                    ignoredQuesPath: String = "/production/guangda/config",
                    minQuesLen: Int = 2,
                    maxQuesLen: Int = 40,
                    similarity: Double = 0.5,
                    shortQuesLen: Int = 5,
                    highSimilarity: Double = 0.6
                   )


}
