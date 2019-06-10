package com.xiaoi.spark.question

import com.xiaoi.common.HDFSUtil
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
  * 合并未回答问题1天，7天，30天各类型统计数量
  *
  * Created by zzj on 9/5/17.
  * Modified History 删除相似问统计，因为插入主表时没有使用
  */
object MergeUnansCnt {

  /**
    * 合并未回答问题1天，7天，30天各类型统计数量
    * @param params
    */
  def run(params: Params): Unit = {
    val onedayInput = params.stepCntInput+"/oneday"
    val weekInput = params.stepCntInput+"/sevendays"
    val monthInput = params.stepCntInput+"/onemonth"
    val outputPath = params.stepCntOut
    val dfsUri = params.dfsUri
    val removeMode = params.removeMode

    val conf = new SparkConf().setAppName("RecommendFaq")
    val sc = new SparkContext(conf)

    //删除输出路径
    HDFSUtil.removeOrBackup(removeMode,dfsUri,outputPath)

    // 昨日未回答问题统计(ques, cnt0, cnt11)
    val oneday_data = sc.textFile(onedayInput).map(x => {
      val fields = x.split("\\|")
      (fields(0), (fields(1).toInt, fields(2).toInt))
    }).cache()
    // 7天内未回答问题统计(ques, cnt0, cnt11)
    val week_data = sc.textFile(weekInput).map(x => {
      val fields = x.split("\\|")
      (fields(0), (fields(1).toInt, fields(2).toInt))
    }).cache()
    // 30天内未回答问题统计(ques, cnt0, cnt11)
    val month_data = sc.textFile(monthInput).map(x => {
      val fields = x.split("\\|")
      (fields(0), (fields(1).toInt, fields(2).toInt))
    }).cache()

    // 未回答问题、昨日未回答次数、7天未回答次数、30天未回答次数、相似问30天问到次数（无用，置为0）
    // 昨日提供建议问次数、7天提供建议问次数、30天提供建议问次数
    oneday_data
      .leftOuterJoin(week_data)
      .leftOuterJoin(month_data)
      .map({case (ques, (((oCnt0, oCnt11), weekCnt), monthCnt)) => {
        val (wCnt0, wCnt11) = weekCnt.getOrElse((0, 0))
        val (mCnt0, mCnt11) = monthCnt.getOrElse((0, 0))
        //        val rCnt = relatedCnt.getOrElse(0)
        val cnt_info = List(ques, oCnt0.toString, wCnt0.toString, mCnt0.toString, "0", //TODO 重写主表插入时，可删除此处rcnt=O
          oCnt11.toString, wCnt11.toString, mCnt11.toString)
        cnt_info.mkString("|")
      }})
      .saveAsTextFile(outputPath)

    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("Get the recommend faq") {
      head("Get the recommend faq ")
      opt[String]("stepCntInput")
        .text("Input root path for unanswered questions count")
        .action((x, c) => c.copy(stepCntInput = x))
      opt[String]("stepCntOut")
        .text("Output path for unanswered questions frequency weight")
        .action((x, c) => c.copy(stepCntOut = x))
      opt[String]('u', "dfsUri")
        .text("hadoop fs default name")
        .action((x, c) => c.copy(dfsUri = x))
      opt[Boolean]('r', "removeMode")
        .text("remove or backup output dir that exists")
        .action((x, c) => c.copy(removeMode = x))
      checkConfig { params => success }
    }

    parser.parse(args, defaultParams).map {
      params => run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  case class Params(
                     stepCntInput: String = "",
                     stepCntOut: String = "",
                     dfsUri: String = "",
                     removeMode: Boolean = true
                   )
}
